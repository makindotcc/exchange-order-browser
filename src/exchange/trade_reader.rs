use actix_http::StatusCode;
use async_zip::error::ZipError;
use async_zip::read::stream::ZipFileReader;
use async_zip::read::ZipEntryReader;
use awc::error::SendRequestError;
use futures::future::Ready;
use futures::{future, Stream, StreamExt, TryStreamExt};
use log::error;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::num::{ParseFloatError, ParseIntError};
use std::pin::Pin;
use std::str::FromStr;
use thiserror::Error;
use tokio::io;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::task::spawn_local;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;

pub type ReadResult = Result<Trade, TradeReaderError>;

type CreateStream = Result<Pin<Box<dyn Stream<Item = ReadResult>>>, TradeReaderError>;

type CreateStreamFuture = Pin<Box<dyn Future<Output = CreateStream>>>;

pub trait TradeReader {
    #[allow(clippy::needless_lifetimes)]
    fn stream(self, sample_every_n_trade: usize) -> CreateStreamFuture;
}

#[derive(Debug)]
pub struct Trade {
    pub id: u64,
    pub side: TradeSide,
    pub price: f64,
    pub timestamp: i64,
}

#[derive(serde::Serialize, PartialEq, Debug, Clone, Copy)]
pub enum TradeSide {
    #[serde(rename = "buy")]
    Buy,
    #[serde(rename = "sell")]
    Sell,
}

#[derive(Debug)]
pub struct TradePair {
    pub first: String,
    pub second: String,
}

impl TradePair {
    pub fn new(first: impl Into<String>, second: impl Into<String>) -> Self {
        Self {
            first: first.into(),
            second: second.into(),
        }
    }
}

impl Display for TradePair {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.first, self.second)
    }
}

impl FromStr for TradePair {
    type Err = ();

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut split = text.split('-');
        if let (Some(first), Some(second)) = (split.next(), split.next()) {
            Ok(TradePair {
                first: first.to_string(),
                second: second.to_string(),
            })
        } else {
            Err(())
        }
    }
}

type TradeParser = fn(String) -> Result<Trade, ParseTradeError>;

pub struct ZipCsvTradeReader<R: AsyncRead + Unpin + Sized> {
    zip_reader: ZipLinesReader<R>,
    parse_trade: TradeParser,
}

impl<R> ZipCsvTradeReader<R>
where
    R: AsyncRead + Unpin + Sized,
{
    pub fn new(zip_reader: ZipLinesReader<R>, parse_trade: TradeParser) -> Self {
        Self {
            zip_reader,
            parse_trade,
        }
    }

    fn is_csv_header((index, res): &(usize, io::Result<String>)) -> Ready<bool> {
        future::ready(res.is_ok() && *index == 0)
    }

    fn parse_csv_row(
        parse_trade: TradeParser,
        read_next_result: io::Result<String>,
    ) -> Result<Trade, TradeReaderError> {
        read_next_result
            .map(|line| (parse_trade)(line).map_err(TradeReaderError::Parse))
            .map_err(TradeReaderError::IO)
            .and_then(|flatten| flatten)
    }
}

impl<R> TradeReader for ZipCsvTradeReader<R>
where
    R: AsyncRead + Unpin + Sized + 'static,
{
    fn stream(self, sample_every_n_trade: usize) -> CreateStreamFuture {
        let fut = async move {
            let boxed: Result<Pin<Box<dyn Stream<Item = ReadResult>>>, TradeReaderError> = self
                .zip_reader
                .stream(sample_every_n_trade)
                .await
                .map(move |stream| -> Pin<Box<dyn Stream<Item = ReadResult>>> {
                    let stream = stream
                        .enumerate()
                        .skip_while(Self::is_csv_header)
                        .map(|(_, res)| res)
                        .map(move |read_next_result: io::Result<String>| {
                            Self::parse_csv_row(self.parse_trade, read_next_result)
                        });
                    Box::pin(stream)
                })
                .map_err(TradeReaderError::ZipReader);
            boxed
        };
        Box::pin(fut)
    }
}

pub struct ZipLinesReader<R: AsyncRead + Unpin> {
    zip: ZipFileReader<R>,
}

impl<R: AsyncRead + Unpin + 'static> ZipLinesReader<R> {
    pub async fn stream(
        mut self,
        sample_every_line: usize,
    ) -> Result<impl Stream<Item = io::Result<String>>, ZipReaderError> {
        let (zip_result_writer, zip_result_reader) = oneshot::channel();
        let (writer, receiver) = mpsc::channel(128);
        // nwm kurwa entry_reader z zipa bierze referencje i przez to
        // nie mozemy zwrocic narmalnie streama ktory uzywa entry_reader
        // bo zip pojdzie poza scope i bd dropniety no ogulem nwm
        spawn_local(async move {
            let zip_result = self
                .zip
                .entry_reader()
                .await
                .map_err(ZipReaderError::Unzip)
                .map(|reader_maybe| reader_maybe.ok_or(ZipReaderError::EmptyZip))
                .and_then(|flatten| flatten);
            match zip_result {
                Ok(mut zip) => {
                    let _ = zip_result_writer.send(None);
                    Self::write_essa(writer, &mut zip, sample_every_line).await;
                }
                Err(err) => {
                    let _ = zip_result_writer.send(Some(err));
                }
            }
        });
        let open_reader_err = zip_result_reader
            .await
            .expect("Channel should not be closed");
        if let Some(err) = open_reader_err {
            Err(err)
        } else {
            let stream = ReceiverStream::new(receiver);
            Ok(Box::pin(stream))
        }
    }

    async fn write_essa(
        writer: Sender<Result<String, io::Error>>,
        zip: &mut ZipEntryReader<'_, R>,
        sample_every_line: usize,
    ) {
        let mut entry_reader = BufReader::new(zip);
        let mut buffer = String::with_capacity(64);
        let mut index = 0usize;
        loop {
            buffer.clear();

            match entry_reader.read_line(&mut buffer).await {
                Ok(_) => {
                    if buffer.is_empty() {
                        break;
                    }
                    if index % sample_every_line == 0
                        && (writer.send(Ok(buffer.clone())).await).is_err()
                    {
                        break;
                    }
                    index += 1;
                }
                Err(err) => {
                    if index == 0 && err.kind() == io::ErrorKind::InvalidData {
                        continue;
                    }
                    let _ = writer.send(Err(err)).await;
                    break;
                }
            }
        }
    }
}

pub async fn http_zip_lines_reader(
    client: &awc::Client,
    url: &str,
) -> Result<ZipLinesReader<impl AsyncRead + Unpin>, HttpZipReaderError> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(HttpZipReaderError::SendRequest)?;
    let code = response.status();
    if code.is_success() {
        let zip = ZipFileReader::new(StreamReader::new(
            response.map_err(|err| io::Error::new(io::ErrorKind::Other, err)),
        ));
        Ok(ZipLinesReader { zip })
    } else if code == StatusCode::NOT_FOUND {
        Err(HttpZipReaderError::NotFound)
    } else {
        Err(HttpZipReaderError::InvalidStatusCode(code))
    }
}

#[derive(Debug, Error)]
pub enum TradeReaderError {
    #[error("Could not read zip: {0}")]
    ZipReader(ZipReaderError),
    #[error("IO error: {0}")]
    IO(io::Error),
    #[error("Could not parse trade: {0}")]
    Parse(ParseTradeError),
}

#[derive(Debug, Error)]
pub enum HttpZipReaderError {
    #[error("Could not send request: {0}")]
    SendRequest(SendRequestError),
    #[error("Zip reader error: {0}")]
    ZipReader(ZipReaderError),
    #[error("Trades for given params are not present.")]
    NotFound,
    #[error("Server returned invalid status code: {0}")]
    InvalidStatusCode(StatusCode),
}

#[derive(Debug, Error)]
pub enum ZipReaderError {
    #[error("Unzip error: {0}")]
    Unzip(ZipError),
    #[error("Empty zip")]
    EmptyZip,
}

#[derive(Debug, Error)]
pub enum ParseTradeError {
    #[error("Missing trade id")]
    MissingTradeId,
    #[error("Invalid trade id '{0}': ")]
    InvalidTradeId(String, ParseIntError),
    #[error("Missing side")]
    MissingSide,
    #[error("Invalid side: {0}")]
    InvalidSide(String),
    #[error("Missing price")]
    MissingPrice,
    #[error("Invalid price: {0}")]
    InvalidPrice(ParseFloatError),
    #[error("Missing timestamp")]
    MissingTimestamp,
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(ParseIntError),
    #[error("Underlying stream error: {0}")]
    Stream(io::Error),
    #[error("IO error: {0}")]
    IO(io::Error),
}
