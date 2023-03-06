use crate::binance;
use crate::exchange::olx;
use crate::exchange::trade_reader::{
    HttpZipReaderError, Trade, TradePair, TradeReader, TradeReaderError, TradeSide,
};
use actix_files::Files;
use actix_web::dev::ServiceRequest;
use actix_web::http::StatusCode;
use actix_web::middleware::Condition;
use actix_web::{get, middleware, web, App, HttpResponse, HttpServer, ResponseError};
use actix_web_httpauth::extractors::basic::BasicAuth;
use actix_web_httpauth::extractors::{basic, AuthenticationError};
use actix_web_httpauth::middleware::HttpAuthentication;
use chrono::NaiveDate;
use futures::{Stream, StreamExt, TryStreamExt};
use log::{debug, error, info};
use serde_json::json;
use std::future;
use std::future::Ready;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio::task::{spawn_local, JoinHandle};
use tokio::time::sleep;
use tokio_util::codec::BytesCodec;
use tokio_util::codec::FramedRead;

#[derive(Clone)]
pub struct Credentials {
    pub user: String,
    pub password: String,
}

async fn login(
    req: ServiceRequest,
    req_credentials: BasicAuth,
) -> Result<ServiceRequest, (actix_web::Error, ServiceRequest)> {
    let server_credentials: &Option<Credentials> =
        req.app_data().expect("Missing credentials data!");
    match server_credentials {
        None => Ok(req),
        Some(server_credentials)
            if req_credentials.user_id() == server_credentials.user
                && req_credentials.password() == Some(&server_credentials.password) =>
        {
            Ok(req)
        }
        Some(_) => {
            info!(
                "{} loguje sie ! zlymi pasami: '{}'{}",
                req.connection_info().peer_addr().unwrap_or("-"),
                req_credentials.user_id(),
                req_credentials
                    .password()
                    .map(|pass| format!(":'{}'", pass))
                    .unwrap_or_default(),
            );
            sleep(Duration::from_secs(1)).await;
            let config = req.app_data::<basic::Config>().cloned().unwrap_or_default();
            Err((AuthenticationError::from(config).into(), req))
        }
    }
}

pub async fn start(server_credentials: Option<Credentials>) -> std::io::Result<()> {
    HttpServer::new(move || {
        let http_client = awc::Client::new();
        App::new()
            .configure(|config| {
                if let Some(server_credentials) = server_credentials.clone() {
                    config.app_data(server_credentials);
                }
            })
            .app_data(server_credentials.clone())
            .service(Files::new("/.well-known/", "./frontend/.well-known/").use_hidden_files())
            .service(
                web::scope("")
                    .wrap(
                        middleware::DefaultHeaders::new().add(("Access-Control-Allow-Origin", "*")),
                    )
                    .wrap(Condition::new(
                        server_credentials.is_some(),
                        HttpAuthentication::basic(login),
                    ))
                    .service(view_detailed_dataset)
                    .service(Files::new("/", "./frontend").index_file("index.html"))
                    .app_data(web::Data::new(http_client)),
            )
            .wrap(middleware::Logger::default())
    })
    .bind(("0.0.0.0", 2137))?
    .run()
    .await
}

#[derive(serde::Serialize, PartialEq, Debug, Clone, Copy)]
#[serde(untagged)]
enum Field {
    Timestamp(i64),
    Price(f64),
    Side(TradeSide),
}

#[derive(Debug, Error)]
enum DatasetError {
    #[error("Could not parse date: {0}")]
    ParseDate(chrono::ParseError),
    #[error("Could not parse coin pair")]
    ParseCoinPair,
    #[error("Dataset for given parameters not found")]
    NotFound,
    #[error("Zip reader error: {0}")]
    HttpZipReader(HttpZipReaderError),
}

impl ResponseError for DatasetError {
    fn status_code(&self) -> StatusCode {
        match *self {
            DatasetError::ParseDate(_) | DatasetError::ParseCoinPair => StatusCode::BAD_REQUEST,
            DatasetError::NotFound => StatusCode::NOT_FOUND,
            DatasetError::HttpZipReader(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let error_msg = self.to_string();
        let message = if self.status_code().is_client_error() {
            &error_msg
        } else if self.status_code().is_server_error() {
            "Internal server error"
        } else {
            "Unknown error"
        };
        HttpResponse::build(self.status_code()).json(json!({
            "error": message,
        }))
    }
}

const BINANCE_SAMPLE_TRADE_EVERY: usize = 50;
const OLX_SAMPLE_TRADE_EVERY: usize = 10;

#[get("/dataset/{exchange}/{coin_pair}/{date}")]
async fn view_detailed_dataset(
    client: web::Data<awc::Client>,
    path: web::Path<(String, String, String)>,
) -> actix_web::Result<HttpResponse> {
    let (exchange, raw_coin_pair, raw_date) = path.into_inner();
    let date = NaiveDate::parse_from_str(&raw_date, "%Y-%m-%d").map_err(DatasetError::ParseDate)?;
    let coin_pair: TradePair = raw_coin_pair
        .parse()
        .map_err(|_| DatasetError::ParseCoinPair)?;

    let http_to_dataset_err = |err| match err {
        HttpZipReaderError::NotFound => DatasetError::NotFound,
        other => DatasetError::HttpZipReader(other),
    };
    let trade_reader = match &exchange[..] {
        "binance" => {
            binance::data::aws_trade_reader(&client, &coin_pair, date)
                .await
                .map_err(http_to_dataset_err)?
                .stream(BINANCE_SAMPLE_TRADE_EVERY)
                .await
        }
        "olx" => {
            olx::data::archived_trade_reader(&client, &coin_pair, date)
                .await
                .map_err(http_to_dataset_err)?
                .stream(OLX_SAMPLE_TRADE_EVERY)
                .await
        }
        _ => todo!(),
    };

    debug!(
        "Opening trade reader for {} at {} from {}",
        coin_pair, raw_date, exchange
    );

    let (mut to_write, to_read) = tokio::io::duplex(32767);
    let _join_handle: JoinHandle<Result<(), std::io::Error>> = spawn_local(async move {
        debug!("Draining trades essa.");
        match trade_reader {
            Ok(trade_stream) => {
                let trade_stream = trade_stream.filter_map(handle_trade_error);
                write_trades(trade_stream, to_write).await?;
            }
            Err(err) => match serde_json::to_vec(&json!({ "err": err.to_string() })) {
                Ok(err_serialized) => to_write.write_all(&err_serialized).await?,
                Err(serialize_err) => {
                    error!("Could not serialize trade stream error: {}!", serialize_err)
                }
            },
        };
        Ok(())
    });
    let stream = FramedRead::new(to_read, BytesCodec::new()).map_ok(|b| b.freeze());

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .insert_header(("Cache-Control", "public, max-age=31557600"))
        .streaming(stream))
}

fn handle_trade_error(trade_result: Result<Trade, TradeReaderError>) -> Ready<Option<Trade>> {
    future::ready(match trade_result {
        Ok(trade) => Some(trade),
        Err(err) => {
            error!("Could not read trade: {}", err);
            None
        }
    })
}

async fn write_trades(
    mut trade_stream: impl Stream<Item = Trade> + Sized + Unpin,
    mut to_write: DuplexStream,
) -> Result<(), std::io::Error> {
    let serialize_trade = |trade: Trade| {
        serde_json::to_vec(&[
            Field::Timestamp(trade.timestamp),
            Field::Price(trade.price),
            Field::Side(trade.side),
        ])
    };
    to_write.write_all("[".as_bytes()).await?;
    if let Some(trade) = trade_stream.next().await {
        to_write.write_all(&serialize_trade(trade)?).await?;
    }
    while let Some(trade) = trade_stream.next().await {
        to_write.write_all(",".as_bytes()).await?;
        to_write.write_all(&serialize_trade(trade)?).await?;
    }
    to_write.write_all("]".as_bytes()).await?;
    Ok(())
}
