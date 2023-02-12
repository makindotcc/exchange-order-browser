use crate::exchange::trade_reader::{
    http_zip_lines_reader, HttpZipReaderError, ParseTradeError, Trade, TradePair, TradeSide,
    ZipCsvTradeReader,
};
use awc::error::{PayloadError, SendRequestError};
use chrono::NaiveDate;
use log::error;
use serde::Deserialize;
use std::fmt::Debug;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncRead;

pub async fn aws_trade_reader(
    client: &awc::Client,
    trade_pair: &TradePair,
    date: NaiveDate,
) -> Result<ZipCsvTradeReader<impl AsyncRead + Unpin + Sized>, HttpZipReaderError> {
    let url = trades_archive_url(trade_pair, date);
    let lines_reader = http_zip_lines_reader(client, &url).await?;
    let trades_reader = ZipCsvTradeReader::new(lines_reader, parse_csv_trade);
    Ok(trades_reader)
}

fn trades_archive_url(trade_pair: &TradePair, date: NaiveDate) -> String {
    let formatted_pair = format!("{}{}", trade_pair.first, trade_pair.second);
    format!(
        "https://data.binance.vision/data/futures/um/daily/trades/{}/{}-trades-{}.zip",
        formatted_pair,
        formatted_pair,
        date.format("%Y-%m-%d")
    )
}

fn parse_side(raw: &str) -> Option<TradeSide> {
    match raw {
        "false" => Some(TradeSide::Buy),
        "true" => Some(TradeSide::Sell),
        _ => None,
    }
}

fn parse_csv_trade(row: String) -> Result<Trade, ParseTradeError> {
    let mut columns = row.split(',');
    let trade_id = columns
        .next()
        .map(|str| str.parse::<u64>())
        .ok_or_else(|| ParseTradeError::MissingTradeId)?
        .map_err(|err| ParseTradeError::InvalidTradeId(row.clone(), err))?;
    let price = columns
        .next()
        .map(|str| str.parse::<f64>())
        .ok_or_else(|| ParseTradeError::MissingPrice)?
        .map_err(ParseTradeError::InvalidPrice)?;
    let mut columns = columns.skip(2);
    let timestamp = columns
        .next()
        .map(|str| str.parse::<i64>())
        .ok_or_else(|| ParseTradeError::MissingTimestamp)?
        .map_err(ParseTradeError::InvalidTimestamp)?;
    let side = columns
        .next()
        .ok_or_else(|| ParseTradeError::MissingSide)
        .map(|column| {
            parse_side(column.trim_end()).ok_or_else(|| ParseTradeError::InvalidSide(row.clone()))
        })
        .and_then(|flatten| flatten)?;
    Ok(Trade {
        id: trade_id,
        side,
        price,
        timestamp,
    })
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ListBucketResult {
    #[serde(rename = "IsTruncated")]
    truncated: bool,
    #[serde(rename = "NextMarker")]
    next_marker: Option<String>,
    #[serde(rename = "Contents")]
    contents: Vec<BucketContents>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct BucketContents {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "Size")]
    pub size: u64,
}

impl BucketContents {
    pub fn is_zip(&self) -> bool {
        self.key.ends_with(".zip")
    }
}

#[derive(Debug, Error)]
pub enum ListBucketError {
    #[error("Serialize request error: {0}")]
    SerializeRequest(serde_urlencoded::ser::Error),
    #[error("Send request error: {0}")]
    SendRequest(SendRequestError),
    #[error("Read body error: {0}")]
    ReadBody(PayloadError),
    #[error("Parse response: {0}")]
    Parse(serde_xml_rs::Error),
}

pub async fn list_whole_bucket(trades: &str) -> Result<Vec<BucketContents>, ListBucketError> {
    let client = awc::ClientBuilder::new()
        .timeout(Duration::from_secs(40))
        .finish();
    let mut contents = Vec::new();
    let mut marker = None;
    let contents = loop {
        let mut result = list_bucket_part(&client, trades, marker.as_deref()).await?;
        contents.append(&mut result.contents);
        marker = result.next_marker;
        if !result.truncated {
            break contents;
        }
    };
    Ok(contents)
}

async fn list_bucket_part(
    client: &awc::Client,
    trades: &str,
    marker: Option<&str>,
) -> Result<ListBucketResult, ListBucketError> {
    let mut response = client
        .get("https://s3-ap-northeast-1.amazonaws.com/data.binance.vision")
        .query(&[
            ("delimiter", "/"),
            (
                "prefix",
                &format!("data/futures/um/daily/trades/{}/", trades),
            ),
            ("marker", marker.unwrap_or_default()),
        ])
        .map_err(ListBucketError::SerializeRequest)?
        .send()
        .await
        .map_err(ListBucketError::SendRequest)?;
    let body = response
        .body()
        .await
        .map_err(ListBucketError::ReadBody)?;
    let result: ListBucketResult = serde_xml_rs::from_str(&String::from_utf8_lossy(&body))
        .map_err(ListBucketError::Parse)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_list_bucket() {
        let xml_content = r#"
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>data.binance.vision</Name>
            <Prefix>data/futures/um/daily/aggTrades/BTCUSDT/</Prefix>
            <Marker/>
            <NextMarker>data/futures/um/BTCUSDT-aggTrades-2021-05-13.zip</NextMarker>
            <MaxKeys>1000</MaxKeys>
            <Delimiter>/</Delimiter>
            <IsTruncated>true</IsTruncated>
            <Contents>
                <Key>data/futures/um/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2019-12-31.zip</Key>
                <LastModified>2022-03-03T23:05:43.000Z</LastModified>
                <ETag>"9d88be0772290b887ac4c0df40b46266"</ETag>
                <Size>1518388</Size>
                <StorageClass>STANDARD</StorageClass>
            </Contents>
            <Contents>
                <Key>data/futures/um/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2019-12-30.zip</Key>
                <LastModified>2022-03-03T23:05:43.000Z</LastModified>
                <ETag>"10f4fa87a597bd1dc63d9ced8dbf8b66"</ETag>
                <Size>1518388</Size>
                <StorageClass>STANDARD</StorageClass>
            </Contents>
        </ListBucketResult>
        "#;

        let result: ListBucketResult = serde_xml_rs::de::from_str(xml_content).unwrap();
        assert_eq!(
            result,
            ListBucketResult {
                next_marker: Some("data/futures/um/BTCUSDT-aggTrades-2021-05-13.zip".to_string()),
                truncated: true,
                contents: vec![
                    BucketContents {
                        key: "data/futures/um/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2019-12-31.zip"
                            .to_string(),
                        etag: r#""9d88be0772290b887ac4c0df40b46266""#.to_string(),
                        size: 1518388,
                    },
                    BucketContents {
                        key: "data/futures/um/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2019-12-30.zip"
                            .to_string(),
                        etag: r#""10f4fa87a597bd1dc63d9ced8dbf8b66""#.to_string(),
                        size: 1518388,
                    },
                ],
            }
        );
    }

    #[test]
    fn test_trade_archive_filename() {
        assert_eq!(
            trades_archive_url(&TradePair::new("BTC", "USDT"), NaiveDate::from_ymd(2022, 9, 13)),
            "https://data.binance.vision/data/futures/um/daily/trades/BTCUSDT/BTCUSDT-trades-2022-09-13.zip"
        );
    }
}
