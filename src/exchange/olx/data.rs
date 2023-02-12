use crate::exchange::trade_reader::{
    http_zip_lines_reader, HttpZipReaderError, ParseTradeError, Trade, TradePair, TradeSide,
    ZipCsvTradeReader,
};
use chrono::NaiveDate;
use tokio::io::AsyncRead;

pub async fn archived_trade_reader(
    client: &awc::Client,
    trade_pair: &TradePair,
    date: NaiveDate,
) -> Result<ZipCsvTradeReader<impl AsyncRead + Unpin>, HttpZipReaderError> {
    let url = trades_archive_url(trade_pair, date);
    let lines_reader = http_zip_lines_reader(client, &url).await?;
    let trades_reader = ZipCsvTradeReader::new(lines_reader, parse_trade);
    Ok(trades_reader)
}

fn trades_archive_url(trade_pair: &TradePair, date: NaiveDate) -> String {
    format!(
        "https://static.okx.com/cdn/okex/traderecords/trades/daily/{}/{}-{}-trades-{}.zip",
        date.format("%Y%m%d"),
        trade_pair.first,
        trade_pair.second,
        date.format("%Y-%m-%d")
    )
}

fn parse_side(raw: &str) -> Option<TradeSide> {
    match raw {
        "buy" => Some(TradeSide::Buy),
        "sell" => Some(TradeSide::Sell),
        _ => None,
    }
}

fn parse_trade(row: String) -> Result<Trade, ParseTradeError> {
    let columns = row.split(',').collect::<Vec<_>>();
    let trade_id = columns.first()
        .map(|str| str.parse::<u64>())
        .ok_or_else(|| ParseTradeError::MissingTradeId)?
        .map_err(|err| ParseTradeError::InvalidTradeId(row.clone(), err))?;
    let side = columns
        .get(1)
        .ok_or_else(|| ParseTradeError::MissingSide)
        .map(|column| parse_side(column).ok_or_else(|| ParseTradeError::InvalidSide(row.clone())))
        .and_then(|flatten| flatten)?;
    let price = columns
        .get(3)
        .map(|str| str.parse::<f64>())
        .ok_or_else(|| ParseTradeError::MissingPrice)?
        .map_err(ParseTradeError::InvalidPrice)?;
    let timestamp = columns
        .get(4)
        .map(|str| str.trim_end().parse::<i64>())
        .ok_or_else(|| ParseTradeError::MissingTimestamp)?
        .map_err(ParseTradeError::InvalidTimestamp)?;
    Ok(Trade {
        id: trade_id,
        side,
        price,
        timestamp,
    })
}
