use actix_codec::Framed;
use actix_web::web::Bytes;
use awc::error::{WsClientError, WsProtocolError};
use awc::ws::{Codec, Frame, Message};
use awc::BoxedSocket;
use futures::stream::SplitStream;
use futures::{SinkExt, StreamExt};
use log::{debug, error};
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::spawn_local;
use tokio::time::{sleep, Instant};
use tokio_stream::wrappers::UnboundedReceiverStream;

const KEEP_ALIVE_DELAY: Duration = Duration::from_secs(1);
const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
enum Event {
    TextReceived(Bytes),
    PingReceived(Bytes),
    PongReceived,
    KeepAlive,
    ProtocolError(WsProtocolError),
}

impl Event {
    fn from_frame(frame: Frame) -> Option<Event> {
        match frame {
            Frame::Text(text) => Some(Event::TextReceived(text)),
            Frame::Ping(bytes) => Some(Event::PingReceived(bytes)),
            Frame::Pong(_) => Some(Event::PongReceived),
            _ => None,
        }
    }
}

pub fn listen_updates() -> UnboundedReceiverStream<AggrMessage> {
    let (msg_sender, msg_receiver) = mpsc::unbounded_channel();
    spawn_local(async move {
        loop {
            let result = try_listen_updates(msg_sender.clone()).await;
            match result {
                Err(ListenError::ListenerClosed) => {
                    debug!("Listener closed");
                    return;
                }
                Err(err) => {
                    error!("Caught error while listening to updates: {err}.");
                }
                Ok(_) => {}
            }
        }
    });
    msg_receiver.into()
}

async fn try_listen_updates(listener: UnboundedSender<AggrMessage>) -> Result<(), ListenError> {
    let client = awc::Client::builder()
        .max_http_version(awc::http::Version::HTTP_11)
        .timeout(Duration::from_secs(5))
        .finish();
    let (_, framed) = client
        .ws("wss://fstream.binance.com/ws/btcusdt@aggTrade")
        .connect()
        .await?;

    let (mut output_stream, input_stream) = framed.split();
    let (event_sender, mut event_receiver) = mpsc::unbounded_channel();
    spawn_local(handle_ws_packets(input_stream, event_sender.clone()));

    let mut last_heartbeat = Instant::now();
    spawn_local(keep_alive(event_sender));

    while let Some(event) = event_receiver.recv().await {
        match event {
            Event::TextReceived(bytes) => {
                let message = serde_json::from_slice::<Value>(&bytes)
                    .map_err(ListenError::InvalidPacket)?;
                if let Some(parsed) = AggrMessage::from_json(message) {
                    if listener.send(parsed).is_err() {
                        return Err(ListenError::ListenerClosed);
                    }
                }
            }
            Event::PingReceived(bytes) => {
                output_stream.send(Message::Pong(bytes)).await?;
            }
            Event::PongReceived => {
                debug!("Pong received!");
                last_heartbeat = Instant::now();
            }
            Event::KeepAlive => {
                let duration_since_last_hb = Instant::now().duration_since(last_heartbeat);
                if duration_since_last_hb > TIMEOUT {
                    error!("Heartbeat timeout: {duration_since_last_hb:?} > {TIMEOUT:?}");
                    return Err(ListenError::Timeout(duration_since_last_hb));
                } else {
                    output_stream.send(Message::Ping(Bytes::default())).await?;
                }
            }
            Event::ProtocolError(err) => {
                return Err(ListenError::ProtocolError(err));
            }
        }
    }
    Err(ListenError::EndOfStream)
}

async fn handle_ws_packets(
    stream: SplitStream<Framed<BoxedSocket, Codec>>,
    tx: UnboundedSender<Event>,
) {
    let mut event_stream = stream.map(|result| result.map(Event::from_frame));
    while let Some(event_result) = event_stream.next().await {
        match event_result {
            Ok(Some(event)) => {
                if tx.send(event).is_err() {
                    break;
                }
            }
            Ok(None) => {}
            Err(err) => {
                let _ = tx.send(Event::ProtocolError(err));
                break;
            }
        }
    }
}

async fn keep_alive(tx: UnboundedSender<Event>) {
    loop {
        sleep(KEEP_ALIVE_DELAY).await;
        if tx.send(Event::KeepAlive).is_err() {
            break;
        }
    }
}

#[derive(Debug, Error)]
pub enum ListenError {
    #[error("Could not connect to ws server: {0:?}")]
    ConnectError(WsClientError),
    #[error("Protocol error occurred: {0:?}")]
    ProtocolError(WsProtocolError),
    #[error("Invalid packet structure: {0:?}")]
    InvalidPacket(serde_json::Error),
    #[error("Timed out: {0:?}")]
    Timeout(Duration),
    #[error("End of stream")]
    EndOfStream,
    #[error("Listener channel closed")]
    ListenerClosed,
}

impl From<WsClientError> for ListenError {
    fn from(error: WsClientError) -> Self {
        ListenError::ConnectError(error)
    }
}

impl From<WsProtocolError> for ListenError {
    fn from(protocol_error: WsProtocolError) -> Self {
        ListenError::ProtocolError(protocol_error)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct AggrMessage {
    pub symbol: String,
    pub timestamp: SystemTime,
    pub trade_id: u64,
    pub price: f64,
}

impl AggrMessage {
    pub fn from_json(value: Value) -> Option<Self> {
        let event_type = value.get("e")?;
        if event_type != "aggTrade" {
            return None;
        }
        let symbol = value.get("s")?.as_str()?;
        let timestamp = UNIX_EPOCH + Duration::from_millis(value.get("T")?.as_u64()?);
        let trade_id = value.get("a")?.as_u64()?;
        let price = value.get("p")?.as_str()?.parse().ok()?;
        Some(AggrMessage {
            symbol: symbol.to_string(),
            timestamp,
            trade_id,
            price,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_aggr_message() {
        let value = serde_json::from_str(
            r#"{
                "e": "aggTrade",
                "E": 1663191423013,
                "s": "BTCUSDT",
                "a": 1556789983,
                "p": "20025.58000000",
                "q": "0.13400000",
                "f": 1816909525,
                "l": 1816909527,
                "T": 1663191423013,
                "m": false,
                "M": true
            }"#,
        )
            .unwrap();

        assert_eq!(
            AggrMessage::from_json(value).unwrap(),
            AggrMessage {
                symbol: "BTCUSDT".to_string(),
                timestamp: UNIX_EPOCH + Duration::from_millis(1663191423013),
                trade_id: 1556789983,
                price: 20025.58,
            }
        )
    }
}
