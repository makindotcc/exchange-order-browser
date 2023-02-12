extern crate core;

pub mod exchange;
mod website;

use crate::exchange::binance;
use env_logger::Env;
use log::info;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::new().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
    info!("Hello world.");

    website::start().await
}
