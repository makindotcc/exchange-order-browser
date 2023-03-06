extern crate core;

pub mod exchange;
mod website;

use crate::exchange::binance;
use crate::website::Credentials;
use env_logger::Env;
use log::info;
use std::env;
use std::env::VarError;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::new().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
    info!("Hello world.");
    let credentials = parse_credentials();
    match &credentials {
        Some(_) => info!("Credentials provided. To access website password is required!"),
        None => info!("Credentials NOT provided, website will be accessible WITHOUT password."),
    }
    website::start(credentials).await
}

fn parse_credentials() -> Option<Credentials> {
    let get_cred = |key| match env::var(key) {
        Ok(val) => Some(val),
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(_)) => panic!("{key} is not valid unicode string"),
    };
    let user = get_cred("AUTH_USER");
    let password = get_cred("AUTH_PASSWORD");
    match (user, password) {
        (None, None) => None,
        (user, password) => Some(Credentials {
            user: user.unwrap_or(String::from("")),
            password: password.unwrap_or(String::from("")),
        }),
    }
}
