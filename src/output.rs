use crate::models;
use csv::WriterBuilder;
use serde::Serialize;
use std::io;
use std::pin::Pin;
use tokio_stream::{Stream, StreamExt};
use tracing::error;

#[derive(Serialize)]
struct CSVClient {
    client: String,
    available: rust_decimal::Decimal,
    held: rust_decimal::Decimal,
    total: rust_decimal::Decimal,
    locked: bool,
}

impl From<models::Client> for CSVClient {
    fn from(client: models::Client) -> Self {
        Self {
            client: client.client_id.to_string(),
            available: client.assets.available.round_dp(4),
            held: client.assets.held.round_dp(4),
            total: client.assets.total.round_dp(4),
            locked: client.locked,
        }
    }
}

pub async fn output_accounts(mut clients: Pin<Box<dyn Stream<Item = models::Client> + Send + '_>>) {
    let mut writer = WriterBuilder::new().from_writer(io::stdout());
    while let Some(client) = clients.next().await {
        if let Err(e) = writer.serialize(CSVClient::from(client)) {
            error!("Failed to write account to CSV: {:?}", e);
        }
    }
    writer.flush().expect("Failed to flush CSV writer");
}
