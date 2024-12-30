use crate::models;
use csv::WriterBuilder;
use serde::Serialize;
use std::io;
use std::pin::Pin;
use tokio_stream::{Stream, StreamExt};
use tracing::error;

#[derive(Serialize)]
struct CSVClient {
    client_id: String,
    available: f64,
    held: f64,
    total: f64,
    locked: bool,
}

impl From<models::Client> for CSVClient {
    fn from(client: models::Client) -> Self {
        Self {
            client_id: client.client_id.to_string(),
            available: client.assets.available,
            held: client.assets.held,
            total: client.assets.total,
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
