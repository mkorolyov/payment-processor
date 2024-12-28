use crate::models;
use csv::WriterBuilder;
use std::io;
use std::pin::Pin;
use tokio_stream::{Stream, StreamExt};
use tracing::error;

pub async fn output_accounts(mut clients: Pin<Box<dyn Stream<Item = models::Client> + Send>>) {
    let mut writer = WriterBuilder::new().from_writer(io::stdout());
    while let Some(client) = clients.next().await {
        if let Err(e) = writer.serialize(client) {
            error!("Failed to write account to CSV: {:?}", e);
        }
    }
    writer.flush().expect("Failed to flush CSV writer");
}
