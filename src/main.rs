mod client_repo;
mod models;
mod output;
mod processor;
mod producers;
mod repo;
mod transaction_repo;

use crate::output::output_accounts;
use crate::processor::PaymentProcessor;
use std::{env, process};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // reading the input file path from the command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        error!("Usage: {} <input_file>", args[0]);
        process::exit(1);
    }
    let file_path = args[1].clone(); // Get the input file path

    // Create a channel for transactions
    let (tx, rx) = mpsc::channel(10);
    // shutdown channels
    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();

    // Create a CancellationToken for graceful shutdown
    let shutdown_token = CancellationToken::new();

    // Task tracker to manage spawned tasks
    let tracker = TaskTracker::new();

    // Spawn the producer task
    let producer_shutdown_token = shutdown_token.clone();
    tracker.spawn(async move {
        let transaction_producer =
            producers::CSVTransactionProducer::new(file_path, producer_shutdown_token).clone();
        transaction_producer
            .run(tx)
            .await
            .expect("Failed to run transaction producer");
    });

    // Spawn the processor task
    let processor_shutdown_token = shutdown_token.clone();
    tracker.spawn(async move {
        let clients = client_repo::InMemoryRepo::new();
        let transactions = transaction_repo::InMemoryRepo::new();
        let mut processor = PaymentProcessor::new(clients, transactions);

        processor
            .process_transactions(rx, processor_shutdown_token)
            .await;

        let clients_stream = processor.stream_clients();
        output_accounts(clients_stream).await;

        shutdown_tx
            .send(())
            .expect("Failed to send shutdown signal");
    });

    // Spawn a signal handler for graceful shutdown
    let shutdown_signal_token = shutdown_token.clone();
    tracker.spawn(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl+C, initiating shutdown...");
                shutdown_signal_token.cancel(); // Notify all tasks to stop
            }
            _ = shutdown_rx.recv() => {
                info!("Received shutdown signal, initiating shutdown...");
                shutdown_signal_token.cancel(); // Notify all tasks to stop
            }
        }
    });

    // Wait for all tasks to complete or cancel
    tracker.close();
    tracker.wait().await;

    info!("Application shutdown complete.");
    Ok(())
}
