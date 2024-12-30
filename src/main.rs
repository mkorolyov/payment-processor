mod client_repo;
mod models;
mod output;
mod processor;
mod producers;
mod repo;
mod transaction_repo;

use crate::output::output_accounts;
use crate::processor::PaymentProcessor;
use crate::repo::Clients;
use std::ops::Deref;
use std::sync::Arc;
use std::{env, process};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::ERROR)
        .init();

    // reading the input file path from the command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        error!("Usage: {} <input_file>", args[0]);
        process::exit(1);
    }
    let file_path = args[1].clone(); // Get the input file path

    // Create a channel for transactions
    let (tx, rx) = mpsc::channel(1);

    // Create a CancellationToken for graceful shutdown
    let shutdown_token = CancellationToken::new();

    // Task tracker to manage spawned tasks
    let tracker = TaskTracker::new();

    // Spawn the producer task
    let producer_shutdown_token = shutdown_token.clone();
    tracker.spawn(async move {
        let transaction_producer =
            producers::CSVTransactionProducer::new(file_path, producer_shutdown_token).clone();

        match transaction_producer.run(tx).await {
            Ok(_) => info!("CSV Producer finished"),
            Err(err) => error!(?err, "CSV Producer failed"),
        }
    });

    let clients = Arc::new(client_repo::InMemoryRepo::new());
    let transactions = transaction_repo::InMemoryRepo::new();

    // Spawn the processor task
    let processor_shutdown_token = shutdown_token.clone();
    let finished_shutdown_token = shutdown_token.clone();
    let processor_clients = Arc::clone(&clients);
    tracker.spawn(async move {
        let mut processor = PaymentProcessor::new(processor_clients.deref(), transactions);
        processor
            .process_transactions(rx, processor_shutdown_token)
            .await;

        finished_shutdown_token.cancel()
    });

    // Spawn a signal handler for graceful shutdown
    let shutdown_signal_token = shutdown_token.clone();
    tracker.spawn(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl+C, initiating shutdown...");
                shutdown_signal_token.cancel(); // Notify all tasks to stop
            }
            _ = shutdown_token.cancelled() => {
                info!("finished, initiating shutdown...");
                shutdown_signal_token.cancel(); // Notify all tasks to stop
            }
        }
    });

    // Wait for all tasks to complete or cancel
    tracker.close();
    tracker.wait().await;

    output_accounts(clients.stream_all()).await;

    info!("Application shutdown complete.");
    Ok(())
}
