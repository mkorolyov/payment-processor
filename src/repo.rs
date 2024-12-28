use crate::models;
use crate::models::{Client, StoredTransaction, Transaction, TransactionStatus};
use mockall::automock;
use std::pin::Pin;
use tokio_stream::Stream;

#[automock]
pub trait Transactions {
    fn get(&self, tx: u32) -> Option<StoredTransaction>;
    fn save(&mut self, tx: u32, transaction: &Transaction) -> anyhow::Result<()>;
    fn update_status(&mut self, tx: u32, status: TransactionStatus) -> anyhow::Result<()>;
}

#[automock]
pub trait Clients {
    fn get(&self, client_id: u16) -> Option<Client>;
    fn save(&mut self, client: Client) -> anyhow::Result<()>;
    fn stream_all(&self) -> Pin<Box<dyn Stream<Item = models::Client> + Send>>;
}
