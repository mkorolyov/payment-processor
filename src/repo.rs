use crate::models::{
    Client, ClientAssets, ClientId, StoredTransaction, Transaction, TransactionId,
    TransactionStatus,
};
use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;
use std::pin::Pin;
use tokio_stream::Stream;

#[cfg_attr(test, automock)]
#[async_trait]
pub trait Transactions {
    async fn get(&self, tx: &TransactionId) -> Option<StoredTransaction>;
    async fn save(&mut self, tx: &TransactionId, transaction: &Transaction) -> anyhow::Result<()>;
    async fn update_status(
        &mut self,
        tx: &TransactionId,
        status: TransactionStatus,
    ) -> anyhow::Result<()>;
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait Clients {
    #[allow(dead_code)]
    async fn get(&self, client_id: &ClientId) -> Option<Client>;
    async fn calc_client_account(
        &self,
        client_id: &ClientId,
        diff: ClientAssets,
    ) -> anyhow::Result<()>;
    // lock will not only lock the client accout but also calculate the assets state in same way as calc_client_account function.
    async fn lock(&self, client_id: &ClientId, diff: ClientAssets) -> anyhow::Result<()>;
    fn stream_all(&self) -> Pin<Box<dyn Stream<Item = Client> + Send>>;
}
