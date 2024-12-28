use crate::models::{Client, ClientTx, StoredTransaction, Transaction, TransactionStatus};
use crate::repo;
use anyhow::Result;
use std::pin::Pin;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

pub struct PaymentProcessor<C, T>
where
    C: repo::Clients,
    T: repo::Transactions,
{
    clients: C,
    transactions: T,
}
#[derive(Error, Debug)]
pub enum PaymentProcessorError {
    #[error("Internal error: {context:?}: {cause:?}")]
    Internal {
        context: String,
        cause: Option<anyhow::Error>,
    },
    #[error("Client not found")]
    ClientNotFound,
    #[error("Transaction not found")]
    TransactionNotFound,
    #[error("Insufficient funds")]
    InsufficientFunds,
    #[error("Unsupported transaction state")]
    UnsupportedTransactionState,
}

impl<C, T> PaymentProcessor<C, T>
where
    C: repo::Clients,
    T: repo::Transactions,
{
    pub fn new(clients: C, transactions: T) -> Self {
        Self {
            clients,
            transactions,
        }
    }

    pub async fn process_transactions(
        &mut self,
        mut rx: mpsc::Receiver<Transaction>,
        shutdown: CancellationToken,
    ) {
        while !shutdown.is_cancelled() {
            match rx.recv().await {
                Some(transaction) => {
                    if let Err(e) = self.process_transaction(transaction) {
                        error!("Failed to process transaction: {:?}", e);
                    }
                }
                None => {
                    info!("Transaction processing stopped due to channel closed.");
                    return;
                }
            }
        }

        info!("Transaction processing stopped due to shutdown.");
    }

    pub fn stream_clients(&self) -> Pin<Box<dyn Stream<Item = Client> + Send>> {
        self.clients.stream_all()
    }

    pub fn process_transaction(&mut self, tx: Transaction) -> Result<(), PaymentProcessorError> {
        match &tx {
            Transaction::Deposit { client_tx, amount } => {
                self.handle_deposit(&tx, client_tx, *amount)
            }
            Transaction::Withdrawal { client_tx, amount } => {
                self.handle_withdrawal(&tx, client_tx, *amount)
            }
            Transaction::Dispute { client_tx } => self.handle_dispute(client_tx),
            Transaction::Resolve { client_tx } => self.handle_resolve(client_tx),
            Transaction::Chargeback { client_tx } => self.handle_chargeback(client_tx),
        }
    }

    fn handle_deposit(
        &mut self,
        tx: &Transaction,
        common: &ClientTx,
        amount: f64,
    ) -> Result<(), PaymentProcessorError> {
        self.save_transaction(tx, common)?;

        let mut client = self
            .clients
            .get(common.client_id)
            .unwrap_or_else(|| Client {
                client: common.client_id,
                ..Default::default()
            });

        client.available += amount;
        client.total += amount;
        let res = self
            .clients
            .save(client)
            .map_err(|e| PaymentProcessorError::Internal {
                context: "Failed to save client".to_string(),
                cause: Some(e),
            });

        debug!(
            "Processed deposit: client={}, tx={}, amount={}",
            common.client_id, common.transaction_id, amount
        );

        res
    }

    fn handle_withdrawal(
        &mut self,
        tx: &Transaction,
        common: &ClientTx,
        amount: f64,
    ) -> Result<(), PaymentProcessorError> {
        let mut client_account = self.get_client_account(common.client_id)?;

        if client_account.available < amount {
            debug!(
                "Insufficient funds for withdrawal: client={}, tx={}, amount={}",
                common.client_id, common.transaction_id, amount
            );
            return Err(PaymentProcessorError::InsufficientFunds);
        }

        self.save_transaction(tx, common)?;

        client_account.available -= amount;
        client_account.total -= amount;
        // client safe operation could fail. for http service such partial consistency could be solved by retrying the request,
        // assuming we support same transaction safe operation retry.
        // for other services, we could use a message queue to retry the operation, or use a transactional database to ensure consistency.
        let res = self.save_client(client_account);

        debug!(
            "Processed withdrawal: client={}, tx={}, amount={}",
            common.client_id, common.transaction_id, amount
        );

        res
    }

    fn handle_dispute(&mut self, common: &ClientTx) -> Result<(), PaymentProcessorError> {
        let tx = self.get_stored_transaction(common)?;
        let mut client_account = self.get_client_account(common.client_id)?;

        match tx {
            StoredTransaction {
                status: TransactionStatus::Normal,
                transaction: Transaction::Deposit { amount, .. },
            } => {
                client_account.available -= amount;
                client_account.held += amount;
                debug!(
                    "Processed dispute for deposit: client={}, tx={}",
                    common.client_id, common.transaction_id
                );
            }

            StoredTransaction {
                status: TransactionStatus::Normal,
                transaction: Transaction::Withdrawal { amount, .. },
            } => {
                client_account.held += amount;
                debug!(
                    "Processed dispute for withdrawal: client={}, tx={}",
                    common.client_id, common.transaction_id
                );
            }

            _ => {
                debug!(
                    "Dispute failed: transaction type not supported for tx={}",
                    common.transaction_id
                );
                return Err(PaymentProcessorError::UnsupportedTransactionState);
            }
        }

        self.update_transaction_status(common, TransactionStatus::Disputed)?;
        // here if the client save operation fails, we could not retry the operation, as we already changed the transaction status.
        // to fix such cases in production complex solutions like 2PC, or sagas could be used.
        self.save_client(client_account)
    }

    fn handle_resolve(&mut self, common: &ClientTx) -> Result<(), PaymentProcessorError> {
        let tx = self.get_stored_transaction(common)?;
        let mut client_account = self.get_client_account(common.client_id)?;

        match tx {
            StoredTransaction {
                status: TransactionStatus::Disputed,
                transaction: Transaction::Deposit { amount, .. },
            } => {
                client_account.available += amount;
                client_account.held -= amount;
                debug!(
                    "Processed resolve for disputed deposit: client={}, tx={}",
                    common.client_id, common.transaction_id
                );
            }

            StoredTransaction {
                status: TransactionStatus::Disputed,
                transaction: Transaction::Withdrawal { amount, .. },
            } => {
                client_account.total += amount;
                client_account.available += amount;
                client_account.held -= amount;
                debug!(
                    "Processed resolve for disputed withdrawal: client={}, tx={}",
                    common.client_id, common.transaction_id
                );
            }

            _ => {
                debug!(
                    "Resolve failed: transaction type not supported for tx={}",
                    common.transaction_id
                );
                return Err(PaymentProcessorError::UnsupportedTransactionState);
            }
        }

        self.update_transaction_status(common, TransactionStatus::Resolved)?;
        // here if the client save operation fails, we could not retry the operation, as we already changed the transaction status.
        // to fix such cases in production complex solutions like 2PC, or sagas could be used.
        let res = self.save_client(client_account);

        info!(
            "Processed resolve: client={}, tx={}",
            common.client_id, common.transaction_id
        );

        res
    }

    fn handle_chargeback(&mut self, client_tx: &ClientTx) -> Result<(), PaymentProcessorError> {
        let tx = self.get_stored_transaction(client_tx)?;
        let mut client_account = self.get_client_account(client_tx.client_id)?;

        match tx {
            StoredTransaction {
                status: TransactionStatus::Disputed,
                transaction: Transaction::Deposit { amount, .. },
            } => {
                client_account.total -= amount;
                client_account.held -= amount;
            }

            StoredTransaction {
                status: TransactionStatus::Disputed,
                transaction: Transaction::Withdrawal { amount, .. },
            } => {
                client_account.total += amount;
                client_account.available += amount;
                client_account.held -= amount;
            }

            _ => {
                debug!(
                    "Chargeback failed: transaction type not supported for tx={}",
                    client_tx.transaction_id
                );
                return Err(PaymentProcessorError::UnsupportedTransactionState);
            }
        }

        client_account.locked = true;

        self.update_transaction_status(client_tx, TransactionStatus::Chargeback)?;
        // here if the client save operation fails, we could not retry the operation, as we already changed the transaction status.
        // to fix such cases in production complex solutions like 2PC, or sagas could be used.
        let res = self.save_client(client_account);

        debug!(
            "Processed chargeback: client={}, tx={}",
            client_tx.client_id, client_tx.transaction_id
        );

        res
    }

    fn get_client_account(&self, client_id: u16) -> Result<Client, PaymentProcessorError> {
        match self.clients.get(client_id) {
            Some(account) => Ok(account),
            None => {
                debug!("Client account not found for client={}", client_id);
                Err(PaymentProcessorError::ClientNotFound)
            }
        }
    }

    fn save_client(&mut self, client_account: Client) -> Result<(), PaymentProcessorError> {
        self.clients
            .save(client_account)
            .map_err(|e| PaymentProcessorError::Internal {
                context: "Failed to save client".to_string(),
                cause: Some(e),
            })
    }

    fn get_stored_transaction(
        &mut self,
        common: &ClientTx,
    ) -> Result<StoredTransaction, PaymentProcessorError> {
        match self.transactions.get(common.transaction_id) {
            Some(transaction) => Ok(transaction),
            None => {
                debug!(
                    "Dispute failed: transaction not found for tx={}",
                    common.transaction_id
                );
                Err(PaymentProcessorError::TransactionNotFound)
            }
        }
    }

    fn save_transaction(
        &mut self,
        tx: &Transaction,
        common: &ClientTx,
    ) -> Result<(), PaymentProcessorError> {
        self.transactions
            .save(common.transaction_id, tx)
            .map_err(|e| PaymentProcessorError::Internal {
                context: "Failed to save transaction".to_string(),
                cause: Some(e),
            })
    }

    fn update_transaction_status(
        &mut self,
        common: &ClientTx,
        status: TransactionStatus,
    ) -> Result<(), PaymentProcessorError> {
        self.transactions
            .update_status(common.transaction_id, status)
            .map_err(|e| PaymentProcessorError::Internal {
                context: "Failed to update transaction status".to_string(),
                cause: Some(e),
            })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::{MockClients, MockTransactions};
    use mockall::predicate::*;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn process_transactions_stops_on_shutdown() {
        let clients = MockClients::new();
        let transactions = MockTransactions::new();
        let (_tx, rx) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        let mut processor = PaymentProcessor::new(clients, transactions);
        shutdown.cancel();
        processor.process_transactions(rx, shutdown.clone()).await;

        assert!(shutdown.is_cancelled());
    }

    #[tokio::test]
    async fn process_transactions_handles_deposit() {
        let mut clients = MockClients::new();
        let mut transactions = MockTransactions::new();
        let (tx, mut rx) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        clients.expect_get().with(eq(1)).returning(|_| None);
        clients.expect_save().returning(|_| Ok(()));
        transactions.expect_save().returning(|_, _| Ok(()));

        let mut processor = PaymentProcessor::new(clients, transactions);
        tx.send(Transaction::Deposit {
            client_tx: ClientTx {
                client_id: 1,
                transaction_id: 1,
            },
            amount: 100.0,
        })
        .await
        .unwrap();
        rx.close();

        processor.process_transactions(rx, shutdown).await;
    }

    #[tokio::test]
    async fn process_transactions_handles_withdrawal_insufficient_funds() {
        let mut clients = MockClients::new();
        let mut transactions = MockTransactions::new();
        let (tx, mut rx) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        clients.expect_get().with(eq(1)).returning(|_| {
            Some(Client {
                client: 1,
                available: 50.0,
                held: 0.0,
                total: 50.0,
                locked: false,
            })
        });
        transactions.expect_save().returning(|_, _| Ok(()));

        let mut processor = PaymentProcessor::new(clients, transactions);
        tx.send(Transaction::Withdrawal {
            client_tx: ClientTx {
                client_id: 1,
                transaction_id: 1,
            },
            amount: 100.0,
        })
        .await
        .unwrap();
        rx.close();

        processor.process_transactions(rx, shutdown).await;
    }

    #[tokio::test]
    async fn process_transactions_handles_dispute() {
        let mut clients = MockClients::new();
        let mut transactions = MockTransactions::new();
        let (tx, mut rx) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        clients.expect_get().with(eq(1)).returning(|_| {
            Some(Client {
                client: 1,
                available: 100.0,
                held: 0.0,
                total: 100.0,
                locked: false,
            })
        });
        clients.expect_save().returning(|_| Ok(()));
        transactions.expect_get().with(eq(1)).returning(|_| {
            Some(StoredTransaction {
                status: TransactionStatus::Normal,
                transaction: Transaction::Deposit {
                    client_tx: ClientTx {
                        client_id: 1,
                        transaction_id: 1,
                    },
                    amount: 100.0,
                },
            })
        });
        transactions.expect_update_status().returning(|_, _| Ok(()));

        let mut processor = PaymentProcessor::new(clients, transactions);
        tx.send(Transaction::Dispute {
            client_tx: ClientTx {
                client_id: 1,
                transaction_id: 1,
            },
        })
        .await
        .unwrap();
        rx.close();

        processor.process_transactions(rx, shutdown).await;
    }
}
