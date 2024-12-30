use crate::models::{
    ClientAssets, ClientId, ClientTx, StoredTransaction, Transaction, TransactionId,
    TransactionStatus,
};
use crate::repo;
use anyhow::Result;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub struct PaymentProcessor<'a, C, T>
where
    C: repo::Clients,
    T: repo::Transactions,
{
    clients: &'a C,
    transactions: T,
}
#[derive(Error, Debug)]
pub enum PaymentProcessorError {
    #[error("Internal error: {context:?}: {cause:?}")]
    Internal {
        context: String,
        cause: Option<anyhow::Error>,
    },
    #[error("Transaction not found")]
    TransactionNotFound,
    #[error("Unsupported transaction state")]
    UnsupportedTransactionState,
}

impl<'a, C, T> PaymentProcessor<'a, C, T>
where
    C: repo::Clients,
    T: repo::Transactions,
{
    pub fn new(clients: &'a C, transactions: T) -> Self {
        Self {
            clients,
            transactions,
        }
    }

    // process_transactions processes transactions from the channel until it is closed or a shutdown signal is received.
    pub async fn process_transactions(
        &mut self,
        mut rx: mpsc::Receiver<Transaction>,
        shutdown: CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Transaction processing stopped due to shutdown.");
                    return;
                }
                message = rx.recv() => {
                    match message {
                        Some(transaction) => {
                            if let Err(err) = self.process_transaction(transaction).await {
                                // TODO: enrich error handling and retry failed transactions if needed
                                // TODO: or propagate the error to the caller
                                warn!(?err, "Failed to process transaction");
                            }
                        }
                        None => {
                            info!("Transaction processing stopped due to channel closed.");
                            return;
                        }
                    }
                }
            }
        }
    }

    pub async fn process_transaction(
        &mut self,
        tx: Transaction,
    ) -> Result<(), PaymentProcessorError> {
        match &tx {
            Transaction::Deposit { client_tx, amount } => {
                self.handle_deposit(&tx, client_tx, *amount).await
            }
            Transaction::Withdrawal { client_tx, amount } => {
                self.handle_withdrawal(&tx, client_tx, *amount).await
            }
            Transaction::Dispute { client_tx } => self.handle_dispute(client_tx).await,
            Transaction::Resolve { client_tx } => self.handle_resolve(client_tx).await,
            Transaction::Chargeback { client_tx } => self.handle_chargeback(client_tx).await,
        }
    }

    async fn handle_deposit(
        &mut self,
        tx: &Transaction,
        client_tx: &ClientTx,
        amount: f64,
    ) -> Result<(), PaymentProcessorError> {
        self.save_transaction(tx, &client_tx.transaction_id).await?;

        // adding the deposit amount to the client's available and total assets
        let diff = ClientAssets {
            available: amount,
            held: 0.0,
            total: amount,
        };
        // in case if the client update operation fails, we could retry the operation,
        // as the transaction stored in append only manner and will not be duplicated by transaction_id.
        self.calc_client_account(&client_tx.client_id, diff).await?;

        debug!(client_id = %client_tx.client_id, transaction_id = %client_tx.transaction_id, amount, "Processed deposit");
        Ok(())
    }

    async fn handle_withdrawal(
        &mut self,
        tx: &Transaction,
        client_tx: &ClientTx,
        amount: f64,
    ) -> Result<(), PaymentProcessorError> {
        self.save_transaction(tx, &client_tx.transaction_id).await?;

        // subtracting the withdrawal amount from the client's available and total assets
        let diff = ClientAssets {
            available: -amount,
            held: 0.0,
            total: -amount,
        };
        // in case if the client update operation fails, we could retry the operation,
        // as the transaction stored in append only manner and will not be duplicated by transaction_id.
        self.calc_client_account(&client_tx.client_id, diff).await?;

        debug!(client_id = %client_tx.client_id, transaction_id = %client_tx.transaction_id, amount, "Processed withdrawal");
        Ok(())
    }

    async fn handle_dispute(&mut self, client_tx: &ClientTx) -> Result<(), PaymentProcessorError> {
        let tx = self.get_stored_transaction(client_tx).await?;

        let diff = match tx {
            StoredTransaction {
                status: TransactionStatus::Normal | TransactionStatus::Disputed,
                transaction: Transaction::Deposit { amount, .. },
            } => ClientAssets {
                // transaction the amount should be held, available should be decreased
                available: -amount,
                held: amount,
                total: 0.0,
            },

            StoredTransaction {
                status: TransactionStatus::Normal | TransactionStatus::Disputed,
                transaction: Transaction::Withdrawal { amount, .. },
            } => ClientAssets {
                // amount should be held
                available: 0.0,
                held: amount,
                total: 0.0,
            },

            _ => {
                debug!(transaction_id = %client_tx.transaction_id, "Dispute failed: transaction type not supported");
                return Err(PaymentProcessorError::UnsupportedTransactionState);
            }
        };

        // here we can process retries in case of failure, thus updating status from disputed to disputed should be handled correctly.
        self.update_transaction_status(&client_tx.transaction_id, TransactionStatus::Disputed)
            .await?;
        // if this operation fails, we could retry the operation, as the transaction status update is also retriable.
        self.calc_client_account(&client_tx.client_id, diff).await?;

        info!(client_id = %client_tx.client_id, transaction_id = %client_tx.transaction_id, "Processed dispute");
        Ok(())
    }

    async fn handle_resolve(&mut self, client_tx: &ClientTx) -> Result<(), PaymentProcessorError> {
        let tx = self.get_stored_transaction(client_tx).await?;

        let diff = match tx {
            StoredTransaction {
                status: TransactionStatus::Disputed | TransactionStatus::Resolved,
                transaction: Transaction::Deposit { amount, .. },
            } =>
            // amount should be released from held and added to available
            {
                ClientAssets {
                    available: amount,
                    held: -amount,
                    total: 0.0,
                }
            }

            StoredTransaction {
                status: TransactionStatus::Disputed | TransactionStatus::Resolved,
                transaction: Transaction::Withdrawal { amount, .. },
            } =>
            // amount should be released from held
            {
                ClientAssets {
                    available: 0.0,
                    held: -amount,
                    total: 0.0,
                }
            }

            _ => {
                debug!(transaction_id = %client_tx.transaction_id, "Resolve failed: transaction type not supported");
                return Err(PaymentProcessorError::UnsupportedTransactionState);
            }
        };

        // here we can process retries in case of failure, thus updating status from resolved to resolved should be just ignored.
        self.update_transaction_status(&client_tx.transaction_id, TransactionStatus::Resolved)
            .await?;
        // if this operation fails, we could retry the operation, as the transaction status update is also retriable.
        self.calc_client_account(&client_tx.client_id, diff).await?;

        info!(client_id = %client_tx.client_id, transaction_id = %client_tx.transaction_id, "Processed resolve",);

        Ok(())
    }

    async fn handle_chargeback(
        &mut self,
        client_tx: &ClientTx,
    ) -> Result<(), PaymentProcessorError> {
        let tx = self.get_stored_transaction(client_tx).await?;

        let diff = match tx {
            StoredTransaction {
                status: TransactionStatus::Disputed | TransactionStatus::Chargeback,
                transaction: Transaction::Deposit { amount, .. },
            } =>
            // amount should be released from held and deducted from available
            {
                ClientAssets {
                    available: 0.0,
                    held: -amount,
                    total: -amount,
                }
            }

            StoredTransaction {
                status: TransactionStatus::Disputed | TransactionStatus::Chargeback,
                transaction: Transaction::Withdrawal { amount, .. },
            } => ClientAssets {
                // amount should be released from held and added back to available and total
                available: amount,
                held: -amount,
                total: amount,
            },

            _ => {
                debug!(transaction_id = %client_tx.transaction_id, "Chargeback failed: transaction type not supported");
                return Err(PaymentProcessorError::UnsupportedTransactionState);
            }
        };

        // here we can process retries in case of failure, thus updating status from chargeback to chargeback should be handled correctly.
        self.update_transaction_status(&client_tx.transaction_id, TransactionStatus::Chargeback)
            .await?;
        // if this operation fails, we could retry the operation, as the transaction status update is also retriable.
        self.lock_account(&client_tx.client_id, diff).await?;

        debug!(client_id = %client_tx.client_id, transaction_id = %client_tx.transaction_id, "Processed chargeback");
        Ok(())
    }

    async fn calc_client_account(
        &mut self,
        client_id: &ClientId,
        diff: ClientAssets,
    ) -> Result<(), PaymentProcessorError> {
        self.clients
            .calc_client_account(client_id, diff)
            .await
            .map_err(|e| PaymentProcessorError::Internal {
                context: "Failed to compute sum".to_string(),
                cause: Some(e),
            })
    }

    async fn lock_account(
        &mut self,
        client_id: &ClientId,
        diff: ClientAssets,
    ) -> Result<(), PaymentProcessorError> {
        self.clients
            .lock(client_id, diff)
            .await
            .map_err(|e| PaymentProcessorError::Internal {
                context: "Failed to compute sum".to_string(),
                cause: Some(e),
            })
    }

    async fn get_stored_transaction(
        &mut self,
        client_tx: &ClientTx,
    ) -> Result<StoredTransaction, PaymentProcessorError> {
        match self.transactions.get(&client_tx.transaction_id).await {
            Some(transaction) => Ok(transaction),
            None => {
                debug!(transaction_id = %client_tx.transaction_id, "Dispute failed: transaction not found");
                Err(PaymentProcessorError::TransactionNotFound)
            }
        }
    }

    async fn save_transaction(
        &mut self,
        tx: &Transaction,
        transaction_id: &TransactionId,
    ) -> Result<(), PaymentProcessorError> {
        self.transactions
            .save(transaction_id, tx)
            .await
            .map_err(|e| PaymentProcessorError::Internal {
                context: "Failed to save transaction".to_string(),
                cause: Some(e),
            })
    }

    async fn update_transaction_status(
        &mut self,
        transaction_id: &TransactionId,
        status: TransactionStatus,
    ) -> Result<(), PaymentProcessorError> {
        self.transactions
            .update_status(transaction_id, status)
            .await
            .map_err(|e| PaymentProcessorError::Internal {
                context: "Failed to update transaction status".to_string(),
                cause: Some(e),
            })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Client;
    use crate::repo::{Clients, MockClients, MockTransactions};
    use crate::{client_repo, transaction_repo};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn process_transactions_stops_on_channel_closed() {
        let clients = MockClients::new();
        let transactions = MockTransactions::new();
        let (_tx, mut rx) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        let mut processor = PaymentProcessor::new(&clients, transactions);
        rx.close();
        processor.process_transactions(rx, shutdown.clone()).await;
    }

    #[tokio::test]
    async fn process_transactions_stops_on_shutdown() {
        let clients = MockClients::new();
        let transactions = MockTransactions::new();
        let (_tx, rx) = mpsc::channel(1);
        let shutdown = CancellationToken::new();

        let mut processor = PaymentProcessor::new(&clients, transactions);
        shutdown.cancel();
        processor.process_transactions(rx, shutdown.clone()).await;
    }

    #[tokio::test]
    async fn process_transaction_handles_deposit() {
        let clients = client_repo::InMemoryRepo::new();
        let mut transactions = MockTransactions::new();
        let client_tx = gen_client_tx();
        let tx = Transaction::Deposit {
            client_tx,
            amount: 100.0,
        };

        transactions.expect_save().returning(|_, _| Ok(()));

        let mut processor = PaymentProcessor::new(&clients, transactions);

        assert!(
            processor.process_transaction(tx).await.is_ok(),
            "Failed to process deposit"
        );
        let client = clients.get(&ClientId(1)).await.unwrap();
        assert_eq!(
            client.assets,
            ClientAssets {
                available: 100.0,
                held: 0.0,
                total: 100.0,
            }
        );
    }

    #[tokio::test]
    async fn process_transaction_handles_withdrawal_insuffucient_funds_empty_client() {
        let clients = client_repo::InMemoryRepo::new();
        let mut transactions = MockTransactions::new();
        let client_tx = gen_client_tx();
        let tx = Transaction::Withdrawal {
            client_tx,
            amount: 100.0,
        };

        transactions.expect_save().returning(|_, _| Ok(()));

        let mut processor = PaymentProcessor::new(&clients, transactions);

        assert!(
            processor.process_transaction(tx).await.is_err(),
            "Withdrawal should fail"
        );
    }

    #[tokio::test]
    async fn process_transaction_handles_withdrawal() -> Result<(), PaymentProcessorError> {
        let clients = client_repo::InMemoryRepo::new();
        let mut transactions = MockTransactions::new();

        transactions.expect_save().returning(|_, _| Ok(()));

        let client_tx = gen_client_tx();
        let mut processor = PaymentProcessor::new(&clients, transactions);
        processor
            .process_transaction(Transaction::Deposit {
                client_tx,
                amount: 200.0,
            })
            .await?;

        let tx = Transaction::Withdrawal {
            client_tx: ClientTx {
                client_id: ClientId(1),
                transaction_id: TransactionId(2),
            },
            amount: 100.0,
        };

        assert!(
            processor.process_transaction(tx).await.is_ok(),
            "Failed to process withdrawal"
        );
        let got = clients.get(&ClientId(1)).await.unwrap();
        assert_eq!(
            got.assets,
            ClientAssets {
                available: 100.0,
                held: 0.0,
                total: 100.0,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn process_transaction_handles_deposit_dispute() -> Result<(), PaymentProcessorError> {
        let clients = client_repo::InMemoryRepo::new();
        let transactions = transaction_repo::InMemoryRepo::new();

        let mut processor = PaymentProcessor::new(&clients, transactions);
        let client_tx = gen_client_tx();
        processor
            .process_transaction(Transaction::Deposit {
                client_tx: client_tx.clone(),
                amount: 200.0,
            })
            .await?;

        let tx = Transaction::Dispute { client_tx };

        assert!(
            processor.process_transaction(tx).await.is_ok(),
            "Failed to process dispute"
        );
        let got = clients.get(&ClientId(1)).await.unwrap();
        assert_eq!(
            got.assets,
            ClientAssets {
                available: 0.0,
                held: 200.0,
                total: 200.0,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn process_transaction_handles_deposit_dispute_resolve(
    ) -> Result<(), PaymentProcessorError> {
        let clients = client_repo::InMemoryRepo::new();
        let transactions = transaction_repo::InMemoryRepo::new();

        let mut processor = PaymentProcessor::new(&clients, transactions);
        let client_tx = gen_client_tx();
        processor
            .process_transaction(Transaction::Deposit {
                client_tx: client_tx.clone(),
                amount: 200.0,
            })
            .await?;

        processor
            .process_transaction(Transaction::Dispute {
                client_tx: client_tx.clone(),
            })
            .await?;

        let tx = Transaction::Resolve { client_tx };

        assert!(
            processor.process_transaction(tx).await.is_ok(),
            "Failed to process resolve"
        );
        let got = clients.get(&ClientId(1)).await.unwrap();
        assert_eq!(
            got.assets,
            ClientAssets {
                available: 200.0,
                held: 0.0,
                total: 200.0,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn process_transaction_handles_deposit_dispute_chargeback(
    ) -> Result<(), PaymentProcessorError> {
        let clients = client_repo::InMemoryRepo::new();
        let transactions = transaction_repo::InMemoryRepo::new();

        let mut processor = PaymentProcessor::new(&clients, transactions);
        let client_tx = gen_client_tx();
        processor
            .process_transaction(Transaction::Deposit {
                client_tx: client_tx.clone(),
                amount: 200.0,
            })
            .await?;

        processor
            .process_transaction(Transaction::Dispute {
                client_tx: client_tx.clone(),
            })
            .await?;

        let tx = Transaction::Chargeback { client_tx };

        assert!(
            processor.process_transaction(tx).await.is_ok(),
            "Failed to process chargeback"
        );
        let got = clients.get(&ClientId(1)).await.unwrap();
        assert_eq!(
            got,
            Client {
                client_id: ClientId(1),
                assets: ClientAssets {
                    available: 0.0,
                    held: 0.0,
                    total: 0.0,
                },
                locked: true,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn process_transaction_handles_withdrawal_dispute() -> Result<(), PaymentProcessorError> {
        let clients = client_repo::InMemoryRepo::new();
        let transactions = transaction_repo::InMemoryRepo::new();

        clients
            .calc_client_account(
                &ClientId(1),
                ClientAssets {
                    available: 200.0,
                    held: 0.0,
                    total: 200.0,
                },
            )
            .await
            .unwrap();

        let mut processor = PaymentProcessor::new(&clients, transactions);
        let client_tx = gen_client_tx();
        processor
            .process_transaction(Transaction::Withdrawal {
                client_tx: client_tx.clone(),
                amount: 200.0,
            })
            .await?;

        let got = clients.get(&ClientId(1)).await.unwrap();
        assert_eq!(
            got.assets,
            ClientAssets {
                available: 0.0,
                held: 0.0,
                total: 0.0,
            }
        );

        let tx = Transaction::Dispute { client_tx };

        assert!(
            processor.process_transaction(tx).await.is_ok(),
            "Failed to process dispute"
        );
        let got = clients.get(&ClientId(1)).await.unwrap();
        assert_eq!(
            got.assets,
            ClientAssets {
                available: 0.0,
                held: 200.0,
                total: 0.0,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn process_transaction_handles_withdrawal_dispute_resolve(
    ) -> Result<(), PaymentProcessorError> {
        let clients = client_repo::InMemoryRepo::new();
        let transactions = transaction_repo::InMemoryRepo::new();

        clients
            .calc_client_account(
                &ClientId(1),
                ClientAssets {
                    available: 200.0,
                    held: 0.0,
                    total: 200.0,
                },
            )
            .await
            .unwrap();

        let mut processor = PaymentProcessor::new(&clients, transactions);
        let client_tx = gen_client_tx();
        processor
            .process_transaction(Transaction::Withdrawal {
                client_tx: client_tx.clone(),
                amount: 200.0,
            })
            .await?;

        processor
            .process_transaction(Transaction::Dispute {
                client_tx: client_tx.clone(),
            })
            .await?;

        let tx = Transaction::Resolve { client_tx };

        assert!(
            processor.process_transaction(tx).await.is_ok(),
            "Failed to process resolve"
        );
        let got = clients.get(&ClientId(1)).await.unwrap();
        assert_eq!(
            got.assets,
            ClientAssets {
                available: 0.0,
                held: 0.0,
                total: 0.0,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn process_transaction_handles_withdrawal_dispute_chargeback(
    ) -> Result<(), PaymentProcessorError> {
        let clients = client_repo::InMemoryRepo::new();
        let transactions = transaction_repo::InMemoryRepo::new();

        clients
            .calc_client_account(
                &ClientId(1),
                ClientAssets {
                    available: 200.0,
                    held: 0.0,
                    total: 200.0,
                },
            )
            .await
            .unwrap();

        let mut processor = PaymentProcessor::new(&clients, transactions);
        let client_tx = gen_client_tx();
        processor
            .process_transaction(Transaction::Withdrawal {
                client_tx: client_tx.clone(),
                amount: 200.0,
            })
            .await?;

        processor
            .process_transaction(Transaction::Dispute {
                client_tx: client_tx.clone(),
            })
            .await?;

        let tx = Transaction::Chargeback { client_tx };

        assert!(
            processor.process_transaction(tx).await.is_ok(),
            "Failed to process chargeback"
        );
        let got = clients.get(&ClientId(1)).await.unwrap();
        assert_eq!(
            got,
            Client {
                client_id: ClientId(1),
                assets: ClientAssets {
                    available: 200.0,
                    held: 0.0,
                    total: 200.0,
                },
                locked: true,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn process_transaction_handles_dispute_for_unknown() {
        let clients = client_repo::InMemoryRepo::new();
        let transactions = transaction_repo::InMemoryRepo::new();

        let mut processor = PaymentProcessor::new(&clients, transactions);
        let client_tx = gen_client_tx();
        assert!(
            processor
                .process_transaction(Transaction::Dispute { client_tx })
                .await
                .is_err(),
            "Dispute should fail for unknown transaction"
        );
    }

    #[tokio::test]
    async fn process_transaction_handles_resolve_for_unknown() {
        let clients = client_repo::InMemoryRepo::new();
        let transactions = transaction_repo::InMemoryRepo::new();

        let mut processor = PaymentProcessor::new(&clients, transactions);
        let client_tx = gen_client_tx();
        assert!(
            processor
                .process_transaction(Transaction::Resolve { client_tx })
                .await
                .is_err(),
            "Resolve should fail for unknown transaction"
        );
    }

    #[tokio::test]
    async fn process_transaction_handles_chargeback_for_unknown() {
        let clients = client_repo::InMemoryRepo::new();
        let transactions = transaction_repo::InMemoryRepo::new();

        let mut processor = PaymentProcessor::new(&clients, transactions);
        let client_tx = gen_client_tx();
        assert!(
            processor
                .process_transaction(Transaction::Chargeback { client_tx })
                .await
                .is_err(),
            "Chargeback should fail for unknown transaction"
        );
    }

    #[tokio::test]
    async fn process_transaction_handles_resolve_for_invalid_state() {
        let clients = client_repo::InMemoryRepo::new();
        let transactions = transaction_repo::InMemoryRepo::new();

        let mut processor = PaymentProcessor::new(&clients, transactions);
        let client_tx = gen_client_tx();
        processor
            .process_transaction(Transaction::Deposit {
                client_tx: client_tx.clone(),
                amount: 200.0,
            })
            .await
            .unwrap();

        assert!(
            processor
                .process_transaction(Transaction::Resolve { client_tx })
                .await
                .is_err(),
            "Resolve should fail for invalid state"
        );
    }

    #[tokio::test]
    async fn process_transaction_handles_chargeback_for_invalid_state() {
        let clients = client_repo::InMemoryRepo::new();
        let transactions = transaction_repo::InMemoryRepo::new();

        let mut processor = PaymentProcessor::new(&clients, transactions);
        let client_tx = gen_client_tx();
        processor
            .process_transaction(Transaction::Deposit {
                client_tx: client_tx.clone(),
                amount: 200.0,
            })
            .await
            .unwrap();

        assert!(
            processor
                .process_transaction(Transaction::Chargeback { client_tx })
                .await
                .is_err(),
            "Chargeback should fail for invalid state"
        );
    }

    fn gen_client_tx() -> ClientTx {
        ClientTx {
            client_id: ClientId(1),
            transaction_id: TransactionId(1),
        }
    }
}
