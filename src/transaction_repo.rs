use crate::models::{StoredTransaction, Transaction, TransactionId, TransactionStatus};
use crate::repo::Transactions;
use async_trait::async_trait;
use std::collections::HashMap;

pub struct InMemoryRepo {
    transactions: HashMap<TransactionId, StoredTransaction>,
}

impl InMemoryRepo {
    pub fn new() -> Self {
        Self {
            transactions: HashMap::new(),
        }
    }
}

#[async_trait]
impl Transactions for InMemoryRepo {
    async fn get(&self, tx: &TransactionId) -> Option<StoredTransaction> {
        self.transactions.get(tx).cloned()
    }

    async fn save(&mut self, tx: &TransactionId, transaction: &Transaction) -> anyhow::Result<()> {
        self.transactions.insert(
            tx.clone(),
            StoredTransaction {
                transaction: transaction.clone(),
                status: TransactionStatus::Normal,
            },
        );
        Ok(())
    }

    async fn update_status(
        &mut self,
        tx: &TransactionId,
        status: TransactionStatus,
    ) -> anyhow::Result<()> {
        if let Some(t) = self.transactions.get_mut(tx) {
            t.status = status;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Transaction not found"))
        }
    }
}
