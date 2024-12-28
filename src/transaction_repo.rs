use crate::models::{StoredTransaction, Transaction, TransactionStatus};
use crate::repo::Transactions;
use std::collections::HashMap;

pub struct InMemoryRepo {
    transactions: HashMap<u32, StoredTransaction>,
}

impl InMemoryRepo {
    pub fn new() -> Self {
        Self {
            transactions: HashMap::new(),
        }
    }
}

impl Transactions for InMemoryRepo {
    fn get(&self, tx: u32) -> Option<StoredTransaction> {
        self.transactions.get(&tx).cloned()
    }

    fn save(&mut self, tx: u32, transaction: &Transaction) -> anyhow::Result<()> {
        self.transactions.insert(
            tx,
            StoredTransaction {
                transaction: transaction.clone(),
                status: TransactionStatus::Normal,
            },
        );
        Ok(())
    }

    fn update_status(&mut self, tx: u32, status: TransactionStatus) -> anyhow::Result<()> {
        if let Some(t) = self.transactions.get_mut(&tx) {
            t.status = status;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Transaction not found"))
        }
    }
}
