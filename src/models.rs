use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct ClientTx {
    #[serde(rename = "client")]
    pub client_id: u16,
    #[serde(rename = "tx")]
    pub transaction_id: u32,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Transaction {
    Deposit {
        #[serde(flatten)]
        client_tx: ClientTx,
        amount: f64,
    },
    Withdrawal {
        #[serde(flatten)]
        client_tx: ClientTx,
        amount: f64,
    },
    Dispute {
        #[serde(flatten)]
        client_tx: ClientTx,
    },
    Resolve {
        #[serde(flatten)]
        client_tx: ClientTx,
    },
    Chargeback {
        #[serde(flatten)]
        client_tx: ClientTx,
    },
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct Client {
    pub client: u16,
    pub available: f64,
    pub held: f64,
    pub total: f64,
    pub locked: bool,
}

#[derive(Clone, PartialEq)]
pub enum TransactionStatus {
    Normal,
    Disputed,
    Resolved,
    Chargeback,
}

#[derive(Clone)]
pub struct StoredTransaction {
    pub transaction: Transaction,
    pub status: TransactionStatus,
}
