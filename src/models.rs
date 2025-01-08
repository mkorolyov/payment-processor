use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq, Hash, Eq)]
pub struct ClientId(pub u16);

impl Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq, Hash, Eq)]
pub struct TransactionId(pub u32);

impl Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct ClientTx {
    #[serde(rename = "client")]
    pub client_id: ClientId,
    #[serde(rename = "tx")]
    pub transaction_id: TransactionId,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Transaction {
    Deposit {
        #[serde(flatten)]
        client_tx: ClientTx,
        amount: rust_decimal::Decimal,
    },
    Withdrawal {
        #[serde(flatten)]
        client_tx: ClientTx,
        amount: rust_decimal::Decimal,
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

#[derive(Debug, Serialize, Default, Clone, PartialEq)]
pub struct ClientAssets {
    pub available: rust_decimal::Decimal,
    pub held: rust_decimal::Decimal,
    pub total: rust_decimal::Decimal,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct Client {
    pub client_id: ClientId,
    pub assets: ClientAssets,
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
