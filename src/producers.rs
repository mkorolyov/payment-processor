use crate::models;
use crate::models::{ClientId, Transaction, TransactionId};
use anyhow::Result;
use csv::ReaderBuilder;
use std::fs::File;
use std::io::BufReader;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct CSVTransactionProducer {
    file_path: String,
    shutdown: CancellationToken,
}

impl CSVTransactionProducer {
    pub fn new(file_path: String, shutdown: CancellationToken) -> Self {
        Self {
            file_path,
            shutdown,
        }
    }

    pub async fn run(&self, tx: Sender<Transaction>) -> Result<()> {
        let file = File::open(&self.file_path)?;
        let reader = BufReader::new(file);
        let mut csv_reader = ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_reader(reader);

        for result in csv_reader.deserialize::<RawTransaction>() {
            if self.shutdown.is_cancelled() {
                info!("CSV Producer shutting down");
                break;
            }

            let raw_transaction = match result {
                Ok(raw) => raw,
                Err(err) => {
                    error!(?err, "Failed to parse CSV row");
                    continue;
                }
            };

            let transaction = match Transaction::try_from(raw_transaction) {
                Ok(transaction) => transaction,
                Err(err) => {
                    error!(?err, "Failed to parse transaction");
                    continue;
                }
            };

            if let Err(err) = tx.send(transaction).await {
                warn!(?err, "Transaction channel closed, stopping CSV Producer");
                return Ok(());
            }
        }

        Ok(())
    }
}

// Direct deserialization of CSV rows into Transaction is not supported by Serde
// We need to first deserialize into a RawTransaction and then convert it into a Transaction
// more details: https://github.com/BurntSushi/rust-csv/issues/211
#[derive(Debug, serde::Deserialize)]
struct RawTransaction {
    #[serde(rename = "type")]
    transaction_type: String,
    client: ClientId,
    tx: TransactionId,
    amount: Option<f64>,
}

impl TryFrom<Result<RawTransaction, csv::Error>> for Transaction {
    type Error = anyhow::Error;

    fn try_from(raw: Result<RawTransaction, csv::Error>) -> Result<Self> {
        match raw {
            Ok(raw) => Transaction::try_from(raw),
            Err(e) => Err(e.into()),
        }
    }
}

impl TryFrom<RawTransaction> for Transaction {
    type Error = anyhow::Error;

    fn try_from(raw: RawTransaction) -> Result<Self> {
        match raw.transaction_type.as_str() {
            "deposit" => {
                let amount = raw
                    .amount
                    .ok_or_else(|| anyhow::anyhow!("Missing amount"))?;
                Ok(Transaction::Deposit {
                    client_tx: models::ClientTx {
                        client_id: raw.client,
                        transaction_id: raw.tx,
                    },
                    amount,
                })
            }
            "withdrawal" => {
                let amount = raw
                    .amount
                    .ok_or_else(|| anyhow::anyhow!("Missing amount"))?;
                Ok(Transaction::Withdrawal {
                    client_tx: models::ClientTx {
                        client_id: raw.client,
                        transaction_id: raw.tx,
                    },
                    amount,
                })
            }
            "dispute" => Ok(Transaction::Dispute {
                client_tx: models::ClientTx {
                    client_id: raw.client,
                    transaction_id: raw.tx,
                },
            }),
            "resolve" => Ok(Transaction::Resolve {
                client_tx: models::ClientTx {
                    client_id: raw.client,
                    transaction_id: raw.tx,
                },
            }),
            "chargeback" => Ok(Transaction::Chargeback {
                client_tx: models::ClientTx {
                    client_id: raw.client,
                    transaction_id: raw.tx,
                },
            }),
            _ => Err(anyhow::anyhow!(
                "Unknown transaction type: {}",
                raw.transaction_type
            )),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ClientTx;
    use csv::ReaderBuilder;
    use std::fmt;

    struct TestCase {
        name: &'static str,            // Name of the test case
        input: &'static str,           // Input CSV data
        expected: Option<Transaction>, // Expected parsed transaction or None for errors
    }

    impl fmt::Debug for TestCase {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "TestCase {{ name: {} }}", self.name)
        }
    }

    #[tokio::test]
    async fn test_csv_parsing() {
        let test_cases = vec![
            TestCase {
                name: "Valid Deposit",
                input: "type,client,tx,amount\ndeposit,1,1,100.0",
                expected: Some(Transaction::Deposit {
                    client_tx: ClientTx {
                        client_id: ClientId(1),
                        transaction_id: TransactionId(1),
                    },
                    amount: 100.0,
                }),
            },
            TestCase {
                name: "Valid Withdrawal",
                input: "type,client,tx,amount\nwithdrawal,2,2,50.0",
                expected: Some(Transaction::Withdrawal {
                    client_tx: ClientTx {
                        client_id: ClientId(2),
                        transaction_id: TransactionId(2),
                    },
                    amount: 50.0,
                }),
            },
            TestCase {
                name: "Valid Dispute",
                input: "type,client,tx,amount\ndispute,3,3,",
                expected: Some(Transaction::Dispute {
                    client_tx: ClientTx {
                        client_id: ClientId(3),
                        transaction_id: TransactionId(3),
                    },
                }),
            },
            TestCase {
                name: "Valid Resolve",
                input: "type,client,tx,amount\nresolve,4,4,",
                expected: Some(Transaction::Resolve {
                    client_tx: ClientTx {
                        client_id: ClientId(4),
                        transaction_id: TransactionId(4),
                    },
                }),
            },
            TestCase {
                name: "Valid Chargeback",
                input: "type,client,tx,amount\nchargeback,5,5,",
                expected: Some(Transaction::Chargeback {
                    client_tx: ClientTx {
                        client_id: ClientId(5),
                        transaction_id: TransactionId(5),
                    },
                }),
            },
            TestCase {
                name: "Invalid Transaction Type",
                input: "type,client,tx,amount\ninvalid,6,6,",
                expected: None, // Invalid type should result in an error
            },
            TestCase {
                name: "Missing Amount for Deposit",
                input: "type,client,tx,amount\ndeposit,7,7,", // Missing amount
                expected: None,
            },
            TestCase {
                name: "Missing Amount for Withdrawal",
                input: "type,client,tx,amount\nwithdrawal,8,8,", // Missing amount
                expected: None,
            },
        ];

        for case in &test_cases {
            let mut reader = ReaderBuilder::new()
                .trim(csv::Trim::All)
                .from_reader(case.input.as_bytes());

            let raw = reader.deserialize::<RawTransaction>().next().unwrap();
            let parsed_transaction = Transaction::try_from(raw);

            let expected = case.expected.clone();

            if let Some(expected_transaction) = expected {
                match parsed_transaction {
                    Ok(parsed) => assert_eq!(parsed, expected_transaction),
                    Err(e) => panic!(
                        "Test case '{}' failed: expected {:?}, got error {:?}",
                        case.name, expected_transaction, e
                    ),
                }
            } else {
                assert!(
                    parsed_transaction.is_err(),
                    "Test case '{}' failed: expected error, got {:?}",
                    case.name,
                    parsed_transaction
                );
            }
        }
    }
}
