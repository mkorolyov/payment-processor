use crate::models::{Client, ClientAssets, ClientId};
use crate::repo::Clients;
use anyhow::Result;
use async_trait::async_trait;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::RwLock;
use tokio_stream::Stream;

pub struct InMemoryRepo {
    clients: RwLock<HashMap<ClientId, Client>>,
}

impl InMemoryRepo {
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
        }
    }

    fn add_diff(diff: &ClientAssets, client: &Client) -> Result<Client> {
        // check if the client has enough funds
        if client.assets.available + diff.available < dec!(0.0)
            || client.assets.total + diff.total < dec!(0.0)
        {
            return Err(anyhow::anyhow!("Insufficient funds"));
        }

        let new_client = Client {
            client_id: client.client_id.clone(),
            assets: ClientAssets {
                total: client.assets.total + diff.total,
                held: client.assets.held + diff.held,
                available: client.assets.available + diff.available,
            },
            locked: client.locked,
        };
        Ok(new_client)
    }

    fn thread_safe_get(&self, client_id: &ClientId) -> Option<Client> {
        self.clients.read().unwrap().get(client_id).cloned()
    }

    fn thread_safe_insert(&self, client_id: ClientId, client: Client) {
        self.clients.write().unwrap().insert(client_id, client);
    }
}

#[async_trait]
impl Clients for InMemoryRepo {
    async fn get(&self, client_id: &ClientId) -> Option<Client> {
        self.thread_safe_get(client_id)
    }

    async fn calc_client_account(&self, client_id: &ClientId, diff: ClientAssets) -> Result<()> {
        match self.thread_safe_get(client_id) {
            Some(client) if !client.locked => {
                let new_client = Self::add_diff(&diff, &client)?;
                self.thread_safe_insert(client_id.clone(), new_client);
            }
            None => {
                if diff.total < dec!(0.0) {
                    return Err(anyhow::anyhow!("Insufficient funds"));
                }
                self.thread_safe_insert(
                    client_id.clone(),
                    Client {
                        client_id: client_id.clone(),
                        assets: diff,
                        locked: false,
                    },
                );
            }
            _ => {
                return Err(anyhow::anyhow!("Client is locked"));
            }
        };
        Ok(())
    }

    async fn lock(&self, client_id: &ClientId, diff: ClientAssets) -> Result<()> {
        match self.thread_safe_get(client_id) {
            Some(client) => {
                let mut new_client = Self::add_diff(&diff, &client)?;
                new_client.locked = true;
                self.thread_safe_insert(client_id.clone(), new_client);
            }
            None => {
                return Err(anyhow::anyhow!("Client not found"));
            }
        };
        Ok(())
    }

    fn stream_all(&self) -> Pin<Box<dyn Stream<Item = Client> + Send>> {
        let values = self
            .clients
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<Client>>();
        Box::pin(tokio_stream::iter(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn add_diff_increases_client_assets() {
        let client = Client {
            client_id: ClientId(1),
            assets: ClientAssets {
                available: dec!(100.0),
                held: dec!(50.0),
                total: dec!(150.0),
            },
            locked: false,
        };
        let diff = ClientAssets {
            available: dec!(50.0),
            held: dec!(20.0),
            total: dec!(70.0),
        };

        let result = InMemoryRepo::add_diff(&diff, &client).unwrap();
        assert_eq!(result.assets.available, dec!(150.0));
        assert_eq!(result.assets.held, dec!(70.0));
        assert_eq!(result.assets.total, dec!(220.0));
    }

    #[test]
    fn add_diff_decreases_client_assets() {
        let client = Client {
            client_id: ClientId(1),
            assets: ClientAssets {
                available: dec!(100.0),
                held: dec!(50.0),
                total: dec!(150.0),
            },
            locked: false,
        };
        let diff = ClientAssets {
            available: dec!(-50.0),
            held: dec!(-20.0),
            total: dec!(-70.0),
        };

        let result = InMemoryRepo::add_diff(&diff, &client).unwrap();
        assert_eq!(result.assets.available, dec!(50.0));
        assert_eq!(result.assets.held, dec!(30.0));
        assert_eq!(result.assets.total, dec!(80.0));
    }

    #[test]
    fn add_diff_insufficient_funds() {
        let client = Client {
            client_id: ClientId(1),
            assets: ClientAssets {
                available: dec!(100.0),
                held: dec!(50.0),
                total: dec!(150.0),
            },
            locked: false,
        };
        let diff = ClientAssets {
            available: dec!(-150.0),
            held: dec!(0.0),
            total: dec!(-150.0),
        };

        let result = InMemoryRepo::add_diff(&diff, &client);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn calc_on_locked_client_account() -> Result<()> {
        let repo = InMemoryRepo::new();
        let client_id = ClientId(1);
        let diff = ClientAssets {
            available: dec!(100.0),
            held: dec!(50.0),
            total: dec!(150.0),
        };
        repo.calc_client_account(&client_id, diff.clone()).await?;
        repo.lock(&client_id, diff.clone()).await?;
        let result = repo.calc_client_account(&client_id, diff.clone()).await;
        assert!(result.is_err());
        Ok(())
    }
}
