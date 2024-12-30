use crate::models::{Client, ClientAssets, ClientId};
use crate::repo::Clients;
use anyhow::Result;
use async_trait::async_trait;
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
        if client.assets.available + diff.available < 0.0 || client.assets.total + diff.total < 0.0 {
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
            Some(client) => {
                let new_client = Self::add_diff(&diff, &client)?;
                self.thread_safe_insert(client_id.clone(), new_client);
            }
            None => {
                if diff.total < 0.0 {
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
                available: 100.0,
                held: 50.0,
                total: 150.0,
            },
            locked: false,
        };
        let diff = ClientAssets {
            available: 50.0,
            held: 20.0,
            total: 70.0,
        };

        let result = InMemoryRepo::add_diff(&diff, &client).unwrap();
        assert_eq!(result.assets.available, 150.0);
        assert_eq!(result.assets.held, 70.0);
        assert_eq!(result.assets.total, 220.0);
    }

    #[test]
    fn add_diff_decreases_client_assets() {
        let client = Client {
            client_id: ClientId(1),
            assets: ClientAssets {
                available: 100.0,
                held: 50.0,
                total: 150.0,
            },
            locked: false,
        };
        let diff = ClientAssets {
            available: -50.0,
            held: -20.0,
            total: -70.0,
        };

        let result = InMemoryRepo::add_diff(&diff, &client).unwrap();
        assert_eq!(result.assets.available, 50.0);
        assert_eq!(result.assets.held, 30.0);
        assert_eq!(result.assets.total, 80.0);
    }

    #[test]
    fn add_diff_insufficient_funds() {
        let client = Client {
            client_id: ClientId(1),
            assets: ClientAssets {
                available: 100.0,
                held: 50.0,
                total: 150.0,
            },
            locked: false,
        };
        let diff = ClientAssets {
            available: -150.0,
            held: 0.0,
            total: -150.0,
        };

        let result = InMemoryRepo::add_diff(&diff, &client);
        assert!(result.is_err());
    }
}
