use crate::models;
use crate::repo::Clients;
use anyhow::Result;
use std::collections::HashMap;
use std::pin::Pin;
use tokio_stream::Stream;

pub struct InMemoryRepo {
    clients: HashMap<u16, models::Client>,
}

impl InMemoryRepo {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }
}

impl Clients for InMemoryRepo {
    fn get(&self, client_id: u16) -> Option<models::Client> {
        self.clients.get(&client_id).cloned()
    }

    fn save(&mut self, client: models::Client) -> Result<()> {
        self.clients.insert(client.client, client);
        Ok(())
    }

    fn stream_all(&self) -> Pin<Box<dyn Stream<Item = models::Client> + Send>> {
        // Clone all clients to allow streaming
        let clients: Vec<models::Client> = self.clients.values().cloned().collect();

        // Create a stream from the vector
        let stream = tokio_stream::iter(clients);

        // Return the stream as a pinned box
        Box::pin(stream)
    }
}
