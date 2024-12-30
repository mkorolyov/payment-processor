# Toy Payment Engine
### Demo:

Run the following command to execute the application:

```shell
cargo run -- assets/transactions.csv
```

### Notes:

* The application is divided into three main components: the transaction producer, the transaction processor, and the output writer. The transaction producer communicates with the transaction processor using an mpsc channel, emphasizing that the processor is entirely independent from the producer. This design allows the producer to be replaced with other transaction sources, such as HTTP requests, Kafka messages, etc.
* Client balances are printed at the end of the processing. Clients are stored in an in-memory storage wrapped in a stream, simulating a real-world scenario where clients might be streamed from a database or another source.
* The type system ensures transactional consistency and protects against invalid operations. For example:
* A resolve transaction is only processed for transactions that are already disputed, or as a retry for transactions already marked as resolved.
* An in-memory storage mechanism is used for both transactions and clients. This storage is non-persistent and will lose all data when the application stops. The client in-memory storage uses a single read-write lock (RwLock) for simplicity. In real-world scenarios, a more granular locking strategy should be employed. The RwLock also facilitates sharing the client storage across threads, as seen in main.rs.
* Possible issues in real-world systems, such as successfully storing a transaction but failing to update the client's account state, could be resolved using retries (as shown in the code). However, more advanced strategies like Two-Phase Commit (2PC) or Saga patterns can also be applied.