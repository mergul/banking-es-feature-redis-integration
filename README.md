# Banking ES - Event Sourcing with CQRS and Kafka

Bu proje, Event Sourcing, CQRS (Command Query Responsibility Segregation) ve Apache Kafka kullanarak geliştirilmiş bir bankacılık sistemidir.

## Özellikler

- **Event Sourcing**: Tüm işlemler event'ler olarak saklanır
- **CQRS**: Command ve Query'ler ayrılmış
- **Apache Kafka**: Event streaming için
- **PostgreSQL**: Event store ve projection store
- **Redis**: Distributed locking
- **Debezium**: CDC (Change Data Capture)
- **Bulk Insert Sistemi**: Yüksek performanslı toplu işlemler

## Bulk Insert Sistemi

Sistem, büyük ölçekli işlemler için optimize edilmiş bir bulk insert sistemi içerir. Bu sistem, işlem sırasında event_store ve projections.rs dosyalarındaki ayarları geçici olarak değiştirip, işlem tamamlandıktan sonra geri alır.

### Bulk Insert Özellikleri

#### 1. **BulkInsertConfigManager**

- `start_bulk_mode()`: Bulk insert modunu başlatır ve geçici optimizasyonları uygular
- `end_bulk_mode()`: Bulk insert modunu sonlandırır ve orijinal ayarları geri yükler
- `is_bulk_mode()`: Bulk modda olup olmadığını kontrol eder

#### 2. **Event Store Optimizasyonları**

- **Batch Size**: 1000 → 5000 (5x artış)
- **Batch Timeout**: 100ms → 50ms (2x hızlanma)
- **Processor Count**: 8 → 16 (2x paralellik)
- **Queue Size**: 5000 → 10000 (2x kapasite)

#### 3. **Projection Store Optimizasyonları**

- **Cache TTL**: Devre dışı (bulk işlemler için)
- **Batch Size**: 500 → 2000 (4x artış)
- **Batch Timeout**: 100ms → 25ms (4x hızlanma)

### Kullanım Örnekleri

#### Otomatik Bulk Config ile İşlemler

```rust
// Bulk config ile hesap oluşturma
let account_ids = batching
    .submit_create_operations_batch_with_bulk_config(accounts)
    .await?;

// Bulk config ile para yatırma
let deposit_results = batching
    .submit_operations_with_bulk_config(deposit_operations)
    .await?;
```

#### Manuel Bulk Mod Kontrolü

```rust
// Bulk modu başlat
batching.start_bulk_mode_all_partitions().await?;

// İşlemleri yap
let results = batching
    .submit_operations_as_direct_batches(operations)
    .await?;

// Bulk modu sonlandır
batching.end_bulk_mode_all_partitions().await?;
```

### Performans İyileştirmeleri

| Metrik     | Normal Mod  | Bulk Mod    | İyileştirme |
| ---------- | ----------- | ----------- | ----------- |
| Batch Size | 1000        | 5000        | 5x          |
| Timeout    | 100ms       | 50ms        | 2x          |
| Processors | 8           | 16          | 2x          |
| Cache      | Aktif       | Devre dışı  | Optimize    |
| Throughput | ~1000 ops/s | ~5000 ops/s | 5x          |

### Güvenlik ve Tutarlılık

- **Distributed Locking**: Redis ile aggregate bazlı kilitleme
- **Transaction Safety**: Tüm işlemler transaction içinde
- **Rollback Support**: Hata durumunda otomatik geri alma
- **Config Restoration**: İşlem sonunda orijinal ayarlar geri yüklenir

## Kurulum

```bash
# Bağımlılıkları yükle
cargo build

# Veritabanını hazırla
cargo run --bin setup_database

# Sistemi başlat
cargo run --bin main
```

## Örnekler

Bulk insert örneklerini çalıştırmak için:

```bash
cargo run --example bulk_insert_example
```

## Katkıda Bulunma

1. Fork yapın
2. Feature branch oluşturun (`git checkout -b feature/amazing-feature`)
3. Commit yapın (`git commit -m 'Add amazing feature'`)
4. Push yapın (`git push origin feature/amazing-feature`)
5. Pull Request oluşturun

## Lisans

sudo -u kafka ln -sf /home/mesut/RustroverProjects/cqrs-kafka/banking-es-feature-redis-integration/debezium-config.json $KAFKA_HOME/config/debezium-config.json
Bu proje MIT lisansı altında lisanslanmıştır.

The Optimal Solution: A Staged Event Processing Pipeline
The most robust and scalable solution is to stop thinking of the process as 32 identical, independent workers and instead model it as a multi-stage pipeline. Each stage will have a specific job and a controlled level of concurrency, communicating with the next stage via asynchronous channels.

This architecture decouples the different types of work, allowing you to optimize each part of the process independently and eliminate contention.

Here is the blueprint:

Stage 1: Consume & Distribute (Low Concurrency)

A small number of tasks (e.g., 1-2) consume messages from Kafka.
Their only job is to do the initial, lightweight parsing (extract_events_parallel_optimized) and then immediately distribute the ProcessableEvents into different channels based on their type.
AccountCreated events go into a dedicated "Creations Channel". This is their fast path to the database.
MoneyDeposited/MoneyWithdrawn events go into a "Modifications Channel". This is the slow path that requires a database read.
Stage 2: Prepare Modifications (Controlled Concurrency)

A limited pool of workers (e.g., 4-8, this is a tunable number) reads from the "Modifications Channel".
Each worker takes a batch of modification events, performs the get_accounts_batch database read to fetch the current state, applies the events, and produces the final AccountProjections.
Because the concurrency is limited here, you prevent the database from being overwhelmed with read requests.
The resulting AccountProjections for updating are sent to a "Prepared Updates Channel".
Stage 3: Write to Database (Low/Single Concurrency)

A single, dedicated worker listens to both the "Creations Channel" and the "Prepared Updates Channel".
It collects projections from both channels into batches.
It then calls the appropriate cdc_batching_service methods (submit_account_creations_bulk, submit_account_updates_bulk) to write to the database.
By serializing all database writes through a single worker, you completely eliminate write-write contention and dramatically improve database performance and consistency.
This architecture is superior because:

It separates workloads: AccountCreated events (pure INSERTs) never have to wait for MoneyDeposited events (which need a SELECT).
It eliminates contention: You have precise control over how many tasks are reading from the DB (Stage 2) and how many are writing (Stage 3), preventing the "thundering herd".
It's highly efficient: It allows for true bulk processing at the database layer, as the final write stage can build very large, clean batches for INSERT and UPDATE operations.
Refactoring Your Code
This is an architectural change, so it's more than a simple diff. However, the good news is that your existing functions in cdc_event_processor.rs are well-suited to be plugged into this new pipeline.

Here’s how you would refactor UltraOptimizedCDCEventProcessor:

Break up process_cdc_events_batch: This monolithic function would be dismantled. Its logic would be distributed into the new pipeline stages.

Reuse extract_events_parallel_optimized: The logic inside this function is perfect for Stage 1 (Consume & Distribute). Instead of returning EventBatches, it would send ProcessableEvents to the appropriate mpsc::channel based on the event type.

Reuse prepare_account_updates_and_transactions: This function is the core of Stage 2 (Prepare Modifications). The preparation workers would call this function.

Reuse the submit*\* logic: The tokio::join! block that calls batching_service.submit*\* is the core of Stage 3 (Write to Database). The single write worker would use this logic.

The main implementation effort would be in cdc_debezium.rs to set up the channels and spawn the new, specialized worker pools instead of the 32 generic workers you have now.

This approach provides the most robust and performant solution to the contention issues you're observing, ensuring your system can scale effectively.

You are correct, the process begins in cdc_debezium.rs inside the start_consuming_with_cancellation_token function, which then kicks off the staged pipeline.

Here is a clear breakdown of the pipeline's architecture:

High-Level Overview
The system is designed as a multi-stage, sharded pipeline. This architecture is intentionally chosen to handle a very high volume of events efficiently by separating different kinds of work and processing them in parallel where it makes sense.

Here's a simplified flow diagram:

[Kafka Topic: kafka_outbox_cdc]
|
| (Polls large batches of messages)
v
+---------------------------------+
| Stage 1: Consume & Distribute |
| (1 Worker) |
+---------------------------------+
| |
| (Creations)| (Modifications)
v v
[8 Creation [8 Modification
Channels] Channels]
| |
| v
| +-------------------------+
| | Stage 2: Prepare |
| | (8 Parallel Workers) |
| +-------------------------+
| | |
| (Prepared | (Transaction|
| Updates) | Projections)
v v v
[8 Prepared [1 Transaction
Channels] Channel]
| |
v v
+--------------------+ +----------------------+
| Stage 3: | | Stage 3.5: |
| Account DB Writers | | Transaction DB Writer|
| (8 Parallel Workers) | | (1 Worker) |
+--------------------+ +----------------------+
| |
v v
[PostgreSQL: account_projections Table]

Stage 1: Consumption & Distribution
How many workers consume?

There is only one asynchronous worker (consume_and_distribute_task) that polls messages from Kafka. This is a deliberate design choice. A single, efficient consumer can typically saturate a network connection by fetching very large batches of messages from Kafka, so adding more consumers often doesn't increase speed and can complicate offset management.
How fast do messages arrive?

This depends on your database load and Debezium's configuration. However, the pipeline is built for high velocity. The poll_interval_ms in your Debezium config is set to an aggressive 5ms, meaning it checks for new database changes every 5 milliseconds.
What happens in this stage?

The single worker polls Kafka, fetching up to 5000 messages at a time or whatever is available after a 50ms timeout.
It quickly parses these raw messages into a structured ProcessableEvent.
It then acts as a distributor, immediately sending each ProcessableEvent into one of two sets of channels based on the event type:
AccountCreated events go into one of the 8 Creation Channels.
MoneyDeposited and MoneyWithdrawn events go into one of the 8 Modification Channels.
The distribution is "sharded" by the aggregate_id (the account ID), ensuring all events for the same account always go to the same downstream worker.
Stage 2: Parallel Preparation
How many workers?

There are 8 parallel workers (prepare_modifications_task), one for each of the "Modification Channels".
What happens in this stage?

Each worker receives ProcessableEvents for modifications.
Crucially, this is the "slow path" because it requires I/O. For each event, the worker must fetch the current AccountProjection from the database to apply the change (e.g., add or subtract from the balance).
After applying the event, it produces two things:
An updated AccountProjection which it sends to one of the 8 Prepared Channels.
A new TransactionProjection which it sends to the single, shared Transaction Channel.
Batching: In the latest version of the code, this stage processes events one by one. The previous batching logic was removed from this stage to ensure a continuous, fast-flowing stream of data to the final database writers, which are better equipped to handle the batching.

Stage 3 & 3.5: Batched Database Writes
This is the final stage and the primary place where events are accumulated into large, efficient batches for writing to the database.

How many workers?

There are 9 database writer workers in total, split by task:
8 parallel workers (write_to_database_task) handle creating and updating account_projections.
1 dedicated worker (write_transactions_task) handles inserting transaction_projections.
Where do we accumulate messages to batches?

Right here. This is the most important batching point. Both types of writer tasks use the tokio_stream::StreamExt::chunks_timeout method, which is the ideal tool for this job.
The 8 Account Writers merge their "Creation" and "Prepared Update" channels into a single stream. They then pull from this stream, accumulating events into a chunk. A batch is flushed to the database if either:
The batch size reaches 2000 events.
A 50ms timeout is reached since the last event was received.
The 1 Transaction Writer does the same. It accumulates batches of transaction projections and flushes if its batch size is met or a 50ms timeout occurs.
This "batch at the last moment" strategy is highly efficient. It allows the final writers to build large, optimal batches when the system is under heavy load (maximizing throughput) while still processing events quickly when the load is light (maintaining low latency).

Yes, you are absolutely correct. The system is designed to handle transaction_projections in a highly optimized way, using a dedicated channel to feed a worker that performs a direct COPY insert. This is a very efficient pattern for high-throughput, append-only data like transaction records.

Let's walk through the exact data flow:

The TransactionProjection Data Flow
Preparation (cdc_debezium.rs): In the prepare_modifications_task, when an event like MoneyDeposited is processed, the system creates both an updated AccountProjection and a new TransactionProjection.

Dedicated Channel (cdc_debezium.rs): The newly created TransactionProjection is immediately sent into a dedicated, high-capacity MPSC channel (transactions_tx). This separates it from the AccountProjection updates, which require a slower update path.

Dedicated Worker (cdc_debezium.rs): A single, dedicated worker task, write_transactions_task, is responsible for consuming from this channel. Using a single worker for this INSERT-only task avoids database contention and is extremely fast.

Efficient Batching (cdc_debezium.rs): This worker uses the chunks_timeout stream adaptor. This is the ideal way to batch: it collects transactions from the channel and flushes the batch either when it's full or when a 50ms timeout is reached. This ensures both high throughput under load and low latency when idle.

Direct COPY Insert (projections.rs): The flush_transaction_batch function calls submit_transaction_creations_bulk, which ultimately calls bulk_insert_transaction_projections in your ProjectionStore. This function then executes the most efficient bulk-insert method: bulk_insert_transactions_direct_copy.

This entire flow is designed to be as fast as possible, correctly using a dedicated channel and the COPY BINARY command for maximum database INSERT performance.
