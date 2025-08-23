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
