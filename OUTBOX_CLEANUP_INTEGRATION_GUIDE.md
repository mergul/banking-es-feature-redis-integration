# Outbox Cleanup Service Integration Guide

## Genel Bakış

Bu rehber, projeye yeni **Gelişmiş Outbox Temizleme Servisi**'nin nasıl entegre edileceğini açıklar. Bu servis, mevcut basit DELETE sorguları yerine **iki aşamalı temizleme stratejisi** kullanır.

## Yeni Strateji vs Eski Strateji

### Eski Strateji (Mevcut)

- Basit DELETE sorguları
- Tek aşamalı temizlik
- Sınırlı konfigürasyon
- Vakum yok

### Yeni Strateji (Gelişmiş)

- **İki aşamalı temizlik:** Önce işaretleme, sonra fiziksel silme
- **Güvenlik marjı:** İşaretlenen kayıtlar için ek bekleme süresi
- **Batch işleme:** Büyük veri setleri için optimize edilmiş
- **Sınırlı döngüler:** Zaman ve batch limitleri
- **Vakum desteği:** Otomatik disk alanı temizliği
- **Detaylı metrikler:** Temizlik performansı izleme
- **Health check:** Servis sağlığı kontrolü

## Entegrasyon Adımları

### 1. Veritabanı Migration'ı

```bash
# Migration'ı çalıştır
psql -d banking_es -f migrations/015_add_deleted_at_to_cdc_outbox.sql
```

Bu migration:

- `kafka_outbox_cdc` tablosuna `deleted_at` kolonu ekler
- Performans için gerekli indeksleri oluşturur
- Temizlik istatistikleri için fonksiyonlar ekler

### 2. Konfigürasyon

`outbox_cleanup_config.env` dosyasını kullanarak servisi konfigüre edin:

```env
# Retention Policy
OUTBOX_RETENTION_HOURS=24
OUTBOX_SAFETY_MARGIN_MINUTES=30

# Cleanup Schedule
OUTBOX_CLEANUP_INTERVAL_MINUTES=60
OUTBOX_MAX_CYCLE_DURATION_MINUTES=15

# Batch Processing
OUTBOX_BATCH_SIZE=1000
OUTBOX_MAX_BATCHES_PER_CYCLE=10
OUTBOX_BATCH_DELAY_MS=100

# Performance Settings
OUTBOX_ENABLE_VACUUM=true
OUTBOX_ENABLE_ADVANCED_CLEANUP=true
```

### 3. CDC Service Manager Entegrasyonu

Yeni temizleme servisi otomatik olarak CDC Service Manager'a entegre edilmiştir:

```rust
// CDC Service Manager artık yeni temizleme servisini kullanır
let cdc_service_manager = CDCServiceManager::new(
    config,
    outbox_repo,
    kafka_producer,
    kafka_consumer,
    cache_service,
    pools,
    Some(metrics),
    Some(consistency_manager),
)?;

// Servis başlatıldığında otomatik olarak temizleme servisi de başlar
cdc_service_manager.start().await?;
```

### 4. Test Etme

```bash
# Integration testini çalıştır
cargo test test_outbox_cleanup_service_integration

# Performance testini çalıştır
cargo test test_cleanup_performance
```

## Konfigürasyon Seçenekleri

### Retention Policy

- `retention_hours`: Kayıtları ne kadar süre tutacağız (varsayılan: 24 saat)
- `safety_margin_minutes`: İşaretleme ve fiziksel silme arasındaki güvenlik marjı (varsayılan: 30 dakika)

### Batch Processing

- `batch_size`: Bir seferde işlenecek maksimum kayıt sayısı (varsayılan: 1000)
- `max_batches_per_cycle`: Bir döngüde maksimum batch sayısı (varsayılan: 10)
- `batch_delay_ms`: Batch'ler arası bekleme süresi (varsayılan: 100ms)

### Performance

- `enable_vacuum`: Vakum işlemini etkinleştir (varsayılan: true)
- `max_cycle_duration_minutes`: Maksimum döngü süresi (varsayılan: 15 dakika)

## Monitoring ve Metrikler

### Temizlik Metrikleri

```rust
let metrics = cleaner.cleanup_cycle().await?;
println!("Marked: {}, Deleted: {}, Duration: {}ms",
    metrics.marked_for_deletion,
    metrics.physically_deleted,
    metrics.cleanup_duration_ms);
```

### İstatistikler

```rust
let stats = cleaner.get_outbox_stats().await?;
println!("Total: {}, Active: {}, Marked: {}",
    stats.total_records,
    stats.active_records,
    stats.marked_for_deletion);
```

### Health Check

```rust
let health_checker = CleanupHealthCheck::new(cleaner);
let is_healthy = health_checker.health_check().await?;
```

## SQL Fonksiyonları

### Manuel Temizlik

```sql
-- Manuel temizlik tetikle
SELECT * FROM trigger_cdc_outbox_cleanup(24, 30);

-- İstatistikleri görüntüle
SELECT * FROM get_cdc_outbox_cleanup_stats();
```

## Troubleshooting

### Yaygın Sorunlar

1. **Migration Hatası**

   ```bash
   # Tabloyu kontrol et
   \d kafka_outbox_cdc

   # deleted_at kolonunu manuel ekle
   ALTER TABLE kafka_outbox_cdc ADD COLUMN deleted_at TIMESTAMP;
   ```

2. **Performans Sorunları**

   - Batch size'ı azalt
   - Delay süresini artır
   - Max cycle duration'ı azalt

3. **Disk Alanı Sorunları**
   - Vakum'u etkinleştir
   - Retention süresini azalt
   - Batch size'ı artır

### Log Seviyeleri

```rust
// Debug logları için
RUST_LOG=debug cargo run

// Temizlik servisi logları
RUST_LOG=banking_es::infrastructure::outbox_cleanup_service=debug cargo run
```

## Fallback Stratejisi

Yeni servis başarısız olursa, sistem otomatik olarak eski temizleme stratejisine geri döner:

```rust
if let Some(cleaner) = &self.outbox_cleaner {
    // Yeni gelişmiş temizleme servisi
    cleaner.cleanup_cycle().await?;
} else {
    // Eski temizleme stratejisi
    self.start_cleanup_task().await?;
}
```

## Performans Optimizasyonu

### Büyük Veri Setleri İçin

```rust
let config = CleanupConfig {
    batch_size: 5000,           // Büyük batch'ler
    max_batches_per_cycle: 20,  // Daha fazla batch
    batch_delay_ms: 200,        // Daha uzun delay
    max_cycle_duration_minutes: 30, // Daha uzun döngü
    ..Default::default()
};
```

### Yüksek Trafik İçin

```rust
let config = CleanupConfig {
    retention_hours: 12,        // Daha kısa retention
    safety_margin_minutes: 15,  // Daha kısa safety margin
    cleanup_interval_minutes: 30, // Daha sık temizlik
    ..Default::default()
};
```

## Güvenlik

### Güvenlik Marjı

- İşaretlenen kayıtlar hemen silinmez
- `safety_margin_minutes` kadar bekler
- Bu süre içinde geri alınabilir

### Transaction Güvenliği

- Her batch ayrı transaction'da çalışır
- Hata durumunda rollback yapılır
- Partial failure'lar izole edilir

## Monitoring Dashboard

### Grafana Metrikleri

```sql
-- Temizlik performansı
SELECT
    DATE_TRUNC('hour', created_at) as hour,
    COUNT(*) as records_processed,
    AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as avg_processing_time
FROM kafka_outbox_cdc
WHERE deleted_at IS NOT NULL
GROUP BY hour
ORDER BY hour DESC;
```

### Alerting

- Temizlik servisi down
- Çok fazla marked record
- Temizlik süresi çok uzun
- Disk alanı kritik seviyede

## Sonuç

Bu yeni temizleme servisi:

- ✅ Daha güvenli (iki aşamalı)
- ✅ Daha performanslı (batch işleme)
- ✅ Daha izlenebilir (detaylı metrikler)
- ✅ Daha esnek (konfigürasyon)
- ✅ Daha sağlam (fallback stratejisi)

Mevcut sistemi bozmadan, daha gelişmiş bir temizleme stratejisi sunar.
