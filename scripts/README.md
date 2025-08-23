# Debezium Connector Yönetim Scriptleri

Bu dizin, Debezium connector'ını yönetmek için kullanılan script'leri içerir.

## 📁 Dosya Yapısı

```
scripts/
├── start_debezium_connector.sh    # Connector'ı başlatır
├── stop_debezium_connector.sh     # Connector'ı durdurur
├── check_debezium_connector.sh    # Connector durumunu kontrol eder
└── README.md                      # Bu dosya
```

## 🔧 Konfigürasyon

### Symlink Oluşturma

Debezium konfigürasyon dosyası Kafka home dizinine symlink olarak oluşturulmuştur:

```bash
# Symlink konumu
$KAFKA_HOME/config/debezium-config.json -> /path/to/project/debezium-config.json

# Mevcut symlink kontrolü
ls -la $KAFKA_HOME/config/debezium-config.json
```

### Konfigürasyon Parametreleri

- **Connector Name**: `banking-es-connector`
- **Kafka Connect URL**: `http://localhost:8083`
- **Database**: PostgreSQL (`banking_es`)
- **Table**: `public.kafka_outbox_cdc`
- **Topic Prefix**: `banking-es`

## 🚀 Kullanım

### 1. Connector Durumunu Kontrol Et

```bash
./scripts/check_debezium_connector.sh
```

**Çıktı:**

- Kafka Connect durumu
- Mevcut connector'lar
- Connector durumu (RUNNING/STOPPED)
- Task durumları
- Konfigürasyon detayları
- Hata mesajları (varsa)

### 2. Connector'ı Başlat

```bash
./scripts/start_debezium_connector.sh
```

**Özellikler:**

- Kafka Connect durumu kontrolü
- Mevcut connector kontrolü
- Otomatik yeniden başlatma seçeneği
- Başarı/hata durumu raporlama

### 3. Connector'ı Durdur

```bash
./scripts/stop_debezium_connector.sh
```

**Özellikler:**

- Güvenli durdurma
- Onay sorusu
- Durum kontrolü

## 📊 Performans Parametreleri

### Debezium Konfigürasyonu

```json
{
  "poll.interval.ms": "5", // 5ms polling
  "max.batch.size": "4096", // 4096 mesaj/batch
  "max.queue.size": "16384", // 16384 mesaj queue
  "heartbeat.interval.ms": "500" // 500ms heartbeat
}
```

### Consumer Optimizasyonu

```rust
// CDC Consumer timeout optimizasyonu
let optimal_timeout_ms = poll_interval_ms * 2; // 10ms (2x poll interval)
let batch_timeout_ms = 25; // 5x poll interval
```

## 🔍 Troubleshooting

### Yaygın Sorunlar

#### 1. Kafka Connect Çalışmıyor

```bash
# Kafka Connect başlatma
$KAFKA_HOME/bin/connect-distributed.sh $KAFKA_HOME/config/connect-distributed.properties
```

#### 2. Connector Bulunamadı

```bash
# Konfigürasyon dosyasını kontrol et
ls -la $KAFKA_HOME/config/debezium-config.json

# Symlink'i yeniden oluştur
sudo ln -sf /path/to/project/debezium-config.json $KAFKA_HOME/config/debezium-config.json
```

#### 3. Database Bağlantı Hatası

```bash
# PostgreSQL durumunu kontrol et
sudo systemctl status postgresql

# Bağlantıyı test et
psql -h 127.0.0.1 -U postgres -d banking_es
```

#### 4. Permission Hatası

```bash
# Script'leri çalıştırılabilir yap
chmod +x scripts/*.sh

# Kafka dizini izinlerini kontrol et
ls -la $KAFKA_HOME/config/
```

### Log Kontrolü

```bash
# Kafka Connect logları
tail -f $KAFKA_HOME/logs/connect.log

# Debezium connector logları
curl -s http://localhost:8083/connectors/banking-es-connector/status | jq '.'
```

## 📈 Monitoring

### Connector Metrikleri

```bash
# Detaylı durum
curl -s http://localhost:8083/connectors/banking-es-connector/status | jq '.'

# Task metrikleri
curl -s http://localhost:8083/connectors/banking-es-connector/tasks | jq '.'
```

### Kafka Topic Monitoring

```bash
# Topic listesi
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Topic detayları
$KAFKA_HOME/bin/kafka-topics.sh --describe --topic banking-es.public.kafka_outbox_cdc --bootstrap-server localhost:9092
```

## 🔄 Otomatik Yeniden Başlatma

### Systemd Service (Opsiyonel)

```bash
# Service dosyası oluştur
sudo nano /etc/systemd/system/debezium-connector.service

# Service'i etkinleştir
sudo systemctl enable debezium-connector.service
sudo systemctl start debezium-connector.service
```

## 📝 Notlar

- Tüm script'ler renkli output kullanır
- Hata durumlarında detaylı bilgi verir
- Güvenli durdurma/başlatma işlemleri
- Otomatik durum kontrolü
- JSON parsing için `jq` kullanır

## 🆘 Destek

Sorun yaşarsanız:

1. Script çıktılarını kontrol edin
2. Kafka Connect loglarını inceleyin
3. Database bağlantısını test edin
4. Konfigürasyon dosyasını doğrulayın
