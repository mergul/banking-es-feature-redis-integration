#!/bin/bash

# Debezium Connector Durum Kontrol Scripti
# Bu script Kafka Connect üzerindeki Debezium connector'ın durumunu kontrol eder

set -e

# Konfigürasyon
CONNECTOR_NAME="banking-es-connector"
KAFKA_CONNECT_URL="http://localhost:8083"

# Renkli output için
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${BLUE}📊 Debezium Connector Durum Kontrolü${NC}"
echo "=================================="

# Kafka Connect'in çalışıp çalışmadığını kontrol et
echo -e "${YELLOW}🔍 Kafka Connect durumu kontrol ediliyor...${NC}"
if ! curl -s "$KAFKA_CONNECT_URL" > /dev/null; then
    echo -e "${RED}❌ Kafka Connect çalışmıyor!${NC}"
    echo "Kafka Connect başlatma komutu:"
    echo "  \$KAFKA_HOME/bin/connect-distributed.sh \$KAFKA_HOME/config/connect-distributed.properties"
    exit 1
fi

echo -e "${GREEN}✅ Kafka Connect çalışıyor${NC}"

# Tüm connector'ları listele
echo -e "${CYAN}📋 Mevcut connector'lar:${NC}"
CONNECTORS=$(curl -s "$KAFKA_CONNECT_URL/connectors" | jq -r '.[]' 2>/dev/null || echo "")
if [ -z "$CONNECTORS" ]; then
    echo "  Henüz connector yok"
else
    echo "$CONNECTORS" | while read -r connector; do
        echo "  - $connector"
    done
fi

echo

# Belirli connector'ın durumunu kontrol et
echo -e "${CYAN}🎯 $CONNECTOR_NAME Durumu:${NC}"

if ! curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
    echo -e "${RED}❌ Connector bulunamadı: $CONNECTOR_NAME${NC}"
    exit 0
fi

# Connector detaylarını al
CONNECTOR_INFO=$(curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME")
CONNECTOR_STATUS=$(curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status")

# Temel bilgileri göster
echo -e "${GREEN}✅ Connector mevcut${NC}"

# Durum bilgisi
STATE=$(echo "$CONNECTOR_STATUS" | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")
WORKER_ID=$(echo "$CONNECTOR_STATUS" | jq -r '.connector.worker_id' 2>/dev/null || echo "UNKNOWN")

echo -e "  Durum: ${YELLOW}$STATE${NC}"
echo -e "  Worker ID: $WORKER_ID"

# Task durumları
TASKS=$(echo "$CONNECTOR_STATUS" | jq -r '.tasks[] | "\(.id): \(.state)"' 2>/dev/null || echo "")
if [ -n "$TASKS" ]; then
    echo -e "  Task'lar:"
    echo "$TASKS" | while read -r task; do
        TASK_ID=$(echo "$task" | cut -d: -f1)
        TASK_STATE=$(echo "$task" | cut -d: -f2 | xargs)
        if [ "$TASK_STATE" = "RUNNING" ]; then
            echo -e "    Task $TASK_ID: ${GREEN}$TASK_STATE${NC}"
        else
            echo -e "    Task $TASK_ID: ${RED}$TASK_STATE${NC}"
        fi
    done
fi

echo

# Konfigürasyon bilgileri
echo -e "${CYAN}⚙️  Konfigürasyon Detayları:${NC}"
CONFIG=$(echo "$CONNECTOR_INFO" | jq -r '.config' 2>/dev/null || echo "{}")

# Önemli konfigürasyon değerlerini göster
echo -e "  Database: $(echo "$CONFIG" | jq -r '.database.hostname // "N/A"')"
echo -e "  Database Name: $(echo "$CONFIG" | jq -r '.database.dbname // "N/A"')"
echo -e "  Table: $(echo "$CONFIG" | jq -r '.table.include.list // "N/A"')"
echo -e "  Topic Prefix: $(echo "$CONFIG" | jq -r '.topic.prefix // "N/A"')"
echo -e "  Poll Interval: $(echo "$CONFIG" | jq -r '.poll.interval.ms // "N/A"')ms"
echo -e "  Max Batch Size: $(echo "$CONFIG" | jq -r '.max.batch.size // "N/A"')"

echo

# Hata kontrolü
ERRORS=$(echo "$CONNECTOR_STATUS" | jq -r '.connector.trace // empty' 2>/dev/null || echo "")
if [ -n "$ERRORS" ]; then
    echo -e "${RED}⚠️  Hatalar:${NC}"
    echo "$ERRORS"
    echo
fi

# Performans metrikleri (varsa)
echo -e "${CYAN}📈 Performans Metrikleri:${NC}"
curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq -r '.tasks[] | "Task \(.id): \(.state)"' 2>/dev/null || echo "Metrikler alınamadı"

echo
echo -e "${GREEN}✅ Durum kontrolü tamamlandı!${NC}" 