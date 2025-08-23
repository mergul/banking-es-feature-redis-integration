#!/bin/bash

# Debezium Connector Durdurma Scripti
# Bu script Kafka Connect üzerindeki Debezium connector'ı durdurur

set -e

# Konfigürasyon
CONNECTOR_NAME="banking-es-connector"
KAFKA_CONNECT_URL="http://localhost:8083"

# Renkli output için
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🛑 Debezium Connector Durdurma Scripti${NC}"
echo "=================================="

# Kafka Connect'in çalışıp çalışmadığını kontrol et
echo -e "${YELLOW}🔍 Kafka Connect durumu kontrol ediliyor...${NC}"
if ! curl -s "$KAFKA_CONNECT_URL" > /dev/null; then
    echo -e "${RED}❌ Kafka Connect çalışmıyor.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Kafka Connect çalışıyor${NC}"

# Connector'ın var olup olmadığını kontrol et
if ! curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  Connector bulunamadı: $CONNECTOR_NAME${NC}"
    exit 0
fi

# Connector durumunu kontrol et
STATUS=$(curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")
echo -e "Mevcut durum: $STATUS"

if [ "$STATUS" = "STOPPED" ]; then
    echo -e "${YELLOW}⚠️  Connector zaten durdurulmuş${NC}"
    exit 0
fi

# Onay al
read -p "Connector'ı durdurmak istediğinizden emin misiniz? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "İşlem iptal edildi."
    exit 0
fi

# Connector'ı durdur
echo -e "${YELLOW}🛑 Connector durduruluyor...${NC}"
RESPONSE=$(curl -s -X DELETE "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME")

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Connector başarıyla durduruldu!${NC}"
else
    echo -e "${RED}❌ Connector durdurulurken hata oluştu!${NC}"
    echo "Hata: $RESPONSE"
    exit 1
fi

# Durumu tekrar kontrol et
echo -e "${YELLOW}⏳ Durum kontrol ediliyor...${NC}"
sleep 2

if ! curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Connector tamamen kaldırıldı${NC}"
else
    echo -e "${YELLOW}⚠️  Connector hala mevcut, durumu kontrol edin${NC}"
fi

echo -e "${GREEN}✅ İşlem tamamlandı!${NC}" 