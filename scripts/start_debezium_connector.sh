#!/bin/bash

# Debezium Connector Başlatma Scripti
# Bu script Kafka Connect üzerinde Debezium connector'ı başlatır

set -e

# Konfigürasyon
KAFKA_HOME=${KAFKA_HOME:-/home/kafka/kafka}
CONNECTOR_NAME="banking-es-connector"
CONFIG_FILE="$KAFKA_HOME/config/debezium-config.json"
KAFKA_CONNECT_URL="http://localhost:8083"

# Renkli output için
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔧 Debezium Connector Başlatma Scripti${NC}"
echo "=================================="

# Konfigürasyon dosyasını kontrol et
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}❌ Konfigürasyon dosyası bulunamadı: $CONFIG_FILE${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Konfigürasyon dosyası bulundu: $CONFIG_FILE${NC}"

# Kafka Connect'in çalışıp çalışmadığını kontrol et
echo -e "${YELLOW}🔍 Kafka Connect durumu kontrol ediliyor...${NC}"
if ! curl -s "$KAFKA_CONNECT_URL" > /dev/null; then
    echo -e "${RED}❌ Kafka Connect çalışmıyor. Lütfen önce Kafka Connect'i başlatın.${NC}"
    echo "Kafka Connect başlatma komutu:"
    echo "  $KAFKA_HOME/bin/connect-distributed.sh $KAFKA_HOME/config/connect-distributed.properties"
    exit 1
fi

echo -e "${GREEN}✅ Kafka Connect çalışıyor${NC}"

# Mevcut connector'ları listele
echo -e "${YELLOW}📋 Mevcut connector'lar:${NC}"
curl -s "$KAFKA_CONNECT_URL/connectors" | jq -r '.[]' 2>/dev/null || echo "Henüz connector yok"

# Eğer connector zaten varsa, durumunu kontrol et
if curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  Connector zaten mevcut: $CONNECTOR_NAME${NC}"
    
    # Connector durumunu kontrol et
    STATUS=$(curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")
    echo -e "Durum: $STATUS"
    
    if [ "$STATUS" = "RUNNING" ]; then
        echo -e "${GREEN}✅ Connector zaten çalışıyor${NC}"
        read -p "Yine de yeniden başlatmak istiyor musunuz? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "İşlem iptal edildi."
            exit 0
        fi
        
        # Connector'ı durdur
        echo -e "${YELLOW}🛑 Connector durduruluyor...${NC}"
        curl -X DELETE "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME"
        sleep 5
    fi
fi

# Connector'ı başlat
echo -e "${YELLOW}🚀 Debezium connector başlatılıyor...${NC}"
RESPONSE=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d @"$CONFIG_FILE" \
    "$KAFKA_CONNECT_URL/connectors")

echo "Yanıt: $RESPONSE"

# Başarı kontrolü
if echo "$RESPONSE" | jq -e '.name' > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Connector başarıyla başlatıldı!${NC}"
    
    # Durumu kontrol et
    echo -e "${YELLOW}⏳ Connector durumu kontrol ediliyor...${NC}"
    sleep 3
    
    STATUS=$(curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")
    echo -e "Durum: $STATUS"
    
    if [ "$STATUS" = "RUNNING" ]; then
        echo -e "${GREEN}🎉 Connector başarıyla çalışıyor!${NC}"
    else
        echo -e "${RED}⚠️  Connector başlatıldı ama durumu: $STATUS${NC}"
    fi
    
else
    echo -e "${RED}❌ Connector başlatılamadı!${NC}"
    echo "Hata detayı:"
    echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"
    exit 1
fi

echo -e "${BLUE}📊 Connector detayları:${NC}"
echo "=================================="
curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq '.' 2>/dev/null || echo "Detaylar alınamadı"

echo -e "${GREEN}✅ İşlem tamamlandı!${NC}" 