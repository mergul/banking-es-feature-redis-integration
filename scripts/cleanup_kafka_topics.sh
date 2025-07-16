#!/bin/bash
KAFKA_BIN="/home/kafka/kafka/bin"
BROKER="localhost:9092"
TOPICS=(
  "banking-es.public.kafka_outbox_cdc"
  "banking-es-events"
  "banking-es-cache"
  "banking-es-dlq"
)

TMP_JSON="delete-records.json"
echo '{ "partitions": [' > $TMP_JSON
FIRST=1
for topic in "${TOPICS[@]}"; do
  PARTITIONS=$($KAFKA_BIN/kafka-topics.sh --bootstrap-server $BROKER --topic "$topic" --describe | grep -Eo 'Partition: [0-9]+' | awk '{print $2}')
  for partition in $PARTITIONS; do
    if [ $FIRST -eq 0 ]; then
      echo ',' >> $TMP_JSON
    fi
    echo -n "{\"topic\": \"$topic\", \"partition\": $partition, \"offset\": -1}" >> $TMP_JSON
    FIRST=0
  done
done
echo '], "version":1 }' >> $TMP_JSON

$KAFKA_BIN/kafka-delete-records.sh --bootstrap-server $BROKER --offset-json-file $TMP_JSON

rm $TMP_JSON