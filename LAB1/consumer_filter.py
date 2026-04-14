from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")

for message in consumer:
    tx = message.value
    if tx['amount'] > 3000:
        print(f'ALERT: {tx["tx_id"]} | {tx["amount"]} PLN | {tx["store"]} | {tx["category"]}')
    
# TWÓJ KOD
# Dla każdej wiadomości: sprawdź czy amount > 3000, jeśli tak — wypisz ALERT
# Format: ALERT: TX0042 | 2345.67 PLN | Warszawa | elektronika

# ConsumerRecord(topic='transactions', partition=0, offset=281, 
# timestamp=1774961451173, timestamp_type=0, key=None, 
# value={'tx_id': 'TX1262', 'user_id': 'u16', 'amount': 4530.77, 'store': 'Wrocław', 
# 'category': 'elektronika', 'timestamp': '2026-03-31T12:50:51.173300'}, headers=[], 
# checksum=None, serialized_key_size=-1, serialized_value_size=151, serialized_header_size=-1)
