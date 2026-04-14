from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime, timedelta

# Napisz konsumenta wykrywającego anomalie prędkości: 
# alert jeśli ten sam user_id wykona więcej niż 3 transakcje w ciągu 60 sekund

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

id_counts = defaultdict(list)

for message in consumer:
    # value={'tx_id': 'TX1262', 'user_id': 'u16', 'amount': 4530.77, 'store': 'Wrocław', 
    # 'category': 'elektronika', 'timestamp': '2026-03-31T12:50:51.173300'}
    tx = message.value
    user_id = tx['user_id']
    timestamp = tx['timestamp']
    id_counts[user_id].append(datetime.fromisoformat(timestamp))
    if len(id_counts[user_id]) > 3:
        time_diff = id_counts[user_id][-1] - id_counts[user_id][-4]
        if time_diff < timedelta(seconds=60):
            print(f'ALERT - użytkownik: {user_id} wykonał więcej niż 3 transakcje w ciągu {time_diff.total_seconds():.2f} sek')
            del id_counts[user_id]
        else:
            del id_counts[user_id][0]
