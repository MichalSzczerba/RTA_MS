from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id = 'risk_assessment',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Dodaje poziom ryzyka transakcji")

for message in consumer:
    tx = message.value
    if tx['amount'] > 3000:
        risk_level = 'HIGH'
    elif tx['amount'] > 1000:
        risk_level = 'MEDIUM'
    else:
        risk_level = 'LOW'
        
    tx['risk_level'] = risk_level
    print(tx)

# TWÓJ KOD
# Czytaj z 'transactions' (użyj INNEGO group_id!)
# Dodaj pole risk_level na podstawie amount
# Wypisz wzbogaconą transakcję
