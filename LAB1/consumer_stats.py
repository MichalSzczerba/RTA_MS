from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='analytics',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

category_counts = defaultdict(int)
total_amount = defaultdict(float)
max_amount = defaultdict(float)
min_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    tx = message.value
    category = tx['category']
    amount = tx['amount']
    category_counts[category] += 1
    msg_count += 1
    total_amount[category] += amount
    
    # if min_amount[category] == 0:
    #     min_amount[category] = amount
    
    if amount > max_amount[category]:
        max_amount[category] = amount
        
    if category not in min_amount or amount < min_amount[category]:
        min_amount[category] = amount

    if msg_count == 10:
        print('RAPORT:')
        for cat in sorted(total_amount.keys()):
            print(f'{cat} | {category_counts[cat]} | {total_amount[cat]:.2f} PLN | MIN: {min_amount[cat]:.2f} PLN | MAX: {max_amount[cat]:.2f} PLN')
        print()
        msg_count = 0

# TWÓJ KOD
# śledzi per kategoria: - liczbę transakcji - łączny przychód - min i max kwotę
# co 10 transakcji
