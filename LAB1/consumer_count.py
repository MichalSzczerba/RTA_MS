from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='analytics',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']
    store_counts[store] += 1
    msg_count += 1

    if store not in total_amount:
        total_amount[store] = amount
    else:
        total_amount[store] += amount

    if msg_count == 10:
        print('RAPORT:')
        for city in sorted(total_amount.keys()):
            print(f'{city} | {store_counts[city]} | {total_amount[city]:.2f} PLN | {total_amount[city]/store_counts[city]:.2f} PLN')
        print()
        msg_count = 0
        #store_counts.clear()
        #total_amount.clear()

# TWÓJ KOD
# Dla każdej wiadomości:
#   1. Zwiększ store_counts[store]
#   2. Dodaj amount do total_amount[store]
#   3. Co 10 wiadomości wypisz tabelę:
#      Sklep | Liczba | Suma | Średnia
