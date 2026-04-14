from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    # TWÓJ KOD
    # Zwróć słownik z polami: tx_id, user_id, amount, store, category, timestamp
    
    # {tx_id:"TX0001", user_id:random z u01-u20, amount:random float z (5.0 - 5000.0),
    #  store:random z (“Warszawa”, “Kraków”, “Gdańsk”, “Wrocław”),
    #  category:random z (“elektronika”, “odzież”, “żywność”, “książki”), timestamp:czas ISO}
    
    tx_id = f'TX{random.randint(1000,9999)}'
    user_id = f'u{random.randint(1, 20):02d}'
    amount = round(random.uniform(5.0, 5000.0), 2)
    store = random.choice(('Warszawa', 'Kraków', 'Gdańsk', 'Wrocław'))
    category = random.choice(('elektronika', 'odzież', 'żywność', 'książki'))
    timestamp = datetime.now().isoformat()
    
    return {'tx_id':tx_id, 'user_id':user_id, 'amount':amount,
            'store':store, 'category':category, 'timestamp':timestamp}

# TWÓJ KOD
# Pętla: generuj transakcję, wyślij do tematu 'transactions', wypisz, sleep 1s
for i in range(1000):
    tx = generate_transaction()
    producer.send('transactions', value = tx)
    print(f"[{i+1}] {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']}")
    time.sleep(0.5)

producer.flush()
producer.close()
