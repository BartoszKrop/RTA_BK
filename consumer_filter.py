from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")

for message in consumer:
    data = message.value
    
    if data.get('amount', 0) > 3000:
        # Formatujemy wyjście f-stringiem zgodnie z wymaganiami
        print(f"ALERT: {data.get('transaction_id')} | {data.get('amount')} PLN | {data.get('city')} | {data.get('category')}")
