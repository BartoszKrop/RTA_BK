from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

for message in consumer:
    data = message.value
    
    store = data.get('store', 'Nieznany')
    amount = data.get('amount', 0.0)
    
    store_counts[store] += 1
    
    total_amount[store] = total_amount.get(store, 0.0) + amount
    
    msg_count += 1
    
    if msg_count % 10 == 0:
        print("\n" + "=" * 55)
        print(f"Podsumowanie po {msg_count} wiadomościach:")
        print("-" * 55)
        # Nagłówek tabeli
        print(f"{'Sklep':<15} | {'Liczba':<8} | {'Suma':<10} | {'Średnia':<10}")
        print("-" * 55)
        
        for current_store, count in store_counts.items():
            total_sum = total_amount[current_store]
            average = total_sum / count if count > 0 else 0

            print(f"{current_store:<15} | {count:<8} | {total_sum:<10.2f} | {average:<10.2f}")
        print("=" * 55 + "\n")
