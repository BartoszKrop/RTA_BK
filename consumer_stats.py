from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
stats = defaultdict(lambda: {'count': 0, 'total': 0.0, 'min': float('inf'), 'max': float('-inf')})
msg_count = 0

for message in consumer:
    data = message.value

    category = data.get('category', 'Nieznana')
    amount = data.get('amount', 0.0)
    
    cat_stats = stats[category]
    
    cat_stats['count'] += 1
    cat_stats['total'] += amount
    cat_stats['min'] = min(cat_stats['min'], amount)
    cat_stats['max'] = max(cat_stats['max'], amount)
    
    msg_count += 1
    
    if msg_count % 10 == 0:
        print("\n" + "=" * 65)
        print(f"Statystyki per kategoria po {msg_count} wiadomościach:")
        print("-" * 65)
        print(f"{'Kategoria':<15} | {'Liczba':<8} | {'Suma PLN':<10} | {'Min PLN':<8} | {'Max PLN':<8}")
        print("-" * 65)
        
        for cat, s in stats.items():
            count = s['count']
            total = s['total']
            min_val = s['min']
            max_val = s['max']
            
            print(f"{cat:<15} | {count:<8} | {total:<10.2f} | {min_val:<8.2f} | {max_val:<8.2f}")
            
        print("=" * 65 + "\n")
