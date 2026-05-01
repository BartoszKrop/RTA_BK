from kafka import KafkaConsumer
from collections import defaultdict
import json
import time

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_history = defaultdict(list)
TIME_WINDOW_SEC = 60
MAX_TRANSACTIONS = 3

print("Rozpoczynam monitorowanie anomalii...")

for message in consumer:
    data = message.value
    user_id = data.get('user_id')
    
    if not user_id:
        continue 
        
    current_time = time.time()
    
    # Dodanie aktualnego czasu do historii użytkownika
    user_history[user_id].append(current_time)
    
    # Usuwanie z historii wpisów starszych niż 60 sekund
    user_history[user_id] = [t for t in user_history[user_id] if current_time - t <= TIME_WINDOW_SEC]
    
    # Alert tylko w momencie przekroczenia limitu
    if len(user_history[user_id]) > MAX_TRANSACTIONS:
        print(f" PODEJRZANA AKTYWNOŚĆ: Użytkownik {user_id} wykonał {len(user_history[user_id])} transakcji w ciągu ostatnich 60 sekund!")
