from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='risk_enricher_group',  
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    transaction = message.value
    
    amount = transaction.get('amount', 0)
    
    if amount > 3000:
        transaction['risk_level'] = "HIGH"
    elif amount > 1000:
        transaction['risk_level'] = "MEDIUM"
    else:
        transaction['risk_level'] = "LOW"
        
    print(transaction)
