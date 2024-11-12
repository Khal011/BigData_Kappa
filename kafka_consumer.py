from kafka import KafkaConsumer
import json

# Configurer le consommateur Kafka
consumer = KafkaConsumer(
    'sales_topic',  # Le nom du topic auquel vous souhaitez vous abonner
    bootstrap_servers='localhost:9092',  # Use the correct Kafka broker address
    max_poll_records = 100,
    auto_offset_reset='earliest',  # Start reading from the beginning if no offset exists
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)



# Consommer les messages
for message in consumer:
    sale_event = message.value  # Le message consommé
    print(f"Événement reçu: {sale_event}")
    # Vous pouvez ajouter ici la logique pour traiter les données, comme les enregistrer dans une base de données
