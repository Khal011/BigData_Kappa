import pandas as pd
from kafka import KafkaProducer
import json
import time

# Configurer le producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

# Charger les données à partir du fichier Excel
file_path = "Online-Retail.xlsx"  # Remplacez par le chemin réel de votre fichier
data = pd.read_excel(file_path)

# Filtrer et sélectionner les colonnes utiles
data = data[['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']]

# Limiter à 100 lignes
data = data.head(100)


# Fonction pour envoyer chaque ligne de données comme message Kafka
def send_sales_data():
    for _, row in data.iterrows():
        # Préparer l'événement de vente
        sale_event = {
            'invoice_no': int(row['InvoiceNo']),  # Ensure it's an integer
            'stock_code': str(row['StockCode']),  # Ensure it's a string
            'description': str(row['Description']),  # Ensure it's a string
            'quantity': int(row['Quantity']),  # Ensure it's an integer
            'invoice_date': str(row['InvoiceDate']),  # Convert to string format
            'unit_price': float(row['UnitPrice']),  # Ensure it's a float
            'customer_id': int(row['CustomerID']) if not pd.isnull(row['CustomerID']) else None,  # Integer or None
            'country': str(row['Country'])  # Ensure it's a string
        }

        # Envoyer l'événement au topic Kafka
        producer.send('sales_topic', sale_event)
        print(f"Événement envoyé : {sale_event}")
        
        # Pause pour simuler un flux en temps réel
        time.sleep(1)

# Appeler la fonction pour envoyer les données
send_sales_data()
