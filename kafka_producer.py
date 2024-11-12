import pandas as pd
from kafka import KafkaProducer
import json
import time

# Configurer le producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Charger les données à partir du fichier Excel
file_path = "Online-Retail.xlsx"  # Remplacez par le chemin réel de votre fichier
data = pd.read_excel(file_path)

# Filtrer et sélectionner les colonnes utiles
data = data[['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']]

# Fonction pour envoyer chaque ligne de données comme message Kafka
def send_sales_data():
    for _, row in data.iterrows():
        # Préparer l'événement de vente
        sale_event = {
            'invoice_no': row['InvoiceNo'],
            'stock_code': row['StockCode'],
            'description': row['Description'],
            'quantity': int(row['Quantity']),
            'invoice_date': str(row['InvoiceDate']),
            'unit_price': float(row['UnitPrice']),
            'customer_id': int(row['CustomerID']) if not pd.isnull(row['CustomerID']) else None,
            'country': row['Country']
        }
        
        # Envoyer l'événement au topic Kafka
        producer.send('sales_topic', sale_event)
        print(f"Événement envoyé : {sale_event}")
        
        # Pause pour simuler un flux en temps réel
        time.sleep(1)

# Appeler la fonction pour envoyer les données
send_sales_data()
