from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Configurer le consommateur Kafka
consumer = KafkaConsumer(
    'sales_topic',  # Le nom du topic auquel vous souhaitez vous abonner
    bootstrap_servers='localhost:9092',  # Use the correct Kafka broker address
    max_poll_records = 100,
    auto_offset_reset='earliest',  # Start reading from the beginning if no offset exists
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)




# Connexion à Cassandra
cluster = Cluster(['localhost'], port=9042)  # Assurez-vous que Cassandra est en cours d'exécution sur localhost

session = cluster.connect()

# Create the keyspace if it doesn't exist
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sales_data
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

# Connect to the keyspace 'sales_data'
session.set_keyspace('sales_data') # Utilisez le keyspace 'sales_data'

# Vérifier si la table existe, et la créer si nécessaire
create_table_query = """
    CREATE TABLE IF NOT EXISTS sales (
        invoice_no int PRIMARY KEY,
        stock_code text,
        description text,
        quantity int,
        invoice_date text,
        unit_price float,
        customer_id int,
        country text
    );
"""

session.execute("DROP TABLE IF EXISTS sales;")


session.execute(create_table_query)  # Exécute la création de la table si elle n'existe pas

# Préparer la requête d'insertion dans la table Cassandra
insert_query = """
    INSERT INTO sales (invoice_no, stock_code, description, quantity, invoice_date, unit_price, customer_id, country)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""
prepared_statement = session.prepare(insert_query)


# Consommer les messages de Kafka
for message in consumer:
    sale_event = message.value  # Le message consommé
    print(f"Événement reçu: {sale_event}")

    # Extract data from the event
    invoice_no = int(sale_event['invoice_no'])
    stock_code = str(sale_event['stock_code'])
    description = str(sale_event['description'])
    quantity = int(sale_event['quantity'])
    invoice_date = str(sale_event['invoice_date'])
    unit_price = float(sale_event['unit_price'])
    customer_id = int(sale_event['customer_id']) if sale_event['customer_id'] is not None else None
    country = str(sale_event['country'])

    # Insérer les données dans Cassandra
    session.execute(prepared_statement, (
        invoice_no, stock_code, description, quantity, invoice_date, unit_price, customer_id, country
    ))

    print(f"Événement inséré dans Cassandra: {sale_event}")

# Ne pas oublier de fermer la connexion à Cassandra lorsque vous avez terminé
# cluster.shutdown()