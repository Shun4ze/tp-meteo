from kafka import KafkaConsumer
import psycopg2
import json
from datetime import datetime

# Configuration de Kafka
KAFKA_TOPIC = 'tp-meteo'
KAFKA_SERVER = 'localhost:9092'

# Configuration de la base de données Neon PostgreSQL
# La chaîne de connexion est intégrée ici
PASSWORD_DB = "mvtlo1AR6jHD"
DB_CONNECTION_STRING = (
    "postgresql://tp-meteo_owner:7UYJ0hQWijns@ep-ancient-cake-a22wfnyt.eu-central-1.aws.neon.tech/"
    "tp-meteo?sslmode=require"
)

# Connexion à la base de données PostgreSQL avec psycopg2
db_connection = psycopg2.connect(DB_CONNECTION_STRING)
cursor = db_connection.cursor()

# Initialisation du consumer Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("En attente de messages...")

# Consommer les messages et insérer dans la base de données
for message in consumer:
    data = message.value

    # Extraire les informations nécessaires
    ville = data['name']
    latitude = data['coord']['lat']
    longitude = data['coord']['lon']
    temperature = data['main']['temp']
    humidite = data['main']['humidity']
    vent = data['wind']['speed']
    
    # Récupérer la date et l'heure actuelles
    date = datetime.now()

    # Insérer les données dans la table meteo
    try:
        cursor.execute(
            """
            INSERT INTO meteo (ville, latitude, longitude, temperature, humidite, vent, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (ville, latitude, longitude, temperature, humidite, vent, date)
        )
        db_connection.commit()
        print(f"Données insérées pour {ville}: Temp={temperature}°C, Humidité={humidite}%, Vent={vent} m/s, Date={date}")
    except Exception as e:
        print(f"Erreur lors de l'insertion pour {ville}: {e}")
        db_connection.rollback()

# Fermer la connexion à la base de données à la fin du traitement
cursor.close()
db_connection.close()
