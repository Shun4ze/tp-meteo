from kafka import KafkaProducer
import requests
import json
import time

API_KEY = "6365575828f48fe0263c2d56aa2e323c"  

cities = ["Paris", "London", "Tokyo"] 

def get_weather_data(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200: #200 veut dire que la requete a reussit
        return response.json()  # Retourne les données sous forme de dictionnaire
    else:
        print(f"Erreur lors de la récupération des données pour {city}")
        return None


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Connexion à Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialisation des données en JSON
)

def send_weather_data_to_kafka():
    while True:
        for city in cities:
            # Récupérer les données météo
            weather_data = get_weather_data(city)
            if weather_data:
                # Envoi des données dans le topic Kafka
                producer.send('tp-meteo', value=weather_data)
                print(f"Données météo envoyées pour {city}: {weather_data['main']['temp']}°C")
        
        # Attendre 1 minute avant de renvoyer les données
        time.sleep(60)

# Lancer l'envoi des données
send_weather_data_to_kafka()

