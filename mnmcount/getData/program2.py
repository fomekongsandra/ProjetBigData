import requests
from hdfs import InsecureClient
import json

# Configuration de l'API OpenWeatherMap
api_url = 'https://api.openweathermap.org/data/2.5/forecast?q=limoges,FR1&appid=eea045ed57d81cb0b2ad92319810b8c6'

# Configuration du client HDFS
hdfs_url = 'http://localhost:9870'
hdfs_user = 'hadoop'
hdfs_path = '/user/datalakemeteo/raw/meteo.json'

# Effectuer une requête à l'API OpenWeatherMap
response = requests.get(api_url)

if response.status_code == 200:
    weather_data = response.json()
    
    # Afficher les données dans le terminal
    print(json.dumps(weather_data, indent=2))
    
    # Écrire les données dans HDFS
    hdfs_client = InsecureClient(hdfs_url, user=hdfs_user)

    try:
        with hdfs_client.write(hdfs_path, encoding='utf-8') as writer:
            writer.write(json.dumps(weather_data))
        print(f"Les données ont été écrites avec succès dans HDFS : {hdfs_path}")
    except Exception as e:
        print(f"Erreur lors de l'écriture des données dans HDFS : {e}")
else:
    print("Erreur lors de la requête à l'API OpenWeatherMap")
    exit(1)
