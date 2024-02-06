from hdfs import InsecureClient
import requests
import json

# Clé d'API (remplacez-la par votre propre clé)
api_key = 'c12dfa99e6f13e7f5d124758cdefcbba'

# URL de l'API pour les films populaires
popular_movies_url = 'https://api.themoviedb.org/3/movie/popular?language=en-US'

# URL de l'API pour la liste des genres
genres_url = 'https://api.themoviedb.org/3/genre/movie/list?language=en'

# Paramètres de requête pour les deux API
params = {
    'api_key': api_key,
    'page': 1  # Commencer à la première page
}

# Fonction pour récupérer toutes les pages d'une API
def get_all_pages(api_url):
    all_data = []
    page = 1
    total_pages = float('inf')  # Initialisation avec une valeur arbitrairement grande

    while page <= total_pages:
        params['page'] = page  # Mettre à jour le numéro de page dans les paramètres de requête
        response = requests.get(api_url, params=params)

        if response.status_code == 200:
            data = response.json()

            # Vérifier si la clé 'genres' est présente dans la réponse JSON (spécifique à la liste des genres)
            if 'genres' in data:
                all_data.extend(data['genres'])
                total_pages = 1  # Il n'y a généralement qu'une seule page pour la liste des genres
                page += 1
            elif 'results' in data:
                all_data.extend(data['results'])
                total_pages = data['total_pages']
                page += 1
            else:
                print(f"La clé 'genres' ou 'results' n'est pas présente dans la réponse JSON de {api_url}.")
                break
        else:
            print(f"Erreur de requête: {response.status_code}")
            print(response.text)
            break

    return all_data

# Récupérer toutes les pages de l'API pour les films populaires
all_popular_movies_data = get_all_pages(popular_movies_url)

# Récupérer toutes les pages de l'API pour la liste des genres
all_genres_data = get_all_pages(genres_url)

# Chemins de destination sur HDFS
hdfs_popular_movies_path = '/user/datalakefilm/films/all_popular_movies.json'
hdfs_genres_path = '/user/datalakefilm/films/all_genres.json'

# Établir une connexion avec HDFS
client = InsecureClient('http://localhost:9870', user='hadoop')

# Écrire les données des films populaires sur HDFS
with client.write(hdfs_popular_movies_path, encoding='utf-8') as writer:
    json.dump(all_popular_movies_data, writer, ensure_ascii=False, indent=2)

print(f"Données de toutes les pages des films populaires stockées dans {hdfs_popular_movies_path}")

# Écrire les données des genres sur HDFS
with client.write(hdfs_genres_path, encoding='utf-8') as writer:
    json.dump(all_genres_data, writer, ensure_ascii=False, indent=2)

print(f"Données de toutes les pages de la liste des genres stockées dans {hdfs_genres_path}")
