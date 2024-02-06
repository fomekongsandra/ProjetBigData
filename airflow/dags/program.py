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
def movies_data () :
# Récupérer toutes les pages de l'API pour les films populaires
    all_popular_movies_data = get_all_pages(popular_movies_url)

    # Définir le nom du fichier JSON pour les films populaires
    popular_movies_json_file_path = '/user/datalakefilm/films/all_popular_movies.json'

    # Écrire les données dans le fichier JSON
    with open(popular_movies_json_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(all_popular_movies_data, json_file, indent=2)

    print(f"Données de toutes les pages des films populaires récupérées et stockées dans {popular_movies_json_file_path}")

    # Récupérer toutes les pages de l'API pour la liste des genres
    all_genres_data = get_all_pages(genres_url)

    # Définir le nom du fichier JSON pour la liste des genres
    genres_json_file_path = 'D:\\MS2D2Limoges\\BigData\\PR7BIGDATA\\getData\\storage\\all_genres.json'

    # Écrire les données dans le fichier JSON
    with open(genres_json_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(all_genres_data, json_file, indent=2)

    print(f"Données de toutes les pages de la liste des genres récupérées et stockées dans {genres_json_file_path}")

def ingest_data():
    movies_data()
    