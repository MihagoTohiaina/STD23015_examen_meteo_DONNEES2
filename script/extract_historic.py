import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import time # Ajout pour la pause

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("data/historical_extraction.log"), # Assurez-vous que ce fichier est dans data
                        logging.StreamHandler()
                    ])

def extract_historical_weather(latitude: float, longitude: float, city: str) -> bool:
    """
    Extrait les données météo historiques via l'API Open-Meteo
    
    Args:
        latitude (float): Latitude de la ville
        longitude (float): Longitude de la ville
        city (str): Nom de la ville pour le nommage
        
    Returns:
        bool: True si l'extraction réussit, False sinon
    """
    try:
        # Calcul des dates (3 dernières années)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3*365) # 3 ans pour avoir des données suffisantes
        
        # Configuration de la requête API
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'start_date': start_date.strftime("%Y-%m-%d"),
            'end_date': end_date.strftime("%Y-%m-%d"),
            'daily': ['temperature_2m_max', 'temperature_2m_min', 
                      'precipitation_sum', 'rain_sum', 'snowfall_sum'],
            'timezone': 'auto'
        }
        
        # Envoi de la requête
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        # Traitement des données
        data = response.json()
        daily_data = data['daily']
        
        df = pd.DataFrame({
            'ville': city,
            'date': daily_data['time'],
            'temp_max': daily_data['temperature_2m_max'],
            'temp_min': daily_data['temperature_2m_min'],
            'precipitation': daily_data['precipitation_sum'],
            'pluie': daily_data['rain_sum'],
            'neige': daily_data['snowfall_sum']
        })
        
        # Sauvegarde en CSV
        os.makedirs("data/historical", exist_ok=True)
        file_path = f"data/historical/{city.lower().replace(' ', '_')}_historical.csv"
        df.to_csv(file_path, index=False)
        
        return True
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur réseau/API pour {city}: {str(e)}")
    except KeyError as e:
        logging.error(f"Champ manquant dans la réponse pour {city}: {str(e)}")
    except Exception as e:
        logging.error(f"Erreur inattendue pour {city}: {str(e)}")
        
    return False

def get_city_coordinates(city: str) -> tuple:
    """Retourne les coordonnées (latitude, longitude) pour une ville donnée"""
    CITY_COORDINATES = {
        "Paris": (48.8566, 2.3522),
        "New York": (40.7128, -74.0060),
        "Tokyo": (35.6762, 139.6503),
        "Sydney": (-33.8688, 151.2093),
        "Moscow": (55.7558, 37.6173),
        "Antananarivo": (-18.8792, 47.5079),
        "São Paulo": (-23.5505, -46.6333) # Ajout de São Paulo
    }
    return CITY_COORDINATES.get(city, (None, None))

def main():
    """Fonction principale pour l'extraction des données historiques"""
    CITIES = ["Paris", "New York", "Tokyo", "Sydney", "São Paulo", "Moscow", "Antananarivo"] # Assurez-vous que toutes les villes sont ici
    
    for city in CITIES:
        lat, lon = get_city_coordinates(city)
        if lat is not None and lon is not None:
            success = extract_historical_weather(lat, lon, city)
            if success:
                logging.info(f"Données historiques pour {city} extraites avec succès")
            else:
                logging.error(f"Échec de l'extraction des données historiques pour {city}")
        else:
            logging.error(f"Coordonnées non trouvées pour {city}")
        
        # NOUVEAU : Ajouter un délai pour éviter les erreurs "Too Many Requests"
        time.sleep(2) # Attendre 2 secondes entre chaque requête API
        logging.info(f"Pause de 2 secondes avant d'extraire les données de la prochaine ville.")

if __name__ == "__main__":
    main()