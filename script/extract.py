import os
import requests
import pandas as pd
from datetime import datetime
import logging
from dotenv import load_dotenv

load_dotenv()


def extract_current_weather(cities: list, api_key: str) -> bool:
    """
    Extrait les données météo actuelles pour plusieurs villes via l'API OpenWeather
    
    Args:
        cities (list): Liste des noms de villes à interroger
        api_key (str): Clé d'API OpenWeather
        
    Returns:
        bool: True si l'extraction réussit pour au moins une ville
        
    Exemple:
        >>> extract_current_weather(["Paris", "Antananarivo"], "abc123")
    """
    successful_extractions = 0
    date_str = datetime.now().strftime("%Y-%m-%d")
    os.makedirs("data/current", exist_ok=True)
    
    for city in cities:
        try:
            # Configuration de la requête API
            url = "https://api.openweathermap.org/data/2.5/weather"
            params = {
                'q': city,
                'appid': api_key,
                'units': 'metric',
                'lang': 'fr'
            }
            
            # Envoi de la requête
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            # Extraction des données
            data = response.json()
            weather_data = {
                'ville': city,
                'pays': data.get('sys', {}).get('country', 'Inconnu'),
                'date_extraction': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'timestamp_donnees': data['dt'],
                'temperature': data['main']['temp'],
                'temp_min': data['main']['temp_min'],
                'temp_max': data['main']['temp_max'],
                'humidite': data['main']['humidity'],
                'pression': data['main']['pressure'],
                'vent_vitesse': data['wind']['speed'],
                'vent_direction': data['wind'].get('deg', None),
                'precipitation': data.get('rain', {}).get('1h', 0),
                'couverture_nuageuse': data['clouds']['all'],
                'conditions': data['weather'][0]['description'],
                'timezone': data.get('timezone', None)
            }
            
            # Sauvegarde en CSV
            df = pd.DataFrame([weather_data])
            file_path = f"data/current/{date_str}_{city}.csv"
            df.to_csv(file_path, index=False)
            
            successful_extractions += 1
            logging.info(f"Données actuelles extraites avec succès pour {city}")
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Erreur réseau/API pour {city}: {str(e)}")
        except KeyError as e:
            logging.error(f"Champ manquant dans la réponse pour {city}: {str(e)}")
        except Exception as e:
            logging.error(f"Erreur inattendue pour {city}: {str(e)}")
    
    return successful_extractions > 0

if __name__ == "__main__":
    # Configuration
    API_KEY = os.getenv("API_KEY") 
    VILLES = ["Paris", "New York", "Tokyo", "Sydney", "São Paulo", "Moscow", "Antananarivo"]
    
    # Configuration du logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("data/weather_extraction.log"),
            logging.StreamHandler()
        ]
    )
    
    # Extraction des données
    extract_current_weather(VILLES, API_KEY)