import os
import requests
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, Any

def extract_meteo(city: str, api_key: str, date: str) -> bool:
    """
    Extrait les données météo étendues pour une ville via l'API OpenWeather
    (Adapté pour l'analyse comparative climatique)
    
    Args:
        city (str): Nom de la ville à interroger
        api_key (str): Clé d'API OpenWeather
        date (str): Date au format 'YYYY-MM-DD' pour l'organisation
        
    Returns:
        bool: True si l'extraction réussit, False sinon
        
    Exemple:
        >>> extract_meteo("Paris", "abc123", "2025-06-01")
    """
    try:
        # Configuration de la requête API (OneCall 3.0 pour données historiques/étendues)
        url = "https://api.openweathermap.org/data/3.0/onecall"
        geo_params = {
            'q': city,
            'appid': api_key
        }
        
        # D'abord obtenir les coordonnées géographiques
        geo_response = requests.get(
            "http://api.openweathermap.org/geo/1.0/direct",
            params=geo_params,
            timeout=10
        )
        geo_response.raise_for_status()
        geo_data = geo_response.json()[0]
        lat, lon = geo_data['lat'], geo_data['lon']
        
        # Paramètres pour les données climatiques étendues
        params = {
            'lat': lat,
            'lon': lon,
            'exclude': 'minutely,hourly',  # On garde daily et current
            'appid': api_key,
            'units': 'metric',
            'lang': 'fr'
        }
        
        # Requête principale
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        # Extraction des indicateurs clés pour l'analyse comparative
        weather_data = {
            'ville': city,
            'pays': geo_data.get('country', 'Inconnu'),
            'date_extraction': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'date_analyse': date,
            
            # Données actuelles
            'temperature_actuelle': data['current']['temp'],
            'temperature_ressentie': data['current']['feels_like'],
            'humidite': data['current']['humidity'],
            'pression': data['current']['pressure'],
            'vent_vitesse': data['current']['wind_speed'],
            'vent_direction': data['current']['wind_deg'],
            'description': data['current']['weather'][0]['description'],
            
            # Données quotidiennes (moyennes/évolution)
            'temp_jour_moy': data['daily'][0]['temp']['day'],
            'temp_jour_min': data['daily'][0]['temp']['min'],
            'temp_jour_max': data['daily'][0]['temp']['max'],
            'precipitation': data['daily'][0].get('rain', 0),
            'nuage_couverture': data['daily'][0]['clouds'],
            'uv_index': data['daily'][0]['uvi'],
            
            # Indicateurs pour l'analyse climatique
            'amplitude_thermique': data['daily'][0]['temp']['max'] - data['daily'][0]['temp']['min'],
            'risque_pluie': 1 if data['daily'][0].get('rain') else 0
        }
        
        # Création du dossier de destination
        os.makedirs(f"data/raw/{date}", exist_ok=True)
        
        # Sauvegarde en CSV
        output_path = f"data/raw/{date}/meteo_{city.lower().replace(' ', '_')}.csv"
        pd.DataFrame([weather_data]).to_csv(output_path, index=False)
        
        logging.info(f"Données pour {city} sauvegardées avec succès")
        return True
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur réseau/API pour {city}: {str(e)}")
    except (KeyError, IndexError) as e:
        logging.error(f"Structure de réponse inattendue pour {city}: {str(e)}")
    except Exception as e:
        logging.error(f"Erreur inattendue pour {city}: {type(e).__name__}: {str(e)}")
        
    return False