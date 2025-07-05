import pandas as pd
import os
import logging
from datetime import datetime
import numpy as np

def create_unified_star_schema():
    """Crée un schéma en étoile avec une seule table de faits unifiée"""
    try:
        # 1. Chargement des données fusionnées
        current = pd.read_csv("data/processed/current_global.csv")
        historical = pd.read_csv("data/processed/historical_global.csv")

        # 2. Préparation des données avec conversion de type explicite
        current_prep = prepare_current_data(current)
        historical_prep = prepare_historical_data(historical)
        
        # 3. Fusion unifiée
        unified_fact = pd.concat([current_prep, historical_prep], ignore_index=True)
        
        # 4. Création des dimensions avec types cohérents
        dims = create_dimensions(unified_fact)
        
        # 5. Conversion des types avant jointure
        unified_fact['date'] = pd.to_datetime(unified_fact['date'])
        dims['temps']['date'] = pd.to_datetime(dims['temps']['date'])
        
        # 6. Jointure finale
        final_fact = unified_fact.merge(
            dims['ville'][['ville_id', 'ville']], 
            on='ville', 
            how='left'
        ).merge(
            dims['temps'][['date_id', 'date']],
            on='date',
            how='left'
        )
        
        # 7. Sauvegarde
        save_results(dims, final_fact)
        
        logging.info("Schéma en étoile unifié créé avec succès")
        return {
            'dim_ville': "data/star_schema/dim_ville.csv",
            'dim_temps': "data/star_schema/dim_temps.csv", 
            'dim_climat': "data/star_schema/dim_climat.csv",
            'fact_weather': "data/star_schema/fact_weather.csv"
        }

    except Exception as e:
        logging.error(f"Erreur lors de la transformation : {str(e)}")
        raise

def prepare_current_data(df):
    """Prépare les données actuelles pour l'unification"""
    df = df.copy()
    # Conversion explicite en date
    df['date'] = pd.to_datetime(df['date_donnees']).dt.date
    df['source_type'] = 'current'
    df['climat_id'] = assign_climate_category(df['temperature'])
    
    # Standardisation des noms de colonnes
    df.rename(columns={
        'pluie_1h': 'precipitation',
        'conditions': 'description'
    }, inplace=True)
    
    return df

def prepare_historical_data(df):
    """Prépare les données historiques pour l'unification"""
    df = df.copy()
    # Conversion explicite en date
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['source_type'] = 'historical'
    
    # Calcul de la température moyenne si nécessaire
    if 'temperature' not in df.columns:
        df['temperature'] = (df['temp_max'] + df['temp_min']) / 2
    
    df['climat_id'] = assign_climate_category(df['temperature'])
    
    # Ajout de colonnes manquantes avec valeurs nulles
    for col in ['humidite', 'pression', 'vent_vitesse', 'couverture_nuageuse', 'description']:
        if col not in df.columns:
            df[col] = None
    
    return df

def assign_climate_category(temperatures):
    """Assignation des catégories climatiques"""
    conditions = [
        (temperatures >= 30),
        (temperatures <= 0),
        (temperatures > 20), 
        (temperatures > 10)
    ]
    choices = [1, 5, 2, 3]  # IDs correspondant à dim_climat
    return np.select(conditions, choices, default=4)

def create_dimensions(df):
    """Crée toutes les dimensions avec gestion robuste des types"""
    os.makedirs("data/star_schema", exist_ok=True)
    
    # Dimension Ville
    dim_ville = pd.DataFrame({
        'ville_id': range(1, len(df['ville'].unique()) + 1),
        'ville': df['ville'].unique()
    })
    
    # Ajout des métadonnées géographiques (à compléter)
    geo_data = {
        'Paris': ('France', 48.8566, 2.3522),
        'New York': ('USA', 40.7128, -74.0060),
        'Tokyo': ('Japan', 35.6762, 139.6503),
        'Sydney': ('Australia', -33.8688, 151.2093),
        'Moscow': ('Russia', 55.7558, 37.6173),
        'Antananarivo': ('Madagascar', -18.8792, 47.5079)
    }
    
    dim_ville['pays'] = dim_ville['ville'].map(lambda x: geo_data.get(x, ('Inconnu',))[0])
    dim_ville['latitude'] = dim_ville['ville'].map(lambda x: geo_data.get(x, (None, 0))[1])
    dim_ville['longitude'] = dim_ville['ville'].map(lambda x: geo_data.get(x, (None, 0, 0))[2])
    
    # Dimension Temps
    dates = pd.to_datetime(df['date'].unique())
    dim_temps = pd.DataFrame({
        'date_id': dates.strftime('%Y%m%d'),
        'date': dates,
        'jour': dates.day,
        'mois': dates.month,
        'mois_nom': dates.month_name(),
        'annee': dates.year,
        'saison': dates.quarter.map({1: 'Hiver', 2: 'Printemps', 3: 'Été', 4: 'Automne'}),
        'jour_semaine': dates.day_name()
    })
    
    # Dimension Climat
    dim_climat = pd.DataFrame({
        'climat_id': [1, 2, 3, 4, 5],
        'type': ['Tropical', 'Sec', 'Tempéré', 'Continental', 'Polaire'],
        'plage_temp': ['25-35°C', '20-40°C', '10-25°C', '-10-30°C', '-40-10°C'],
        'description': ['Chaud et humide', 'Chaud et sec', 'Doux', 'Variations saisonnières', 'Froid']
    })
    
    # Sauvegarde
    dim_ville.to_csv("data/star_schema/dim_ville.csv", index=False)
    dim_temps.to_csv("data/star_schema/dim_temps.csv", index=False) 
    dim_climat.to_csv("data/star_schema/dim_climat.csv", index=False)
    
    return {'ville': dim_ville, 'temps': dim_temps, 'climat': dim_climat}

def save_results(dims, fact):
    """Sauvegarde la table de faits unifiée"""
    final_cols = [
        'ville_id', 'date_id', 'climat_id',
        'temperature', 'temp_min', 'temp_max',
        'humidite', 'pression', 'vent_vitesse',
        'precipitation', 'couverture_nuageuse',
        'description', 'source_type'
    ]
    
    # Sélection et ordonnancement des colonnes
    fact = fact[final_cols]
    
    # Conversion des types pour cohérence
    fact['date_id'] = fact['date_id'].astype(str)
    fact['ville_id'] = fact['ville_id'].astype(int)
    fact['climat_id'] = fact['climat_id'].astype(int)
    
    fact.to_csv("data/star_schema/fact_weather.csv", index=False)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    create_unified_star_schema()