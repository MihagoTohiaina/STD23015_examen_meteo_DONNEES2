import pandas as pd
import os
import logging
from datetime import datetime
import numpy as np

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def prepare_current_data(df):
    """Prépare les données actuelles pour l'unification"""
    df = df.copy()
    # Convertir le timestamp UNIX en datetime et formater en YYYY-MM-DD
    df['date'] = pd.to_datetime(df['timestamp_donnees'], unit='s').dt.strftime('%Y-%m-%d')
    df['source_type'] = 'current'
    df['climat_id'] = assign_climate_category(df['temperature'])
    
    # Standardisation des noms de colonnes pour la table de faits unifiée
    df.rename(columns={
        'precipitation': 'precipitation_current', # Renommer pour distinguer si nécessaire, ou unifier
        'conditions': 'description',
        # 'precipitation' de OpenWeather est une valeur d'une heure. 
        # Pour l'unifier avec historical (qui est souvent daily sum), il faut une décision.
        # Ici, je la garde comme 'precipitation_current' et la mappe à 'precipitation' dans fact_weather.
    }, inplace=True)
    
    # Assurer que les colonnes 'pluie' et 'neige' sont présentes (avec NaN si non applicable)
    for col in ['pluie', 'neige']:
        if col not in df.columns:
            df[col] = np.nan
            
    # Sélectionner et réordonner les colonnes pour qu'elles soient cohérentes avec historical_prep
    # et la structure de la table de faits
    final_cols = [
        'ville', 'date', 'temperature', 'temp_min', 'temp_max', 
        'humidite', 'pression', 'vent_vitesse', 'precipitation_current', # utiliser le nom renommé
        'pluie', 'neige', 'couverture_nuageuse', 'description', 
        'source_type', 'climat_id'
    ]
    
    # Assurez-vous que toutes les colonnes requises sont présentes, en ajoutant celles qui manquent avec NaN
    for col in final_cols:
        if col not in df.columns:
            df[col] = np.nan
    
    return df[final_cols]


def prepare_historical_data(df):
    """Prépare les données historiques pour l'unification"""
    df = df.copy()
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d') # Formater en YYYY-MM-DD
    df['source_type'] = 'historical'
    
    if 'temperature' not in df.columns:
        df['temperature'] = (df['temp_max'] + df['temp_min']) / 2
    
    df['climat_id'] = assign_climate_category(df['temperature'])
    
    # Ajouter des colonnes manquantes spécifiques aux données actuelles avec valeurs nulles
    for col in ['humidite', 'pression', 'vent_vitesse', 'couverture_nuageuse']:
        if col not in df.columns:
            df[col] = np.nan
    if 'description' not in df.columns:
        df['description'] = None # ou une valeur par défaut
        
    # Renommer 'precipitation' pour éviter un conflit si vous utilisez 'precipitation_current'
    # et que vous voulez une colonne unifiée 'precipitation' dans la table de faits.
    # Si 'precipitation' de historical est la somme journalière, et 'precipitation_current' est horaire,
    # il faut décider comment les unifier ou les garder séparées.
    # Pour l'instant, je vais harmoniser le nom de la colonne de précipitation.
    df.rename(columns={'precipitation': 'precipitation_historical'}, inplace=True)

    # Sélectionner et réordonner les colonnes pour qu'elles soient cohérentes avec current_prep
    final_cols = [
        'ville', 'date', 'temperature', 'temp_min', 'temp_max', 
        'humidite', 'pression', 'vent_vitesse', 'precipitation_historical', # utiliser le nom renommé
        'pluie', 'neige', 'couverture_nuageuse', 'description', 
        'source_type', 'climat_id'
    ]

    for col in final_cols:
        if col not in df.columns:
            df[col] = np.nan
            
    return df[final_cols]


def assign_climate_category(temperatures):
    """Assignation des catégories climatiques"""
    conditions = [
        (temperatures >= 30),
        (temperatures <= 0),
        (temperatures > 20), 
        (temperatures > 10)
    ]
    choices = [1, 5, 2, 3]   # IDs correspondant à dim_climat: Tropical, Polaire, Sec (?), Tempéré
    # ATTENTION : La description de dim_climat est: 'Tropical', 'Sec', 'Tempéré', 'Continental', 'Polaire'
    # Il y a une discrépance entre vos choix [1,5,2,3] et les IDs du fichier dim_climat.
    # Je vais suivre les IDs que vous avez définis dans dim_climat.csv pour 'type':
    # 1: Tropical, 2: Sec, 3: Tempéré, 4: Continental, 5: Polaire
    # Basé sur vos règles de température:
    # >= 30 -> Tropical (ID 1)
    # <= 0  -> Polaire (ID 5)
    # > 20  -> Sec (ID 2, si on considère que >20 est "chaud et sec" ou similaire à tropical)
    # > 10  -> Tempéré (ID 3)
    # Default -> Continental (ID 4)
    # C'est une interprétation, vous pouvez l'ajuster.
    choices = [1, 5, 2, 3] # ID de dim_climat: 1:Tropical, 5:Polaire, 2:Sec, 3:Tempéré
    return np.select(conditions, choices, default=4) # Default 4: Continental

def create_dimensions(df):
    """Crée toutes les dimensions avec gestion robuste des types"""
    os.makedirs("data/star_schema", exist_ok=True)
    
    # Dimension Ville
    dim_ville = pd.DataFrame({
        'ville_id': range(1, len(df['ville'].unique()) + 1),
        'ville': df['ville'].unique()
    })
    
    # Ajout des métadonnées géographiques
    geo_data = {
        'Paris': ('France', 48.8566, 2.3522),
        'New York': ('USA', 40.7128, -74.0060),
        'Tokyo': ('Japan', 35.6762, 139.6503),
        'Sydney': ('Australia', -33.8688, 151.2093),
        'Moscow': ('Russia', 55.7558, 37.6173),
        'Antananarivo': ('Madagascar', -18.8792, 47.5079),
        'São Paulo': ('Brazil', -23.5505, -46.6333) # Ajout de São Paulo ici
    }
    
    dim_ville['pays'] = dim_ville['ville'].map(lambda x: geo_data.get(x, ('Inconnu',))[0])
    dim_ville['latitude'] = dim_ville['ville'].map(lambda x: geo_data.get(x, (None, None, None))[1])
    dim_ville['longitude'] = dim_ville['ville'].map(lambda x: geo_data.get(x, (None, None, None))[2])
    
    # Dimension Temps
    # Utiliser les dates de la table de faits unifiée
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
    
    # Dimension Climat (déjà définie)
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

def create_unified_star_schema():
    """Crée un schéma en étoile avec une seule table de faits unifiée, incluant déduplication."""
    try:
        # 1. Chargement des données fusionnées
        current = pd.read_csv("data/processed/current_global.csv")
        historical = pd.read_csv("data/processed/historical_global.csv")

        # 2. Préparation des données avec conversion de type explicite et harmonisation des colonnes
        current_prep = prepare_current_data(current)
        historical_prep = prepare_historical_data(historical)
        
        # 3. Fusion unifiée des faits (concaténation)
        unified_fact = pd.concat([current_prep, historical_prep], ignore_index=True)
        
        # NOUVEAU : Déduplication de la table de faits unifiée
        # Prioriser les données 'current' si une même ville et date existe dans les deux sources.
        # Cela signifie que si vous avez des données OpenWeather pour le 2025-07-05 et des données
        # historiques pour la même date, les données OpenWeather seront conservées.
        unified_fact['date'] = pd.to_datetime(unified_fact['date']).dt.strftime('%Y-%m-%d') # Assurer format date string
        
        # Créer une colonne de priorité pour la déduplication
        unified_fact['priority'] = unified_fact['source_type'].map({'current': 1, 'historical': 2})
        unified_fact = unified_fact.sort_values(by=['ville', 'date', 'priority'], ascending=[True, True, True])
        
        # Dédupliquer en gardant la ligne avec la priorité la plus basse (donc 'current' si présente)
        unified_fact = unified_fact.drop_duplicates(subset=['ville', 'date'], keep='first')
        unified_fact = unified_fact.drop(columns=['priority']) # Supprimer la colonne de priorité

        # Harmonisation de la colonne 'precipitation'
        # Utiliser 'precipitation_current' si elle existe, sinon 'precipitation_historical'
        # et nommer le tout 'precipitation'.
        unified_fact['precipitation'] = unified_fact['precipitation_current'].fillna(unified_fact['precipitation_historical'])
        # Supprimer les colonnes sources spécifiques après harmonisation
        unified_fact.drop(columns=['precipitation_current', 'precipitation_historical'], inplace=True, errors='ignore')
        
        # 4. Création des dimensions à partir du DataFrame de faits dédupliqué
        dims = create_dimensions(unified_fact)
        
        # 5. Préparation des IDs pour la jointure finale
        # Assurez-vous que les colonnes 'ville' et 'date' sont propres pour la jointure
        unified_fact['date'] = pd.to_datetime(unified_fact['date']) # Convertir en datetime pour joindre à dim_temps
        dims['temps']['date'] = pd.to_datetime(dims['temps']['date']) # Convertir en datetime pour joindre à unified_fact
        
        # 6. Jointure finale pour obtenir les IDs des dimensions
        final_fact = unified_fact.merge(
            dims['ville'][['ville_id', 'ville']], 
            on='ville', 
            how='left'
        ).merge(
            dims['temps'][['date_id', 'date']],
            on='date',
            how='left'
        )
        
        # 7. Sauvegarde des résultats
        save_results(dims, final_fact)
        
        logging.info("Schéma en étoile unifié et dédupliqué créé avec succès")
        return {
            'dim_ville': "data/star_schema/dim_ville.csv",
            'dim_temps': "data/star_schema/dim_temps.csv", 
            'dim_climat': "data/star_schema/dim_climat.csv",
            'fact_weather': "data/star_schema/fact_weather.csv"
        }

    except Exception as e:
        logging.error(f"Erreur lors de la transformation : {str(e)}")
        raise

def save_results(dims, fact):
    """Sauvegarde la table de faits unifiée et les dimensions."""
    os.makedirs("data/star_schema", exist_ok=True) 

    # Les dimensions sont déjà sauvegardées dans create_dimensions. On s'assure juste ici.
    dims['ville'].to_csv("data/star_schema/dim_ville.csv", index=False)
    dims['temps'].to_csv("data/star_schema/dim_temps.csv", index=False) 
    dims['climat'].to_csv("data/star_schema/dim_climat.csv", index=False)

    final_cols = [
        'ville_id', 'date_id', 'climat_id',
        'temperature', 'temp_min', 'temp_max',
        'humidite', 'pression', 'vent_vitesse',
        'precipitation', # Colonne unifiée
        'pluie',         # Spécifique historique (peut contenir NaN)
        'neige',         # Spécifique historique (peut contenir NaN)
        'couverture_nuageuse',
        'description', 'source_type'
    ]
    
    # Assurer que toutes les colonnes requises sont dans 'fact', ajouter si manquant avec NaN
    for col in final_cols:
        if col not in fact.columns:
            fact[col] = np.nan
    
    # Sélection et ordonnancement des colonnes
    fact = fact[final_cols]
    
    # Conversion des types pour cohérence
    fact['date_id'] = fact['date_id'].astype(str)
    # Gérer les NaNs potentiels dans ville_id et climat_id avant de convertir en int
    # Cela peut arriver si une ville ou un type de climat n'a pas été mappé correctement
    fact['ville_id'] = fact['ville_id'].fillna(-1).astype(int) 
    fact['climat_id'] = fact['climat_id'].fillna(-1).astype(int)
    
    # Convertir les colonnes numériques qui peuvent avoir des NaNs à des types supportant NaN
    # Les types 'float' peuvent contenir des NaN.
    # Pour les entiers, Pandas a des types comme `Int64` (avec un grand 'I') qui gèrent les NaNs.
    numerical_cols = [
        'temperature', 'temp_min', 'temp_max', 'humidite', 'pression', 
        'vent_vitesse', 'precipitation', 'pluie', 'neige', 'couverture_nuageuse'
    ]
    for col in numerical_cols:
        if col in fact.columns:
            fact[col] = pd.to_numeric(fact[col], errors='coerce') # 'coerce' met NaN si la conversion échoue
    
    fact.to_csv("data/star_schema/fact_weather.csv", index=False)

if __name__ == "__main__":
    create_unified_star_schema()