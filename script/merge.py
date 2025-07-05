# merge.py
import pandas as pd
import os
import logging
from datetime import datetime

# merge.py - Version corrigée
def merge_current_data() -> str:
    input_dir = "data/current"
    output_file = "data/processed/current_global.csv"
    
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Liste tous les fichiers actuels
        all_files = [f for f in os.listdir(input_dir) if f.endswith('_current.csv')]
        
        if not all_files:
            logging.error("Aucun fichier de données actuelles trouvé dans data/current/")
            # Crée un fichier vide avec les colonnes attendues
            columns = [
                'ville', 'date_extraction', 'date_donnees', 'temperature', 
                'temp_min', 'temp_max', 'humidite', 'pression', 'description',
                'vent_vitesse', 'pluie_1h', 'couverture_nuageuse'
            ]
            pd.DataFrame(columns=columns).to_csv(output_file, index=False)
            return output_file
        
        # Charge et concatène tous les fichiers
        dfs = []
        for file in all_files:
            dfs.append(pd.read_csv(f"{input_dir}/{file}"))
        
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df.to_csv(output_file, index=False)
        logging.info(f"Fichier current_global.csv créé avec {len(merged_df)} enregistrements")
        return output_file
        
    except Exception as e:
        logging.error(f"Erreur critique lors de la fusion : {str(e)}")
        raise

def merge_historical_data() -> str:
    """
    Fusionne tous les fichiers CSV de données historiques en un seul fichier global
    
    Returns:
        str: Chemin du fichier global créé/mis à jour
    """
    input_dir = "data/historical"
    output_file = "data/processed/historical_global.csv"
    
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Initialisation du DataFrame global
        global_df = pd.DataFrame()
        if os.path.exists(output_file):
            global_df = pd.read_csv(output_file)
        
        # Collecte et traitement des fichiers historiques
        new_data = []
        for file in os.listdir(input_dir):
            if file.endswith('_historical.csv'):
                city = file.replace('_historical.csv', '').replace('_', ' ')
                df = pd.read_csv(f"{input_dir}/{file}")
                df['date_import'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                new_data.append(df)
        
        if not new_data:
            logging.warning("Aucune nouvelle donnée historique à fusionner")
            return output_file
        
        # Fusion (pas de déduplication nécessaire pour les historiques complets)
        updated_df = pd.concat([global_df] + new_data, ignore_index=True)
        
        # Tri par ville et date
        updated_df = updated_df.sort_values(by=['ville', 'date'])
        
        # Sauvegarde
        updated_df.to_csv(output_file, index=False)
        logging.info(f"Fusion historique réussie : données de {len(new_data)} villes")
        return output_file
        
    except Exception as e:
        logging.error(f"Erreur lors de la fusion des données historiques : {str(e)}")
        raise

def create_analysis_file() -> str:
    """
    Crée un fichier d'analyse combinant données actuelles et indicateurs historiques
    
    Returns:
        str: Chemin du fichier d'analyse créé
    """
    try:
        # Chargement des données fusionnées
        current_df = pd.read_csv("data/processed/current_global.csv")
        historical_df = pd.read_csv("data/processed/historical_global.csv")
        
        # Calcul des statistiques historiques par ville
        stats = historical_df.groupby('ville').agg({
            'temp_max': ['mean', 'std'],
            'temp_min': ['mean', 'std'],
            'precipitation': ['mean', 'sum'],
            'pluie': ['mean', 'sum'],
            'neige': ['mean', 'sum']
        }).reset_index()
        
        # Renommage des colonnes
        stats.columns = [
            'ville',
            'temp_max_moyenne', 'temp_max_std',
            'temp_min_moyenne', 'temp_min_std',
            'precipitation_moyenne', 'precipitation_totale',
            'pluie_moyenne', 'pluie_totale',
            'neige_moyenne', 'neige_totale'
        ]
        
        # Fusion avec les données actuelles
        analysis_df = pd.merge(
            current_df,
            stats,
            on='ville',
            how='left'
        )
        
        # Calcul d'indicateurs complémentaires
        analysis_df['variabilite_climatique'] = (
            analysis_df['temp_max_std'] + analysis_df['temp_min_std']
        )
        analysis_df['stabilite_climatique'] = (
            1 / (1 + analysis_df['variabilite_climatique'])
        )
        
        # Sauvegarde
        os.makedirs("data/analysis", exist_ok=True)
        output_file = "data/analysis/climate_analysis.csv"
        analysis_df.to_csv(output_file, index=False)
        
        logging.info(f"Fichier d'analyse créé avec {len(analysis_df)} enregistrements")
        return output_file
        
    except Exception as e:
        logging.error(f"Erreur lors de la création du fichier d'analyse : {str(e)}")
        raise

def main():
    """Fonction principale pour la fusion des données"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Fusion des données actuelles
        current_path = merge_current_data()
        logging.info(f"Données actuelles fusionnées : {current_path}")
        
        # Fusion des données historiques
        historical_path = merge_historical_data()
        logging.info(f"Données historiques fusionnées : {historical_path}")
        
        # Création du fichier d'analyse
        analysis_path = create_analysis_file()
        logging.info(f"Fichier d'analyse créé : {analysis_path}")
        
    except Exception as e:
        logging.error(f"Échec du processus de fusion : {str(e)}")

if __name__ == "__main__":
    main()