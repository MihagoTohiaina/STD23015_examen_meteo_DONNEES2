import pandas as pd
import os
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def merge_current_data() -> str:
    """
    Fusionne tous les fichiers CSV de données actuelles en un seul fichier global.
    
    Returns:
        str: Chemin du fichier global créé/mis à jour
    """
    input_dir = "data/current"
    output_file = "data/processed/current_global.csv"
    
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # CORRECTION : Rechercher les fichiers avec le format "YYYY-MM-DD_Ville.csv"
        # et non "_current.csv"
        all_files = [f for f in os.listdir(input_dir) if f.endswith('.csv') and "_" in f.split(".")[0]]
        
        if not all_files:
            logging.warning("Aucun fichier de données actuelles trouvé dans data/current/. Création d'un fichier vide.")
            columns = [
                'ville', 'pays', 'date_extraction', 'timestamp_donnees', 'temperature', 
                'temp_min', 'temp_max', 'humidite', 'pression', 'vent_vitesse', 
                'vent_direction', 'precipitation', 'couverture_nuageuse', 'conditions', 'timezone'
            ]
            pd.DataFrame(columns=columns).to_csv(output_file, index=False)
            return output_file
        
        dfs = []
        for file in all_files:
            file_path = os.path.join(input_dir, file)
            try:
                df = pd.read_csv(file_path)
                # Ajout de la date de la donnée pour le matching avec la dimension temps
                # La date est dans le nom du fichier pour les données actuelles
                file_date_str = file.split('_')[0] 
                df['date_donnees'] = file_date_str 
                dfs.append(df)
            except Exception as e:
                logging.error(f"Erreur de lecture du fichier {file_path}: {e}")
                continue # Passer au fichier suivant
        
        if not dfs:
            logging.error("Aucun DataFrame valide à fusionner pour les données actuelles.")
            columns = [ # Assurez-vous que les colonnes sont définies pour un DF vide
                'ville', 'pays', 'date_extraction', 'timestamp_donnees', 'temperature', 
                'temp_min', 'temp_max', 'humidite', 'pression', 'vent_vitesse', 
                'vent_direction', 'precipitation', 'couverture_nuageuse', 'conditions', 'timezone', 'date_donnees'
            ]
            pd.DataFrame(columns=columns).to_csv(output_file, index=False)
            return output_file

        merged_df = pd.concat(dfs, ignore_index=True)
        
        # NOUVEAU : Déduplication des données actuelles.
        # En théorie, un seul enregistrement par ville et par jour d'extraction est attendu.
        merged_df = merged_df.drop_duplicates(subset=['ville', 'date_donnees'], keep='last')
        
        merged_df.to_csv(output_file, index=False)
        logging.info(f"Fichier current_global.csv créé avec {len(merged_df)} enregistrements uniques.")
        return output_file
        
    except Exception as e:
        logging.error(f"Erreur critique lors de la fusion des données actuelles : {str(e)}")
        raise

def merge_historical_data() -> str:
    """
    Fusionne tous les fichiers CSV de données historiques en un seul fichier global,
    en assurant la déduplication.
    
    Returns:
        str: Chemin du fichier global créé/mis à jour
    """
    input_dir = "data/historical"
    output_file = "data/processed/historical_global.csv"
    
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        all_historical_files = [f for f in os.listdir(input_dir) if f.endswith('_historical.csv')]
        
        if not all_historical_files:
            logging.warning("Aucun fichier de données historiques trouvé dans data/historical/. Création d'un fichier vide.")
            columns = ['ville', 'date', 'temp_max', 'temp_min', 'precipitation', 'pluie', 'neige', 'date_import']
            pd.DataFrame(columns=columns).to_csv(output_file, index=False)
            return output_file

        dfs_new_collection = []
        for file in all_historical_files:
            file_path = os.path.join(input_dir, file)
            try:
                df = pd.read_csv(file_path)
                df['date_import'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                dfs_new_collection.append(df)
            except Exception as e:
                logging.error(f"Erreur de lecture du fichier historique {file_path}: {e}")
                continue # Passer au fichier suivant
        
        if not dfs_new_collection:
            logging.warning("Aucune nouvelle donnée historique valide à fusionner.")
            return output_file # Retourne le chemin du fichier existant/vide

        newly_collected_df = pd.concat(dfs_new_collection, ignore_index=True)
        
        # Convertir la colonne 'date' en format standard pour une comparaison fiable
        newly_collected_df['date'] = pd.to_datetime(newly_collected_df['date']).dt.strftime('%Y-%m-%d')

        existing_global_df = pd.DataFrame()
        if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
            try:
                existing_global_df = pd.read_csv(output_file)
                existing_global_df['date'] = pd.to_datetime(existing_global_df['date']).dt.strftime('%Y-%m-%d')
            except Exception as e:
                logging.warning(f"Impossible de lire le fichier historique global existant, il sera recréé : {e}")
                existing_global_df = pd.DataFrame() # Réinitialiser si erreur de lecture

        # NOUVEAU : Concaténation et déduplication pour éviter les doublons
        combined_df = pd.concat([existing_global_df, newly_collected_df], ignore_index=True)
        
        # Déduplication : Conserver la dernière entrée pour chaque combinaison (ville, date)
        # Ceci est crucial pour éviter la répétition des mêmes données historiques à chaque run.
        deduplicated_df = combined_df.drop_duplicates(subset=['ville', 'date'], keep='last')
        
        deduplicated_df = deduplicated_df.sort_values(by=['ville', 'date']).reset_index(drop=True)
        
        deduplicated_df.to_csv(output_file, index=False)
        logging.info(f"Fusion historique réussie : {len(deduplicated_df)} enregistrements uniques.")
        return output_file
        
    except Exception as e:
        logging.error(f"Erreur lors de la fusion des données historiques : {str(e)}")
        raise

def create_analysis_file() -> str:
    """
    Crée un fichier d'analyse combinant données actuelles et indicateurs historiques.
    
    Returns:
        str: Chemin du fichier d'analyse créé
    """
    try:
        # Chargement des données fusionnées
        current_df = pd.read_csv("data/processed/current_global.csv")
        historical_df = pd.read_csv("data/processed/historical_global.csv")
        
        # Assurez-vous que la colonne 'date' est bien au format date pour le regroupement
        historical_df['date'] = pd.to_datetime(historical_df['date'])

        # Calcul des statistiques historiques par ville
        stats = historical_df.groupby('ville').agg({
            'temp_max': ['mean', 'std'],
            'temp_min': ['mean', 'std'],
            'precipitation': ['mean', 'sum'],
            'pluie': ['mean', 'sum'],
            'neige': ['mean', 'sum']
        }).reset_index()
        
        # Renommage des colonnes (aplatir le MultiIndex)
        stats.columns = ['_'.join(col).strip() if col[1] else col[0] for col in stats.columns.values]
        stats.rename(columns={
            'temp_max_mean': 'temp_max_moyenne', 'temp_max_std': 'temp_max_std',
            'temp_min_mean': 'temp_min_moyenne', 'temp_min_std': 'temp_min_std',
            'precipitation_mean': 'precipitation_moyenne', 'precipitation_sum': 'precipitation_totale',
            'pluie_mean': 'pluie_moyenne', 'pluie_sum': 'pluie_totale',
            'neige_mean': 'neige_moyenne', 'neige_sum': 'neige_totale'
        }, inplace=True)
        
        # Fusion avec les données actuelles
        analysis_df = pd.merge(
            current_df,
            stats,
            on='ville',
            how='left'
        )
        
        # Calcul d'indicateurs complémentaires (gérer les NaN pour le STD)
        analysis_df['variabilite_climatique'] = analysis_df['temp_max_std'].fillna(0) + analysis_df['temp_min_std'].fillna(0)
        # Éviter la division par zéro si variabilite_climatique est 0
        analysis_df['stabilite_climatique'] = analysis_df['variabilite_climatique'].apply(lambda x: 1 / (1 + x) if x is not None and x > 0 else 1) # Si 0 ou NaN, stabilité est 1
        
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