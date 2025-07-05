# weather_etl_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# Import des fonctions de vos scripts
from scripts.extract import extract_current_weather
from scripts.extract_historic import main as extract_historical_weather
from scripts.merge import main as merge_data
from scripts.transform import main as transform_data

# Configuration par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Liste des villes à traiter
CITIES = ["Paris", "New York", "Tokyo", "Sydney", "Moscow", "Antananarivo"]

with DAG(
    'climate_comparison_pipeline',
    default_args=default_args,
    description='ETL pour la comparaison climatique entre villes',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'climate'],
) as dag:

    # ========== Tâches d'Extraction ==========
    extract_current_task = PythonOperator(
        task_id='extract_current_weather',
        python_callable=extract_current_weather,
        op_args=[CITIES, Variable.get("OPENWEATHER_API_KEY")],
    )

    extract_historical_task = PythonOperator(
        task_id='extract_historical_weather',
        python_callable=extract_historical_weather,
    )

    # ========== Tâche de Fusion ==========
    merge_task = PythonOperator(
        task_id='merge_data',
        python_callable=merge_data,
    )

    # ========== Tâche de Transformation ==========
    transform_task = PythonOperator(
        task_id='transform_to_star_schema',
        python_callable=transform_data,
    )

    # ========== Tâche de Validation ==========
    def validate_output():
        import os
        required_files = [
            'data/star_schema/dim_ville.csv',
            'data/star_schema/dim_temps.csv',
            'data/star_schema/dim_climat.csv',
            'data/star_schema/fact_current.csv',
            'data/star_schema/fact_historical.csv'
        ]
        for file in required_files:
            if not os.path.exists(file):
                raise ValueError(f"Fichier {file} manquant après transformation")
            if os.path.getsize(file) == 0:
                raise ValueError(f"Fichier {file} est vide")

    validate_task = PythonOperator(
        task_id='validate_output',
        python_callable=validate_output,
    )

    # ========== Orchestration ==========
    [extract_current_task, extract_historical_task] >> merge_task >> transform_task >> validate_task