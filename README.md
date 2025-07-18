# STD23015_examen_meteo_DONNEES2
# Projet : Comparaison climatique entre grandes villes du monde

## Objectif du projet

Ce projet vise à construire un pipeline complet de **collecte**, **traitement**, **modélisation** et **visualisation** de données météo afin de répondre à la problématique suivante :

> **Problématique :** Comment évolue le climat dans plusieurs grandes villes ? Peut-on classer les villes selon leur stabilité, leur niveau de chaleur ou leur variabilité ?

### 🔍 Contexte

Les effets du changement climatique s'observent à différentes échelles, notamment en milieu urbain. Chaque ville subit des variations de températures, d'humidité ou de précipitations selon sa position géographique, son environnement, et les saisons.

L'objectif est d'étudier, comparer et visualiser l'évolution climatique dans plusieurs villes du monde à partir de données historiques et actuelles afin de :

* Identifier les villes les plus chaudes ou les plus stables
* Comprendre les schémas saisonniers
* Suivre l'évolution des indicateurs climatiques clés

---

##  Périmètre et sources

* **Villes analysées** : Paris, New York, Tokyo, Sydney, Moscow, Antananarivo
* **Période couverte** : 8 ans d’historique météo
* **Sources de données** :

  * [OpenWeather API](https://openweathermap.org/)
  * [Open-Meteo Archive API](https://open-meteo.com/)

---

## Architecture du projet

###  Structure des fichiers

```
├───dags
│       weather_etl_dag.py
│       
├───data
│   │   historical_extraction.log
│   │   weather_extraction.log
│   │   
│   ├───analysis
│   │       climate_analysis.csv
│   │       
│   ├───current
│   │       2025-07-05_Antananarivo.csv
│   │       2025-07-05_Moscow.csv
│   │       2025-07-05_New York.csv
│   │       2025-07-05_Paris.csv
│   │       2025-07-05_Sydney.csv
│   │       2025-07-05_São Paulo.csv
│   │       2025-07-05_Tokyo.csv
│   │       
│   ├───historical
│   │       antananarivo_historical.csv
│   │       moscow_historical.csv
│   │       new_york_historical.csv
│   │       paris_historical.csv
│   │       sydney_historical.csv
│   │       tokyo_historical.csv
│   │       
│   ├───processed
│   │       current_global.csv
│   │       historical_global.csv
│   │       
│   └───star_schema
│           dim_climat.csv
│           dim_temps.csv
│           dim_ville.csv
│           fact_weather.csv
│           
├───EDA
│       EDA.ipynb
│       
└───script
        extract.py
        extract_historic.py
        merge.py
        transform.py
        
```

###  Orchestration (Airflow)

Le DAG `weather_etl_dag.py` orchestre l’exécution de :

1.  Extraction des données historiques et actuelles
2.  Fusion des données en fichiers globaux
3.  Transformation en modèle en étoile
4.  Validation de l’output

---

##  Modèle dimensionnel (schéma en étoile)

###  Table de faits : `fact_weather.csv`

Contient les métriques quantitatives pour chaque jour et ville :

* Température, température min/max
* Précipitation
* Vent, humidité, pression
* Description météo

###  Dimensions :

* `dim_ville.csv` : ID ville, nom, pays, latitude/longitude
* `dim_temps.csv` : date, mois, saison, jour de la semaine
* `dim_climat.csv` : catégorie climatique (Tropical, Tempéré, etc.)

---

##  Indicateurs clés

*  Température moyenne par ville / mois / année
*  Écart-type des températures (variabilité climatique)
*  Nombre de jours pluvieux
*  Classement des villes par chaleur moyenne ou stabilité

---

##  Dashboard Power BI

###  Fonctionnalités :

*  Suivi des métriques par ville (température, précipitation)
*  Filtres dynamiques : ville, mois, année, climat, métrique
*  Graphiques : ligne, barres, carte (heatmap)
*  Comparaison entre villes : température moyenne, variabilité, jours de pluie

---

##  Exemple d'interprétation

*  Paris et Tokyo présentent une **grande variabilité saisonnière** (type Continental)
*  Antananarivo est globalement plus stable sur l’année mais plus humide
*  New York a une température moyenne élevée avec des pics estivaux marqués

---

##  Conclusion

Ce projet met en place un pipeline réutilisable et automatisé pour analyser le climat de grandes villes. Il combine extraction multi-source, modélisation robuste et visualisation interactive permettant de répondre aux enjeux climatiques urbains.

> “Les villes peuvent être comparées objectivement sur des critères météo mesurés et standardisés.”

---

##  Pour lancer le projet

1. **Cloner ce dépôt** :

```bash
git clone https://github.com/votre-utilisateur/comparaison-climat.git
```

2. **Configurer Airflow & les variables (**\`\`**)**
3. **Exécuter les DAGs dans l’interface Airflow**
4. **Analyser les CSV ou charger les données dans Power BI**

---

##  Licence 

MIT © 2025
