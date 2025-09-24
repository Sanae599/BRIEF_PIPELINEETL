# Pipeline ETL temps réel – Données GTFS (Lignes d’Azur)

## Contexte

La métropole de Nice et le réseau urbain **Lignes d’Azur** mettent à disposition en open data leurs données de transport via le standard  **GTFS / GTFS-RT** .

L’objectif est de construire un pipeline de traitement **ETL** permettant :

* de collecter et transformer les données statiques (horaires, arrêts, itinéraires) et dynamiques (positions, mises à jour en temps réel),
* de les charger dans un entrepôt de données (**Snowflake** dans ce projet),
* de préparer des jeux de données exploitables pour des analyses et la création d’indicateurs de suivi (ex. retards moyens, ponctualité, état du service par ligne ou arrêt).

Un travail de visualisation a été réalisé en parallèle pour démontrer les usages possibles (tableaux de bord analytiques).

## Architecture

Le pipeline repose sur trois DAGs Airflow orchestrant l’ingestion et le traitement :

1. **`gtfs_static_daily.py`**
   * Télécharge et décompresse l’archive **GTFS statique** depuis  *data.gouv.fr*
   * Crée les tables *BRONZE* (stops, routes, trips, stop_times)
   * Charge les fichiers extraits dans **Snowflake** via un stage interne
2. **`gtfs_rt_minutely.py`**
   * Récupère les flux **GTFS-RT** (Trip Updates & Vehicle Positions) toutes les 2 minutes
   * Transforme les données protobuf en CSV
   * Alimente les tables *BRONZE* correspondantes dans  **Snowflake**
3. **`gtfs_silver.py`**
   * Agrège et structure les données *BRONZE* dans des tables  *SILVER* toutes les 5 minutes
   * Simplifie l’exploitation pour des KPI et futures visualisations (retards, positions GPS, itinéraires, etc.)

## Entrepôt de données (Snowflake)

* **Niveau BRONZE** : ingestion brute depuis GTFS statique & temps réel
* **Niveau SILVER** : tables normalisées, enrichies et prêtes à l’analyse
* Les scripts créent automatiquement la base `GTFS_DB`, les schémas `BRONZE` et `SILVER`, ainsi que les tables nécessaires

## Pipeline ETL (vue d’ensemble)

Le pipeline suit l’**architecture medallion** :

* **EXTRACT** → téléchargement des données GTFS statiques et temps réel
* **TRANSFORM** → nettoyage, mise en forme et intégration
* **LOAD** → stockage dans  **Snowflake**
* **INTEGRATE** → constitution des couches BRONZE, SILVER et GOLD

### Stack technique

* **Airflow** : orchestration des DAGs ETL
* **Snowflake** : stockage et transformation des données
* **Docker Compose** : déploiement local d’Airflow et de ses dépendances
* **Python** : ingestion, parsing protobuf, transformation CSV

## Arborescence du projet

```
tp-airflow-gtfs-snowflake/
├─ dags/
│   ├─ gtfs_static_daily.py
│   ├─ gtfs_rt_minutely.py
│   └─ gtfs_silver.py
├─ data/         # fichiers bruts téléchargés
├─ exports/      # fichiers transformés (CSV)
├─ logs/         # logs Airflow
├─ config/       # configuration Airflow
├─ docker-compose.yml
├─ requirements.txt
└─ Dockerfile
```

## Installation & Exécution

1. **Cloner le repo et se placer dans le dossier :**

   ```bash
   git clone <repo_url>
   cd tp-airflow-gtfs-snowflake
   ```
2. **Configurer les variables d’environnement (.env) :**

   ```ini
   GTFS_STATIC_URL=https://.../gtfs-<dataset>.zip
   GTFS_RT_TU_URL=https://.../trip_updates.pb
   GTFS_RT_VP_URL=https://.../vehicle_positions.pb
   ```
3. **Lancer Airflow avec Docker Compose :**

   ```bash
   docker-compose up -d
   ```
4. **Accéder à l’interface Airflow :**

   [http://localhost:8080](http://localhost:8080) (user/pass configurés dans `docker-compose.yml`).
5. **Activer les DAGs :**

   * `gtfs_static_daily`
   * `gtfs_rt_minutely`
   * `gtfs_silver_insert`

---

## Analyses & KPI

À partir des données intégrées, il a été possible de s'exercer sur Power Bi pour calculer et visualiser :

* Retards moyens au fil de la journée,
* Taux de ponctualité (% bus ≤ 5 min de retard),
* Carte temps réel des bus avec code couleur selon retard,
* Classement des lignes/arrêts les plus problématiques,
* Heatmap des retards (jour × heure).

---

## Dépendances principales

Voir [`requirements.txt`]() :

* `pandas`
* `requests`
* `gtfs-realtime-bindings`
* `snowflake-connector-python`
* `apache-airflow-providers-snowflake==6.5.2`

---

## Perspectives

* Mettre en place un niveau **GOLD** pour calculer directement les KPI dans Snowflake
* Automatiser le traitement des fichiers via dbt
* Déployer l’architecture sur un environnement cloud (Snowflake + Airflow managed)
