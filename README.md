

\# Lakehouse Inspection Bâtiment



Pipeline de données Lakehouse (Landing → Bronze → Silver → Gold) pour l'exploitation

des données d'inspection d'un bâtiment et la préparation d'un dataset ML, réalisé

dans le cadre d'un stage à CID (École Hassania des Travaux Publics — MIG).



\## Architecture



\- \*\*MinIO\*\* — stockage objet compatible S3

\- \*\*Apache Iceberg\*\* — format de table ouvert (catalogue REST)

\- \*\*Apache Kafka\*\* — simulation d'un flux de capteurs (température/humidité)

\- \*\*Apache Spark\*\* — traitement et transformations Bronze/Silver/Gold

\- \*\*Trino\*\* — requêtage interactif des couches Silver/Gold

\- \*\*Apache Airflow\*\* — orchestration automatisée du pipeline

\- \*\*Metabase\*\* — tableau de bord décisionnel



\## Sources de données



| Source                 | Format      | Description                          |

|-------------------------|-------------|----------------------------------------|

| Missions d'inspection    | JSON        | Missions et anomalies détectées        |

| Capteurs                 | Kafka → CSV | Température/humidité simulées en flux  |

| HBIM                     | GeoJSON     | Zones et géométries du bâtiment        |

| Images                   | JPEG/PNG    | Dataset pour Machine Learning          |



\## Structure du projet



├── airflow/dags/ # DAG d'orchestration du pipeline

├── spark/jobs/ # Scripts Bronze, Silver, Gold

├── sensor-simulator/ # Producteur Kafka (Dockerfile, producer.py)

├── kafka-jars/ # Dépendances Kafka pour Spark

├── notebooks/ # Développement itératif (Jupyter)

├── metabase/ # Configuration du dashboard

├── trino/catalog/ # Configuration du catalogue Trino

├── docker-compose.yml

└── .gitignore





\## Dépendances Kafka/Spark



Le fichier `kafka-jars/aws-java-sdk-bundle-1.12.262.jar` (\~268 Mo, trop volumineux pour GitHub)

n'est pas inclus dans ce repo. Téléchargez-le avant de lancer le projet :



```bash

curl -o kafka-jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

```



\## Lancer le projet



```bash

docker compose up -d

```



\## Pipeline



1\. \*\*Landing\*\* : dépôt brut des sources (dont le flux Kafka pour les capteurs)

2\. \*\*Bronze\*\* : ingestion typée, ajout de métadonnées techniques

3\. \*\*Silver\*\* : nettoyage, validation, normalisation par source

4\. \*\*Gold\*\* : agrégats croisés par zone, KPI, dataset ML labellisé (train/val/test)



\## Résultats



\- Tableau de bord Metabase opérationnel

\- Corpus d'images structuré et tracé, prêt pour l'entraînement ML

\- Pipeline reproductible et orchestré via Airflow



\## Auteur



\*\*Maryame Abouiba\*\*

