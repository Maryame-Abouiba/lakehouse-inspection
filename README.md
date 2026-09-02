
## Prérequis

- Docker Desktop installé et lancé
- Au moins 8 Go de RAM disponibles pour Docker
- Le fichier `kafka-jars/aws-java-sdk-bundle-1.12.262.jar` téléchargé (voir section ci-dessous)

## Dépendances Kafka/Spark

Le fichier `kafka-jars/aws-java-sdk-bundle-1.12.262.jar` (~268 Mo, trop volumineux pour GitHub)
n'est pas inclus dans ce repo. Téléchargez-le avant de lancer le projet :

```bash
curl -o kafka-jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
```

## Lancer le projet

Depuis la racine du projet :

```bash
docker compose up -d
```

Vérifiez que tous les conteneurs sont bien démarrés :

```bash
docker ps
```

Vous devez voir : `minio`, `rest` (catalogue Iceberg), `spark-iceberg`, `trino`, `airflow`, `metabase`, et les conteneurs Kafka/sensor-simulator.

## Accès aux services

Une fois `docker compose up -d` lancé, les interfaces web sont accessibles localement :

| Service              | URL                                  | Identifiants par défaut                  |
|-----------------------|----------------------------------------|---------------------------------------------|
| **MinIO Console**       | http://localhost:9001                | Utilisateur : `adminn` — Mot de passe : `password` |
| **MinIO API (S3)**      | http://localhost:9000                | Même identifiants que ci-dessus              |
| **Jupyter Notebook (Spark)** | http://localhost:8888           | Aucun mot de passe (ou token affiché dans les logs `docker logs spark-iceberg`) |
| **Trino**                | http://localhost:8081               | Aucune authentification requise en local     |
| **Apache Airflow**       | http://localhost:8082               | Utilisateur/mot de passe définis dans `docker-compose.yml` (par défaut `airflow` / `airflow`) |
| **Metabase**             | http://localhost:3000               | Compte à créer au premier lancement (setup guidé) |
| **Catalogue REST Iceberg** | http://localhost:8181             | Utilisé en interne par Spark/Trino, pas d'interface web |

⚠️ Si un port est déjà utilisé sur votre machine, modifiez le mapping correspondant dans `docker-compose.yml` (partie `ports:`).

### Se connecter à MinIO

1. Ouvrez http://localhost:9001 dans votre navigateur
2. Connectez-vous avec `adminn` / `password`
3. Vous devez voir les buckets `landing-zone` et `lakehouse`, organisés en sous-dossiers par source et par niveau (bronze/silver/gold)

### Exécuter le pipeline manuellement (Jupyter)

1. Ouvrez http://localhost:8888
2. Naviguez vers `notebooks/`
3. Ouvrez et exécutez les notebooks dans l'ordre : ingestion Bronze → transformations Silver → agrégations Gold

### Exécuter le pipeline automatiquement (Airflow)

1. Ouvrez http://localhost:8082
2. Connectez-vous
3. Activez le DAG `lakehouse_inspection_pipeline` (toggle en haut à gauche)
4. Déclenchez une exécution manuelle avec le bouton ▶️, ou attendez le déclenchement planifié

### Interroger les données (Trino)

Depuis un client SQL connecté à `http://localhost:8081`, ou en ligne de commande :

```sql
SHOW SCHEMAS FROM demo;
SELECT * FROM demo.gold.zone_summary;
```

### Consulter le dashboard (Metabase)

1. Ouvrez http://localhost:3000
2. Lors du premier lancement, suivez l'assistant de configuration et connectez Metabase à Trino (host: `trino`, port: `8080`, catalogue: `demo`)
3. Le dashboard **« Suivi Inspection Bâtiment »** présente les KPI, anomalies par type/criticité, et température moyenne par zone

### Lancer le simulateur de capteurs (Kafka)

Le service `sensor-simulator` démarre automatiquement avec `docker compose up -d` et publie en continu des mesures simulées sur le topic Kafka dédié. Pour suivre les messages émis :

```bash
docker logs -f sensor-simulator
```

## Pipeline

1. **Landing** : dépôt brut des sources (dont le flux Kafka pour les capteurs)
2. **Bronze** : ingestion typée, ajout de métadonnées techniques
3. **Silver** : nettoyage, validation, normalisation par source
4. **Gold** : agrégats croisés par zone, KPI, dataset ML labellisé (train/val/test)

## Résultats

- Tableau de bord Metabase opérationnel
- Corpus d'images structuré et tracé, prêt pour l'entraînement ML
- Pipeline reproductible et orchestré via Airflow

## Arrêter le projet

```bash
docker compose down
```

Pour supprimer aussi les volumes (données stockées) :

```bash
docker compose down -v
```

## Auteur

**Maryame Abouiba**