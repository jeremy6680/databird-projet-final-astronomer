# Projet Final Astronomer — Orchestration dbt + Airflow

Pipeline de données orchestrée avec Apache Airflow (via **Astro CLI**) et **Astronomer Cosmos**, qui exécute un projet **dbt** sur **BigQuery**.

---

## Contexte

Ce repo est une **variante de [`jeremy6680/databird-projet-final`](https://github.com/jeremy6680/databird-projet-final)**, dans laquelle j'ai fait le choix d'utiliser **Astro CLI** et **Astronomer Cosmos** pour orchestrer la pipeline.

- **Le projet dbt** (`dbt/local_bike/`) est issu de la correction du projet final du bootcamp Analytics Engineer de [Databird](https://www.databird.co/), disponible ici : [`Carolinemestre/correction_projet_final`](https://github.com/Carolinemestre/correction_projet_final).
- **L'objectif principal** est la création et l'orchestration de la pipeline Airflow, avec la mise en place de l'environnement Docker. Le choix de passer par Astro CLI (pour l'environnement Docker clé en main) et Cosmos (pour l'intégration dbt native dans Airflow) est une décision délibérée, en complément des notifications d'échec Slack et email.

---

## Stack technique

| Outil | Rôle |
|---|---|
| [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview) | Génération et gestion de l'environnement Airflow local (Docker) |
| [Apache Airflow](https://airflow.apache.org/) | Orchestrateur de la pipeline |
| [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/) | Intégration dbt → Airflow (conversion automatique des modèles en tâches) |
| [dbt](https://www.getdbt.com/) | Transformation des données (staging → intermediate → mart) |
| [Google BigQuery](https://cloud.google.com/bigquery) | Data warehouse cible |
| Slack & Email | Notifications d'échec du DAG |

---

## Architecture du projet

```
projet_final_astronomer/
├── dags/
│   └── local_bike_dbt_dag.py   # DAG principal orchestrant les 3 couches dbt
├── dbt/
│   └── local_bike/
│       └── models/
│           ├── staging/         # Vues de nettoyage des sources brutes
│           ├── intermediate/    # Modèles de jointure et transformation
│           └── mart/            # Tables finales exposées aux analyses
├── .dbt_profiles/
│   ├── profiles.yml             # Connexion BigQuery (service account)
│   └── bigqueryKey.json         # Clé de service GCP (à ne pas commiter !)
├── Dockerfile                   # Image Astro Runtime 3.2-5
├── requirements.txt             # astronomer-cosmos, dbt-bigquery, slack provider
└── airflow_settings.yaml        # Connexions/Variables Airflow locales
```

### Flux du DAG `local_bike_dbt`

```
start → [staging] → [intermediate] → [mart] → end
```

Chaque groupe (`DbtTaskGroup`) est généré automatiquement par Cosmos à partir de la sélection de modèles dbt correspondante. Le DAG tourne quotidiennement (`@daily`) avec 2 tentatives en cas d'échec.

---

## Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installé et démarré
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installé
- Un compte Google Cloud avec un projet BigQuery et une clé de service JSON
- (Optionnel) Un webhook Slack configuré pour les notifications

---

## Installation et lancement

### 1. Cloner le repo

```bash
git clone <url-du-repo>
cd projet_final_astronomer
```

### 2. Configurer les credentials BigQuery

Placer la clé de service GCP dans `.dbt_profiles/` :

```bash
cp /chemin/vers/ma-cle-gcp.json .dbt_profiles/bigqueryKey.json
```

Vérifier/adapter le fichier `.dbt_profiles/profiles.yml` :

```yaml
default:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: <votre-projet-gcp>
      dataset: local_bike_final_project_astronomer
      keyfile: /usr/local/airflow/.dbt_profiles/bigqueryKey.json
```

### 3. Démarrer l'environnement Airflow (Docker)

```bash
astro dev start
```

Cette commande lance 5 conteneurs Docker :

| Conteneur | Rôle |
|---|---|
| Postgres | Base de métadonnées Airflow |
| Scheduler | Surveille et déclenche les tâches |
| DAG Processor | Parse les fichiers de DAGs |
| API Server | Sert l'UI Airflow et l'API |
| Triggerer | Gère les tâches différées |

L'UI Airflow est accessible à : **http://localhost:8080**
- Login : `admin`
- Mot de passe : `admin`

### 4. Configurer les connexions Airflow

Dans l'UI Airflow (`Admin > Connections`) :

**Connexion Slack** (pour les notifications) :
- Conn ID : `slack_webhook`
- Conn Type : `Slack Incoming Webhook`
- Password : URL de votre webhook Slack

**Connexion email SMTP** (pour les notifications email) :
- Configurer via les variables d'environnement dans `.env` ou dans `airflow_settings.yaml`

### 5. Activer et déclencher le DAG

Dans l'UI Airflow, activer le DAG `local_bike_dbt` et le déclencher manuellement, ou attendre le prochain run quotidien.

---

## Commandes Astro CLI utiles

```bash
# Démarrer l'environnement
astro dev start

# Arrêter l'environnement
astro dev stop

# Redémarrer (après modification du Dockerfile ou requirements.txt)
astro dev restart

# Voir les logs en temps réel
astro dev logs --follow

# Ouvrir un shell dans le conteneur scheduler
astro dev bash

# Vérifier l'intégrité des DAGs
astro dev parse

# Lancer les tests
astro dev pytest
```

---

## Commandes dbt utiles (depuis le conteneur)

```bash
# Ouvrir un shell dans le conteneur
astro dev bash

# Installer les dépendances dbt
dbt deps --project-dir /usr/local/airflow/dbt/local_bike --profiles-dir /usr/local/airflow/.dbt_profiles

# Tester la connexion BigQuery
dbt debug --project-dir /usr/local/airflow/dbt/local_bike --profiles-dir /usr/local/airflow/.dbt_profiles

# Lancer tous les modèles
dbt run --project-dir /usr/local/airflow/dbt/local_bike --profiles-dir /usr/local/airflow/.dbt_profiles

# Lancer uniquement le staging
dbt run --select path:models/staging --project-dir /usr/local/airflow/dbt/local_bike --profiles-dir /usr/local/airflow/.dbt_profiles

# Lancer les tests
dbt test --project-dir /usr/local/airflow/dbt/local_bike --profiles-dir /usr/local/airflow/.dbt_profiles
```

---

## Notifications d'échec

En cas d'échec d'une tâche, deux notifications sont envoyées automatiquement :

- **Slack** : message dans le canal configuré avec le nom du DAG, de la tâche et un lien vers les logs
- **Email** : mail HTML envoyé avec les mêmes informations

---

## Fichiers sensibles

Les fichiers suivants **ne doivent pas être commités** (ajoutés au `.gitignore`) :

```
.dbt_profiles/bigqueryKey.json
.dbt_profiles/profiles.yml
.env
```

---

## Crédits

- Projet dbt basé sur la correction du bootcamp Analytics Engineer [Databird](https://www.databird.co/) : [Carolinemestre/correction_projet_final](https://github.com/Carolinemestre/correction_projet_final)
- Environnement Airflow généré avec [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview)
- Intégration dbt-Airflow via [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/)
