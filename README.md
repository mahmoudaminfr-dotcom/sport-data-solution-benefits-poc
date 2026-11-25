# Sport Data Solution — Plateforme Data Avantages Sportifs

Plateforme d'ingénierie de données événementielle et analytique destinée à qualifier l'éligibilité aux avantages sportifs et à la prime de mobilité durable des collaborateurs de **Sport Data Solution** (siège social : 1362 Avenue des Platanes, 34970 Lattes), automatiser les notifications Slack et piloter l'impact budgétaire sous Power BI.

## 1. Architecture Technique et Cadence des Flux

### Composants de la plateforme
- **Base transactionnelle et Data Mart (`PostgreSQL 16`) :** réplication logique (`wal_level = logical`) et cloisonnement en quatre schémas (`hr_restricted`, `oltp_sports`, `gold_mart`, `ops_monitoring`).
- **Streaming CDC (`Debezium 2.7` + `Redpanda`) :** capture différentielle en temps réel (`pgoutput`) des insertions sur `oltp_sports.sport_activities` vers le topic `sds.oltp_sports.sport_activities` depuis la publication `sds_activities_pub` (`REPLICA IDENTITY FULL`).
- **Notifications métier (`Python` vers `Slack`) :** consommateur `confluent-kafka` publiant instantanément les séances `live_event` avec coupe-circuit anti-spam sur l'historique (`op = 'r'` et `historical_seed`) et repli journalisé (`dry_run`) lorsque `SLACK_WEBHOOK_URL` conserve sa valeur modèle.
- **Lakehouse (`MinIO S3` + `Delta Lake`) :** architecture Médaillon (`Bronze`, `Silver`, `Gold`) propulsée par `Polars`, `DuckDB` et `deltalake` sans JVM, synchronisant les tables agrégées vers `gold_mart`.
- **Validation externe (`API Google Maps`) :** contrôle automatisé des distances domicile-travail déclarées avec cache local pseudonymisé (`SHA-256` salé) et repli routier automatique `BAN + OSRM` lorsqu'aucune clé Google Maps n'est renseignée (plafonds paramétrables dans `config/business_rules.yaml`).
- **Orchestration, qualité et supervision (`Kestra`, `Soda Core`, `Pytest`) :** workflow quotidien `flows/daily_benefits_pipeline.yml`, 22 contrats SodaCL multi-couches (`config/soda/checks_pipeline.yml`) et vues de supervision `ops_monitoring`.

Les logigrammes d'architecture figurent dans `docs/diagrams/` (`architecture_executive.drawio` et `workflow_technique_detaille.drawio`).

### Distinction entre flux en direct et actualisation analytique

| Chaîne de traitement | Déclenchement nominal | Latence | Comportement lors d'une démonstration |
|---|---|---|---|
| **1. Flux événementiel Slack** (`PostgreSQL` -> `Debezium` -> `Redpanda` -> `cdc_consumer`) | Temps réel continu au fil de l'eau (capture du journal WAL) | `< 1 seconde` | Toute activité insérée dans `oltp_sports.sport_activities` déclenche immédiatement la notification Slack. |
| **2. Recalcul Lakehouse & `gold_mart`** (`Bronze` -> `Silver` -> `Gold` -> `PostgreSQL`) | Planifié tous les jours à **06h00** (`Europe/Paris`) dans Kestra | Batch quotidien (`< 2 s` d'exécution) | Pour refléter immédiatement de nouvelles activités sans attendre 06h00, exécuter `scripts/recompute_gold_kpis.py --refresh-bronze-silver`. |
| **3. Tableau de bord `Power BI`** (`bi/sport_data_solution_avantages.pbix`) | Connexion PostgreSQL en **mode Import** | À l'actualisation du modèle | Après le recalcul Lakehouse, cliquer sur **Actualiser** dans Power BI Desktop pour recharger `gold_mart` et les vues de supervision. |

## 2. Sécurité et Conformité RGPD

1. **Non-exposition des données nominatives et secrets :** `.env`, les fichiers Excel RH réels (`data/raw/*.xlsx`) et le cache d'itinéraires (`data/cache/*`) sont exclus du versionnement Git via `.gitignore`. Seul un jeu d'essai fictif de 10 collaborateurs (`data/samples/*.xlsx`) est versionné pour permettre le test complet depuis un clone vierge.
2. **Pseudonymisation et hygiène des journaux :** hachage déterministe salé (`employee_key` via `SDS_PII_HASH_SALT`) dès l'extraction `Bronze`. Aucun nom, prénom, adresse postale, commentaire libre ni identifiant RH brut (`employee_id`) n'est stocké dans le Lakehouse S3, dans `gold_mart` ou dans le journal technique `ops_monitoring.pipeline_runs`.
3. **Moindre privilège SQL (RBAC) :** séparation stricte entre `sds_pipeline`, `sds_debezium` (limité à `oltp_sports.sport_activities`) et `sds_bi_reader`. Le rôle Power BI `sds_bi_reader` dispose d'un droit `SELECT` limité au seul schéma `gold_mart` et aux deux vues agrégées requises par le rapport (`ops_monitoring.v_pipeline_health_summary` et `ops_monitoring.v_quality_layer_summary`), sans accès aux tables techniques brutes.

## 3. Installation et Initialisation depuis un Clone Propre

### 3.1 Créer le fichier `.env` à partir du modèle

```powershell
Copy-Item .env.example .env
```

Ouvrir `.env` et personnaliser les mots de passe locaux (`POSTGRES_ADMIN_PASSWORD`, `SDS_PIPELINE_PASSWORD`, `SDS_DEBEZIUM_PASSWORD`, `SDS_BI_READER_PASSWORD`, `MINIO_ROOT_PASSWORD`, `SDS_PII_HASH_SALT`, `KESTRA_BASIC_AUTH_PASSWORD`) :
- **Clé Google Maps optionnelle :** si `GOOGLE_MAPS_API_KEY` conserve sa valeur modèle, le pipeline bascule automatiquement sur le géocodage gratuit `BAN + OSRM`.
- **Webhook Slack optionnel :** si `SLACK_WEBHOOK_URL` conserve sa valeur modèle, le consommateur CDC fonctionne sans erreur en mode `dry_run` (messages formatés visibles dans le terminal). Pour publier dans un canal Slack réel, renseigner l'URL du Webhook entrant.

### 3.2 Placer les deux fichiers Excel attendus dans `data/raw/`

Le pipeline d'ingestion attend deux classeurs Excel dans le dossier `data/raw/` :
- `data/raw/Données+RH.xlsx` (colonnes : `ID salarié`, `Nom`, `Prénom`, `Date de naissance`, `BU`, `Date d'embauche`, `Salaire brut`, `Type de contrat`, `Nombre de jours de CP`, `Adresse du domicile`, `Moyen de déplacement`)
- `data/raw/Données+Sportive.xlsx` (colonnes : `ID salarié`, `Pratique d'un sport`)

Depuis un clone propre (sans données RH réelles), copier le jeu fictif de 10 collaborateurs fourni dans `data/samples/` :

```powershell
Copy-Item "data\samples\*.xlsx" "data\raw\"
```

### 3.3 Démarrer l'infrastructure et initialiser le socle de données

```powershell
uv sync --python 3.12 --extra dev
uv run python -m ruff check .
uv run python -m pytest -v

docker compose --env-file .env -f docker/docker-compose.yml --profile orchestration up -d
docker compose --env-file .env -f docker/docker-compose.yml ps

# Chargement S3 MinIO, audit RH / géocodage et génération de l'historique sportif 12 mois
uv run python -m src.ingestion.minio_loader
uv run python -m src.ingestion.postgres_generator

# Construction initiale Bronze / Silver / Gold, contrôles Soda Core et vues de supervision
uv run python scripts/recompute_gold_kpis.py --refresh-bronze-silver
uv run python -m src.quality.soda_runner
uv run python -m src.monitoring.pipeline_monitor --record-run

# Déploiement idempotent du connecteur CDC Debezium
uv run python -m src.streaming.debezium_connector
```

## 4. Scénario de Démonstration Bout en Bout (+3 Activités)

Ce scénario illustre la séparation entre la notification temps réel Slack et l'actualisation analytique du Lakehouse et de Power BI :

1. **État initial :** ouvrir `bi/sport_data_solution_avantages.pbix` dans Power BI Desktop et relever le compteur d'activités sur l'onglet `Sport et Mobilité` (`1 723` activités sur le jeu de référence).
2. **Terminal 1 — Consommateur CDC Slack en écoute continue** (reste actif jusqu'à `Ctrl+C`) :

```powershell
uv run python -m src.streaming.cdc_consumer
```

3. **Terminal 2 — Injection directe de 3 activités sportives** dans `oltp_sports.sport_activities` :

```powershell
uv run python scripts/inject_live_activity.py --count 3
```

Les **3 notifications arrivent en moins d'une seconde dans Slack** (ou s'affichent dans le Terminal 1 en mode `dry_run`), tandis que Power BI affiche toujours la valeur précédente tant que le recalcul analytique et le rafraîchissement Import n'ont pas été lancés.

4. **Recalcul analytique Lakehouse et actualisation Power BI :**

```powershell
uv run python scripts/recompute_gold_kpis.py --refresh-bronze-silver
```

Cliquer ensuite sur **Actualiser** dans le ruban **Accueil** de Power BI Desktop : le compteur passe immédiatement à **`+3` activités** (`1 726` sur le jeu de référence).

5. **Remise à zéro post-démonstration** (purge uniquement les événements `live_event` pour retrouver l'état initial exact) :

```powershell
uv run python scripts/inject_live_activity.py --reset-live
uv run python scripts/recompute_gold_kpis.py --refresh-bronze-silver
```

*(Note : pour une injection unitaire personnalisée, utiliser `--sport "Course à pied" --duration-minutes 45 --distance-km 8.5`. Pour un vidage automatique non interactif du consommateur CDC en script de recette, ajouter `--max-idle-seconds 5.0`.)*

## 5. Commandes de Simulation Budgétaire, Qualité et Orchestration

### Simulation budgétaire dynamique (couche Gold)

```powershell
# Simulation par ligne de commande (ex. prime à 4 % et seuil à 20 activités annuelles)
uv run python scripts/recompute_gold_kpis.py --prime-rate 0.04 --min-activities 20

# Restauration des paramètres nominaux de config/business_rules.yaml
uv run python scripts/recompute_gold_kpis.py --reset-defaults
```

### Portail de qualité des données (22 contrats Soda Core)

```powershell
# Exécution bloquante sur les 4 couches (hr_audit, bronze, silver, gold)
uv run python -m src.quality.soda_runner

# Mode audit non bloquant
uv run python -m src.quality.soda_runner --no-fail-exit
```

### Orchestration quotidienne Kestra (planifiée à 06h00) et Supervision

```powershell
# Validation locale, déploiement idempotent et déclenchement du flow sds.benefits/daily_benefits_pipeline
uv run python -m src.monitoring.kestra_orchestrator --validate-only
uv run python -m src.monitoring.kestra_orchestrator --deploy --trigger

# Rapport de santé consolidé
uv run python -m src.monitoring.pipeline_monitor --record-run
```

Interfaces locales :
- PostgreSQL 16 (`sds-postgres`) : `127.0.0.1:5433` (réseau interne Docker : `sds-postgres:5432`)
- Console MinIO S3 : `http://localhost:9001`
- Redpanda Console : `http://localhost:8080`
- API Debezium Connect : `http://localhost:8083/connectors`
- Orchestrateur Kestra : `http://localhost:8085`

## 6. Livrables Analytiques

- **Notebook d'audit :** `notebooks/sport_data_solution_audit_analytique.ipynb` (audit du cloisonnement SQL RBAC, du flux CDC, du versionnage Delta Lake, des 22 tests Soda Core et de la sensibilité budgétaire).
- **Rapport Power BI :** `bi/sport_data_solution_avantages.pbix` connecté en lecture seule via `sds_bi_reader`, documenté dans `bi/README_powerbi.md` et `bi/mesures_dax.dax`.
