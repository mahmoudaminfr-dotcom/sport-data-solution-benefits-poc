# Sport Data Solution — Plateforme Data Avantages Sportifs

Plateforme d'ingénierie de données temps réel et analytique destinée à qualifier l'éligibilité aux avantages sportifs des collaborateurs de **Sport Data Solution** (siège social : 1362 Avenue des Platanes, 34970 Lattes), automatiser les notifications Slack et piloter l'impact financier sous Power BI.

## 1. Architecture Technique

- **Base transactionnelle et analytique (`PostgreSQL 16`) :** réplication logique (`wal_level = logical`) et cloisonnement en quatre schémas (`hr_restricted`, `oltp_sports`, `gold_mart`, `ops_monitoring`).
- **Streaming CDC (`Debezium 2.7` + `Redpanda`) :** capture différentielle en temps réel (`pgoutput`) des nouvelles activités vers le topic `sds.oltp_sports.sport_activities` depuis la publication `sds_activities_pub` (`REPLICA IDENTITY FULL`).
- **Notifications métier (`Python` vers `Slack`) :** consommateur `confluent-kafka` publiant instantanément les séances `live_event` avec coupe-circuit anti-spam sur l'historique (`op = 'r'` et `historical_seed`) et repli journalisé (`dry_run`) lorsque `SLACK_WEBHOOK_URL` conserve sa valeur modèle.
- **Lakehouse (`MinIO S3` + `Delta Lake`) :** structuration Médaillon (`Bronze`, `Silver`, `Gold`) propulsée par `Polars`, `DuckDB` et `deltalake` sans JVM, permettant un recalcul historique complet des KPI en moins de 150 millisecondes côté moteur.
- **Validation externe (`API Google Maps`) :** contrôle automatisé des distances domicile-travail déclarées avec cache local pseudonymisé (`SHA-256` salé) et repli routier `BAN + OSRM` (plafonds paramétrables dans `config/business_rules.yaml`).
- **Qualité et supervision (`Soda Core`, `Pytest`, `Kestra`) :** 22 contrats SodaCL multi-couches (`config/soda/checks_pipeline.yml`), orchestration quotidienne et suivi des exécutions dans `ops_monitoring.pipeline_runs` et `ops_monitoring.quality_check_results`.

Les logigrammes d'architecture figurent dans `docs/diagrams/` (`architecture_executive.drawio` et `workflow_technique_detaille.drawio`).

## 2. Sécurité et Conformité RGPD

1. Exclusion stricte des fichiers RH nominatifs (`*.xlsx`), caches d'adresses et secrets `.env` via `.gitignore`.
2. Pseudonymisation déterministe salée (`employee_key` via `SDS_PII_HASH_SALT`) dès l'extraction vers la couche `Bronze` : aucun nom, prénom, identifiant RH brut, commentaire libre ni adresse postale ne transite dans le Lakehouse S3 ou dans `gold_mart`.
3. Séparation des privilèges SQL entre trois rôles applicatifs (`sds_pipeline`, `sds_debezium`, `sds_bi_reader`) via `pg8000` : isolation stricte de `sds_debezium` hors `hr_restricted`, et accès exclusif de `sds_bi_reader` aux vues et tables décisionnelles de `gold_mart` et `ops_monitoring`.

## 3. Démarrage Rapide et Ingestion Initiale

```powershell
uv sync --python 3.12 --extra dev
uv run python -m ruff check .
uv run python -m pytest -v
docker compose --env-file .env -f docker/docker-compose.yml up -d
docker compose --env-file .env -f docker/docker-compose.yml ps
uv run python -m src.ingestion.minio_loader
uv run python -m src.ingestion.postgres_generator
```

## 4. Pipeline Streaming CDC Temps Réel et Notifications Slack

```powershell
# Déploiement idempotent du connecteur Debezium PostgreSQL (pgoutput)
uv run python -m src.streaming.debezium_connector

# Démarrage du consommateur CDC vers Slack (mode continu ou arrêt après inactivité)
uv run python -m src.streaming.cdc_consumer --max-idle-seconds 5.0

# Injection d'une activité unitaire temps réel (live_event) ou d'un lot
uv run python scripts/inject_live_activity.py --sport "Course à pied" --duration-minutes 45 --distance-km 8.5 --comment "Sortie collective"
uv run python scripts/inject_live_activity.py --count 3
```

## 5. Lakehouse Médaillon (Bronze / Silver / Gold) et Simulation Budgétaire

```powershell
# Synchronisation des couches Delta Lake Bronze et Silver depuis PostgreSQL
uv run python -m src.processing.lakehouse_bronze_silver

# Calcul nominal de la couche Gold (Delta Lake sds-gold + PostgreSQL gold_mart)
uv run python scripts/recompute_gold_kpis.py

# Simulation budgétaire dynamique (ex. prime à 4 % et seuil à 20 activités annuelles)
uv run python scripts/recompute_gold_kpis.py --prime-rate 0.04 --min-activities 20

# Restauration des paramètres métier de référence (config/business_rules.yaml)
uv run python scripts/recompute_gold_kpis.py --reset-defaults
```

## 6. Contrôles de Qualité Automatisés (Soda Core)

```powershell
# Exécution bloquante des 22 contrats SodaCL (hr_audit, bronze, silver, gold)
uv run python -m src.quality.soda_runner

# Exécution en mode audit non bloquant (journalisation dans ops_monitoring.quality_check_results)
uv run python -m src.quality.soda_runner --no-fail-exit
```

Interfaces locales :
- PostgreSQL 16 (`sds-postgres`) : `127.0.0.1:5433` (réseau interne Docker : `sds-postgres:5432`)
- Console MinIO S3 : `http://localhost:9001`
- Redpanda Console : `http://localhost:8080`
- API Debezium Connect : `http://localhost:8083/connectors`