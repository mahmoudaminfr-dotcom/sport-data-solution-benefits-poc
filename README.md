# Sport Data Solution — Plateforme Data Avantages Sportifs

Plateforme d'ingénierie de données temps réel et analytique destinée à qualifier l'éligibilité aux avantages sportifs des collaborateurs de **Sport Data Solution** (siège social : 1362 Avenue des Platanes, 34970 Lattes), automatiser les notifications Slack et piloter l'impact financier sous Power BI.

## 1. Architecture Technique

- **Base transactionnelle et analytique (`PostgreSQL 16`) :** réplication logique (`wal_level = logical`) et cloisonnement en quatre schémas (`hr_restricted`, `oltp_sports`, `gold_mart`, `ops_monitoring`).
- **Streaming CDC (`Debezium 2.7` + `Redpanda`) :** capture différentielle en temps réel (`pgoutput`) des nouvelles activités vers le topic `sds.oltp_sports.sport_activities` depuis la publication `sds_activities_pub` (`REPLICA IDENTITY FULL`).
- **Notifications métier (`Python` -> `Slack`) :** consommateur `confluent-kafka` publiant instantanément les séances `live_event` avec coupe-circuit anti-spam sur l'historique (`op = 'r'` et `historical_seed`) et repli journalisé (`dry_run`) lorsque `SLACK_WEBHOOK_URL` conserve sa valeur modèle.
- **Lakehouse (`MinIO S3` + `Delta Lake`) :** structuration Médaillon (`Bronze`, `Silver`, `Gold`) propulsée par `Polars`, `DuckDB` et `deltalake` permettant un recalcul historique complet en moins d'une seconde.
- **Validation externe (`API Google Maps`) :** contrôle automatisé des distances domicile-travail déclarées avec cache local pseudonymisé (`SHA-256` salé) et repli routier `BAN + OSRM` (plafonds paramétrables dans `config/business_rules.yaml`).
- **Qualité et supervision (`Soda Core`, `Pytest`, `Kestra`) :** tests automatisés de cohérence, orchestration quotidienne et suivi des exécutions dans `ops_monitoring.pipeline_runs`.

Les logigrammes d'architecture figurent dans `docs/diagrams/` (`architecture_executive.drawio` et `workflow_technique_detaille.drawio`).

## 2. Sécurité et Conformité RGPD

1. Exclusion stricte des fichiers RH nominatifs (`*.xlsx`), caches d'adresses et secrets `.env` via `.gitignore`.
2. Séparation des privilèges SQL entre trois rôles applicatifs (`sds_pipeline`, `sds_debezium`, `sds_bi_reader`) via `pg8000`.
3. Isolation stricte du connecteur CDC (`sds_debezium`) sans aucun droit sur `hr_restricted`, et révocation totale des droits du rôle décisionnel `sds_bi_reader` sur `hr_restricted` et `oltp_sports`.

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

Interfaces locales :
- PostgreSQL 16 (`sds-postgres`) : `127.0.0.1:5433` (réseau interne Docker : `sds-postgres:5432`)
- Console MinIO S3 : `http://localhost:9001`
- Redpanda Console : `http://localhost:8080`
- API Debezium Connect : `http://localhost:8083/connectors`