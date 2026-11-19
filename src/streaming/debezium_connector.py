import json
import logging
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pg8000.dbapi

from src.settings import EnvSettings

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

PIPELINE_NAME = "cdc_debezium_provisioning"


def verify_cdc_prerequisites(
    conn: pg8000.dbapi.Connection,
    settings: EnvSettings,
) -> dict[str, Any]:
    """Controle la replication logique WAL, la publication cible et le cloisonnement RGPD."""
    cur = conn.cursor()
    try:
        cur.execute("SHOW wal_level;")
        wal_row = cur.fetchone()
        wal_level = str(wal_row[0]) if wal_row else "unknown"
        if wal_level != "logical":
            raise RuntimeError(f"Configuration PostgreSQL non conforme : wal_level={wal_level!r}")

        cur.execute(
            """
            SELECT pt.schemaname, pt.tablename, c.relreplident::text
            FROM pg_publication_tables pt
            JOIN pg_namespace n ON n.nspname = pt.schemaname
            JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
            WHERE pt.pubname = %s;
            """,
            (settings.debezium_publication_name,),
        )
        pub_rows = cur.fetchall()
        matching = [
            row for row in pub_rows if row[0] == "oltp_sports" and row[1] == "sport_activities"
        ]
        if not matching:
            raise RuntimeError(
                f"Table oltp_sports.sport_activities absente de la publication "
                f"{settings.debezium_publication_name!r}"
            )

        replica_identity = str(matching[0][2])
        if replica_identity != "f":
            raise RuntimeError(
                f"REPLICA IDENTITY FULL requis sur oltp_sports.sport_activities "
                f"(actuel={replica_identity!r})"
            )

        role_name = settings.sds_debezium_user
        cur.execute(
            """
            SELECT rolcanlogin, rolreplication, rolsuper
            FROM pg_roles
            WHERE rolname = %s;
            """,
            (role_name,),
        )
        role_row = cur.fetchone()
        if not role_row:
            raise RuntimeError(f"Role PostgreSQL CDC introuvable : {role_name!r}")

        can_login = bool(role_row[0])
        can_replicate = bool(role_row[1])
        is_superuser = bool(role_row[2])
        if not can_login or not can_replicate or is_superuser:
            raise PermissionError(
                f"Attributs du role {role_name!r} invalides : "
                f"login={can_login}, replication={can_replicate}, superuser={is_superuser}"
            )

        cur.execute(
            """
            SELECT
                has_schema_privilege(%s, 'hr_restricted', 'USAGE'),
                has_table_privilege(%s, 'hr_restricted.employees_audit', 'SELECT'),
                has_schema_privilege(%s, 'oltp_sports', 'USAGE'),
                has_table_privilege(%s, 'oltp_sports.sport_activities', 'SELECT');
            """,
            (role_name, role_name, role_name, role_name),
        )
        priv_row = cur.fetchone()
        hr_usage = bool(priv_row[0])
        hr_select = bool(priv_row[1])
        sports_usage = bool(priv_row[2])
        sports_select = bool(priv_row[3])

        if hr_usage or hr_select:
            raise PermissionError(
                f"Violation RGPD : le role {role_name!r} dispose d'un acces a hr_restricted"
            )
        if not sports_usage or not sports_select:
            raise PermissionError(
                f"Droits SELECT manquants pour {role_name!r} sur oltp_sports.sport_activities"
            )

        return {
            "wal_level": wal_level,
            "publication": settings.debezium_publication_name,
            "replica_identity": replica_identity,
            "debezium_role": role_name,
            "hr_restricted_isolated": True,
        }
    finally:
        cur.close()


def load_connector_manifest(manifest_path: Path) -> dict[str, Any]:
    with manifest_path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict) or "config" not in payload:
        raise ValueError(f"Manifeste Debezium invalide : {manifest_path}")
    return payload


def build_debezium_connector_config(settings: EnvSettings) -> dict[str, str]:
    """Assemble la configuration Debezium en ciblant le reseau interne Docker sds-net."""
    base_config: dict[str, str] = {}
    if settings.debezium_manifest_path.exists():
        manifest = load_connector_manifest(settings.debezium_manifest_path)
        raw_cfg = manifest.get("config", {})
        base_config = {str(k): str(v) for k, v in raw_cfg.items()}

    runtime_overrides = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "plugin.name": "pgoutput",
        "database.hostname": settings.debezium_postgres_internal_host,
        "database.port": str(settings.debezium_postgres_internal_port),
        "database.user": settings.sds_debezium_user,
        "database.password": settings.sds_debezium_password.get_secret_value(),
        "database.dbname": settings.postgres_db,
        "topic.prefix": settings.debezium_topic_prefix,
        "schema.include.list": "oltp_sports",
        "table.include.list": "oltp_sports.sport_activities",
        "publication.name": settings.debezium_publication_name,
        "publication.autocreate.mode": "disabled",
        "slot.name": settings.debezium_slot_name,
        "snapshot.mode": base_config.get("snapshot.mode", "initial"),
        "tombstones.on.delete": base_config.get("tombstones.on.delete", "false"),
        "decimal.handling.mode": base_config.get("decimal.handling.mode", "double"),
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "topic.creation.default.replication.factor": "1",
        "topic.creation.default.partitions": "1",
        "topic.creation.default.cleanup.policy": "delete",
    }
    return {**base_config, **runtime_overrides}


def mask_connector_config(config: dict[str, str]) -> dict[str, str]:
    masked = dict(config)
    if "database.password" in masked:
        masked["database.password"] = "***REDACTED***"
    return masked


def _is_status_running(status_payload: dict[str, Any]) -> bool:
    connector_state = status_payload.get("connector", {}).get("state")
    tasks = status_payload.get("tasks", [])
    if connector_state != "RUNNING" or not tasks:
        return False
    return all(task.get("state") == "RUNNING" for task in tasks)


def wait_for_connector_running(
    client: httpx.Client,
    base_url: str,
    connector_name: str,
    *,
    timeout_seconds: float = 30.0,
    poll_interval_seconds: float = 1.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    status_url = f"{base_url}/connectors/{connector_name}/status"
    last_payload: dict[str, Any] = {}

    while time.monotonic() <= deadline:
        resp = client.get(status_url)
        if resp.status_code == 200:
            last_payload = resp.json()
            if _is_status_running(last_payload):
                return last_payload

            failed_tasks = [
                t for t in last_payload.get("tasks", []) if t.get("state") == "FAILED"
            ]
            if failed_tasks:
                trace = failed_tasks[0].get("trace", "trace indisponible")
                raise RuntimeError(
                    f"Tache Debezium en echec pour {connector_name!r} : {trace[:400]}"
                )
        time.sleep(poll_interval_seconds)

    raise TimeoutError(
        f"Delai depasse en attendant l'etat RUNNING pour {connector_name!r} : {last_payload}"
    )


def _record_pipeline_run(
    conn: pg8000.dbapi.Connection,
    *,
    started_at: datetime,
    status: str,
    details: dict[str, Any],
) -> None:
    finished_at = datetime.now(UTC)
    duration_ms = max(0, int((finished_at - started_at).total_seconds() * 1000))
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO ops_monitoring.pipeline_runs (
                run_id, pipeline_name, started_at, finished_at,
                status, rows_processed, anomalies_detected, duration_ms, details
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CAST(%s AS jsonb));
            """,
            (
                str(uuid.uuid4()),
                PIPELINE_NAME,
                started_at,
                finished_at,
                status,
                1,
                0,
                duration_ms,
                json.dumps(details, ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        cur.close()


def deploy_postgres_cdc_connector(
    settings: EnvSettings | None = None,
    *,
    http_client: httpx.Client | None = None,
    pg_conn: pg8000.dbapi.Connection | None = None,
    wait_timeout_seconds: float = 30.0,
    poll_interval_seconds: float = 1.0,
) -> dict[str, Any]:
    cfg = settings or EnvSettings()
    started_at = datetime.now(UTC)
    owns_conn = pg_conn is None
    owns_http = http_client is None

    conn = pg_conn or pg8000.dbapi.connect(**cfg.pg8000_connect_kwargs("pipeline"))
    client = http_client or httpx.Client(timeout=cfg.http_timeout_seconds)
    base_url = cfg.debezium_connect_url.rstrip("/")
    connector_name = cfg.debezium_connector_name

    try:
        prereq_report = verify_cdc_prerequisites(conn, cfg)
        desired_config = build_debezium_connector_config(cfg)

        cfg_resp = client.get(f"{base_url}/connectors/{connector_name}/config")
        status_resp = client.get(f"{base_url}/connectors/{connector_name}/status")

        action = "created"
        if cfg_resp.status_code == 200 and status_resp.status_code == 200:
            existing_cfg = {
                str(k): str(v) for k, v in cfg_resp.json().items() if k != "name"
            }
            current_status = status_resp.json()
            if existing_cfg == desired_config and _is_status_running(current_status):
                action = "unchanged"
                final_status = current_status
            else:
                put_resp = client.put(
                    f"{base_url}/connectors/{connector_name}/config",
                    json=desired_config,
                )
                put_resp.raise_for_status()
                action = "updated"
                client.post(
                    f"{base_url}/connectors/{connector_name}/restart"
                    "?includeTasks=true&onlyFailed=true"
                )
                final_status = wait_for_connector_running(
                    client,
                    base_url,
                    connector_name,
                    timeout_seconds=wait_timeout_seconds,
                    poll_interval_seconds=poll_interval_seconds,
                )
        else:
            put_resp = client.put(
                f"{base_url}/connectors/{connector_name}/config",
                json=desired_config,
            )
            put_resp.raise_for_status()
            action = "created" if put_resp.status_code == 201 else "updated"
            final_status = wait_for_connector_running(
                client,
                base_url,
                connector_name,
                timeout_seconds=wait_timeout_seconds,
                poll_interval_seconds=poll_interval_seconds,
            )

        summary = {
            "connector_name": connector_name,
            "action": action,
            "target_topic": cfg.redpanda_activity_topic,
            "connector_state": final_status.get("connector", {}).get("state"),
            "tasks_count": len(final_status.get("tasks", [])),
            "prerequisites": prereq_report,
            "config_masked": mask_connector_config(desired_config),
        }
        _record_pipeline_run(conn, started_at=started_at, status="SUCCESS", details=summary)
        return summary
    except Exception as exc:
        failure_details = {
            "connector_name": connector_name,
            "error_type": type(exc).__name__,
            "error_message": str(exc)[:500],
        }
        try:
            _record_pipeline_run(
                conn, started_at=started_at, status="FAILED", details=failure_details
            )
        except Exception:
            logger.warning("Echec de journalisation de l'erreur CDC dans pipeline_runs")
        raise
    finally:
        if owns_http:
            client.close()
        if owns_conn:
            conn.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    summary = deploy_postgres_cdc_connector()
    output = {k: v for k, v in summary.items() if k != "config_masked"}
    logger.info("Connecteur Debezium operationnel : %s", json.dumps(output, ensure_ascii=False))


if __name__ == "__main__":
    main()