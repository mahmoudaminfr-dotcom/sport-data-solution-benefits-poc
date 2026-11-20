import argparse
import contextlib
import json
import logging
import os
import sys
import tempfile
import types
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pg8000.dbapi
import polars as pl
import yaml

from src.processing.lakehouse_bronze_silver import (
    pseudonymize_employee_id,
    read_delta_table,
)
from src.settings import EnvSettings

logger = logging.getLogger(__name__)

PIPELINE_NAME = "soda_data_quality_gate"
_DEFAULT_STORAGE = object()


class DataQualityGateError(RuntimeError):
    """Exception levee lorsqu'au moins un controle qualite bloquant echoue."""


@dataclass(frozen=True)
class QualityCheckOutcome:
    layer_name: str
    table_name: str
    check_name: str
    outcome: str
    failed_rows_count: int


def _ensure_distutils_strtobool() -> None:
    """Garantit la presence de distutils.util.strtobool pour Soda Core sous Python 3.12+."""
    try:
        from distutils.util import strtobool  # noqa: F401

        return
    except ModuleNotFoundError:
        pass

    def _strtobool(val: str) -> int:
        lowered = str(val).strip().lower()
        if lowered in ("y", "yes", "t", "true", "on", "1"):
            return 1
        if lowered in ("n", "no", "f", "false", "off", "0"):
            return 0
        raise ValueError(f"Valeur booleenne invalide : {val!r}")

    distutils_mod = sys.modules.get("distutils") or types.ModuleType("distutils")
    util_mod = types.ModuleType("distutils.util")
    util_mod.strtobool = _strtobool  # type: ignore[attr-defined]
    distutils_mod.util = util_mod  # type: ignore[attr-defined]
    sys.modules["distutils"] = distutils_mod
    sys.modules["distutils.util"] = util_mod


@contextlib.contextmanager
def _isolated_soda_environ() -> Iterator[None]:
    """Isole os.environ contre l'appel implicite load_dotenv() de Soda Core."""
    env_snapshot = os.environ.copy()
    try:
        os.environ.setdefault("OTEL_SDK_DISABLED", "true")
        yield
    finally:
        os.environ.clear()
        os.environ.update(env_snapshot)


def _create_configured_soda_scan(
    data_source_name: str,
    scan_definition_name: str,
) -> Any:
    """Instancie Soda Scan sans polluer os.environ et sans telemetrie HTTP."""
    _ensure_distutils_strtobool()
    with _isolated_soda_environ():
        from soda.common.config_helper import ConfigHelper

        cfg_helper = ConfigHelper.get_instance()
        cfg_helper.upsert_value("send_anonymous_usage_stats", False)

        from soda.scan import Scan

        scan = Scan()
        scan.set_scan_definition_name(scan_definition_name)
        scan.set_data_source_name(data_source_name)
        return scan


def _prepare_df_for_duckdb(df: pl.DataFrame) -> pl.DataFrame:
    """Convertit les horodatages tz-aware en UTC naif afin d'eviter l'import pytz de DuckDB."""
    tz_exprs: list[pl.Expr] = []
    for col_name, dtype in df.schema.items():
        if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None:
            tz_exprs.append(
                pl.col(col_name)
                .dt.convert_time_zone("UTC")
                .dt.replace_time_zone(None)
                .alias(col_name)
            )
    return df.with_columns(tz_exprs) if tz_exprs else df


def _materialize_duckdb_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    df: pl.DataFrame,
    tmp_dir: Path,
) -> None:
    prepared = _prepare_df_for_duckdb(df)
    staging_view = f"_stg_{table_name}"
    try:
        import arro3.core

        arrow_tbl = arro3.core.Table.from_arrow(prepared)
        con.register(staging_view, arrow_tbl)
        con.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {staging_view}"
        ).fetchall()
        con.unregister(staging_view)
    except Exception:
        parquet_path = tmp_dir / f"{table_name}.parquet"
        prepared.write_parquet(parquet_path)
        escaped = str(parquet_path).replace("'", "''")
        con.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{escaped}')"
        ).fetchall()


def extract_hr_audit_for_quality(
    conn: pg8000.dbapi.Connection,
    salt: str,
) -> pl.DataFrame:
    """Extrait uniquement les colonnes d'audit RH non nominatives pour validation Soda."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                employee_id,
                business_unit,
                annual_gross_salary_eur,
                declared_commute_mode,
                computed_distance_km,
                max_allowed_distance_km,
                is_commute_anomaly,
                is_mobility_bonus_eligible
            FROM hr_restricted.employees_audit
            ORDER BY employee_id ASC;
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    schema = {
        "employee_key": pl.String,
        "business_unit": pl.String,
        "annual_gross_salary_eur": pl.Float64,
        "declared_commute_mode": pl.String,
        "computed_distance_km": pl.Float64,
        "max_allowed_distance_km": pl.Float64,
        "is_commute_anomaly": pl.Boolean,
        "is_mobility_bonus_eligible": pl.Boolean,
    }
    records = [
        {
            "employee_key": pseudonymize_employee_id(int(r[0]), salt),
            "business_unit": str(r[1]),
            "annual_gross_salary_eur": float(r[2]),
            "declared_commute_mode": str(r[3]),
            "computed_distance_km": float(r[4]) if r[4] is not None else None,
            "max_allowed_distance_km": float(r[5]) if r[5] is not None else None,
            "is_commute_anomaly": bool(r[6]),
            "is_mobility_bonus_eligible": bool(r[7]),
        }
        for r in rows
    ]
    return pl.DataFrame(records, schema=schema)


def _infer_layer_name(table_name: str) -> str:
    lowered = table_name.strip().lower()
    if lowered.startswith("hr_"):
        return "hr_audit"
    if lowered.startswith("bronze_"):
        return "bronze"
    if lowered.startswith("silver_"):
        return "silver"
    if lowered.startswith("gold_"):
        return "gold"
    return "lakehouse"


def _extract_failed_count(outcome: str, diagnostics: dict[str, Any]) -> int:
    if outcome == "PASS":
        return 0
    raw_val = diagnostics.get("value")
    if isinstance(raw_val, int | float):
        numeric = int(abs(round(float(raw_val))))
        return max(1, numeric)
    return 1


def evaluate_soda_checks_on_tables(
    tables: dict[str, pl.DataFrame],
    *,
    checks_path: Path,
    config_path: Path | None = None,
) -> list[QualityCheckOutcome]:
    """Execute les contrats SodaCL sur un dictionnaire de DataFrames Polars via DuckDB."""
    config_data: dict[str, Any] = {}
    if config_path is not None and config_path.exists():
        loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            config_data = loaded

    data_source_name = str(config_data.get("data_source_name", "sds_lakehouse"))
    scan_def_name = str(
        config_data.get("scan_definition_name", "sds_medallion_quality_gate")
    )
    sodacl_str = checks_path.read_text(encoding="utf-8")

    with tempfile.TemporaryDirectory() as tmp_str:
        tmp_dir = Path(tmp_str)
        con = duckdb.connect(":memory:")
        try:
            for tbl_name, df in tables.items():
                _materialize_duckdb_table(con, tbl_name, df, tmp_dir)

            with _isolated_soda_environ():
                scan = _create_configured_soda_scan(data_source_name, scan_def_name)
                scan.add_duckdb_connection(con, data_source_name=data_source_name)
                scan.add_sodacl_yaml_str(sodacl_str)
                exit_code = scan.execute()
                scan_results = scan.get_scan_results()

                if scan.has_error_logs() or exit_code == 3:
                    err_txt = scan.get_error_logs_text()
                    raise RuntimeError(
                        f"Erreur d'execution SodaCL (code={exit_code}) : {err_txt}"
                    )
        finally:
            con.close()

    raw_checks = scan_results.get("checks", [])
    if not raw_checks:
        raise RuntimeError("Aucun controle SodaCL n'a ete evalue lors du scan")

    outcomes: list[QualityCheckOutcome] = []
    for chk in raw_checks:
        tbl_name = str(chk.get("table") or "unknown")
        chk_name = str(chk.get("name") or chk.get("definition") or "unnamed_check")
        raw_outcome = str(chk.get("outcome") or "fail").strip().upper()
        norm_outcome = raw_outcome if raw_outcome in ("PASS", "FAIL", "WARN") else "FAIL"
        diagnostics = chk.get("diagnostics") or {}
        failed_cnt = _extract_failed_count(norm_outcome, diagnostics)
        outcomes.append(
            QualityCheckOutcome(
                layer_name=_infer_layer_name(tbl_name),
                table_name=tbl_name,
                check_name=chk_name[:150],
                outcome=norm_outcome,
                failed_rows_count=failed_cnt,
            )
        )
    return outcomes


def _persist_quality_audit(
    conn: pg8000.dbapi.Connection,
    *,
    run_id: str,
    started_at: datetime,
    status: str,
    rows_evaluated: int,
    outcomes: list[QualityCheckOutcome],
    summary_details: dict[str, Any],
) -> None:
    finished_at = datetime.now(UTC)
    duration_ms = max(0, int((finished_at - started_at).total_seconds() * 1000))
    failed_checks = sum(1 for item in outcomes if item.outcome == "FAIL")

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
                run_id,
                PIPELINE_NAME,
                started_at,
                finished_at,
                status,
                rows_evaluated,
                failed_checks,
                duration_ms,
                json.dumps(summary_details, ensure_ascii=False),
            ),
        )

        if outcomes:
            payload_rows = [
                {
                    "run_id": run_id,
                    "layer_name": item.layer_name,
                    "check_name": item.check_name,
                    "outcome": item.outcome,
                    "failed_rows_count": item.failed_rows_count,
                    "evaluated_at": finished_at.isoformat(),
                }
                for item in outcomes
            ]
            cur.execute(
                """
                INSERT INTO ops_monitoring.quality_check_results (
                    run_id, layer_name, check_name,
                    outcome, failed_rows_count, evaluated_at
                )
                SELECT
                    CAST(r.run_id AS UUID),
                    r.layer_name,
                    r.check_name,
                    r.outcome,
                    r.failed_rows_count,
                    r.evaluated_at
                FROM jsonb_to_recordset(CAST(%s AS jsonb)) AS r(
                    run_id TEXT,
                    layer_name VARCHAR(40),
                    check_name VARCHAR(150),
                    outcome VARCHAR(20),
                    failed_rows_count INTEGER,
                    evaluated_at TIMESTAMPTZ
                );
                """,
                (json.dumps(payload_rows, ensure_ascii=False),),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def run_quality_gate(
    settings: EnvSettings | None = None,
    *,
    pg_conn: pg8000.dbapi.Connection | None = None,
    tables_override: dict[str, pl.DataFrame] | None = None,
    storage_options: Any = _DEFAULT_STORAGE,
    checks_path: Path | None = None,
    config_path: Path | None = None,
    record_monitoring: bool = True,
    raise_on_failure: bool = True,
) -> dict[str, Any]:
    """Execute le portail qualite Soda Core de bout en bout et historise les resultats."""
    cfg = settings or EnvSettings()
    started_at = datetime.now(UTC)
    run_id = str(uuid.uuid4())

    effective_checks_path = checks_path or cfg.soda_checks_path
    effective_config_path = config_path or cfg.soda_config_path
    resolved_storage: dict[str, str] | None = (
        cfg.delta_storage_options() if storage_options is _DEFAULT_STORAGE else storage_options
    )

    need_db = (tables_override is None) or record_monitoring
    owns_conn = pg_conn is None and need_db
    conn = (
        pg8000.dbapi.connect(**cfg.pg8000_connect_kwargs("pipeline"))
        if owns_conn
        else pg_conn
    )

    try:
        if tables_override is not None:
            datasets = tables_override
        else:
            if conn is None:
                raise RuntimeError("Connexion PostgreSQL requise pour extraire hr_audit")
            salt = cfg.sds_pii_hash_salt.get_secret_value()
            datasets = {
                "hr_employees_audit": extract_hr_audit_for_quality(conn, salt),
                "bronze_employees_audit": read_delta_table(
                    cfg.lakehouse_table_uri("bronze", cfg.bronze_employees_table),
                    resolved_storage,
                ),
                "bronze_sport_activities": read_delta_table(
                    cfg.lakehouse_table_uri("bronze", cfg.bronze_activities_table),
                    resolved_storage,
                ),
                "silver_employees_cleansed": read_delta_table(
                    cfg.lakehouse_table_uri("silver", cfg.silver_employees_table),
                    resolved_storage,
                ),
                "silver_sport_activities": read_delta_table(
                    cfg.lakehouse_table_uri("silver", cfg.silver_activities_table),
                    resolved_storage,
                ),
                "gold_fact_employee_benefits": read_delta_table(
                    cfg.lakehouse_table_uri("gold", cfg.gold_employee_benefits_table),
                    resolved_storage,
                ),
                "gold_agg_benefits_by_bu": read_delta_table(
                    cfg.lakehouse_table_uri("gold", cfg.gold_bu_summary_table),
                    resolved_storage,
                ),
                "gold_kpi_executive_snapshot": read_delta_table(
                    cfg.lakehouse_table_uri("gold", cfg.gold_executive_kpis_table),
                    resolved_storage,
                ),
            }

        outcomes = evaluate_soda_checks_on_tables(
            datasets,
            checks_path=effective_checks_path,
            config_path=effective_config_path,
        )

        passed = sum(1 for item in outcomes if item.outcome == "PASS")
        failed = sum(1 for item in outcomes if item.outcome == "FAIL")
        warned = sum(1 for item in outcomes if item.outcome == "WARN")
        rows_evaluated = sum(df.height for df in datasets.values())

        if failed > 0:
            run_status = "FAILED"
        elif warned > 0:
            run_status = "WARNING"
        else:
            run_status = "SUCCESS"

        by_layer: dict[str, dict[str, int]] = {}
        for item in outcomes:
            slot = by_layer.setdefault(
                item.layer_name, {"total": 0, "pass": 0, "fail": 0, "warn": 0}
            )
            slot["total"] += 1
            slot[item.outcome.lower()] += 1

        failed_names = [item.check_name for item in outcomes if item.outcome == "FAIL"]
        summary = {
            "run_id": run_id,
            "status": run_status,
            "total_checks": len(outcomes),
            "passed_checks": passed,
            "failed_checks": failed,
            "warned_checks": warned,
            "rows_evaluated": rows_evaluated,
            "layers": by_layer,
            "failed_check_names": failed_names,
        }

        if record_monitoring and conn is not None:
            _persist_quality_audit(
                conn,
                run_id=run_id,
                started_at=started_at,
                status=run_status,
                rows_evaluated=rows_evaluated,
                outcomes=outcomes,
                summary_details=summary,
            )

        if raise_on_failure and failed > 0:
            raise DataQualityGateError(
                f"Echec du portail qualite Soda Core ({failed}/{len(outcomes)} en echec) : "
                f"{failed_names}"
            )

        return {**summary, "checks": [item.__dict__ for item in outcomes]}
    finally:
        if owns_conn and conn is not None:
            conn.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Portail de controle qualite automatise Soda Core sur le Lakehouse SDS."
    )
    parser.add_argument(
        "--no-fail-exit",
        action="store_true",
        help="Journalise les echecs qualite sans lever d'exception bloquante.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    args = parse_args(argv)
    report = run_quality_gate(raise_on_failure=not args.no_fail_exit)
    compact = {k: v for k, v in report.items() if k != "checks"}
    logger.info("Portail qualite Soda Core valide : %s", json.dumps(compact, ensure_ascii=False))


if __name__ == "__main__":
    main()