import hashlib
import json
import logging
import uuid
from datetime import UTC, date, datetime
from typing import Any

import pg8000.dbapi
import polars as pl
from deltalake import DeltaTable, write_deltalake

from src.settings import BusinessRules, EnvSettings, load_business_rules

logger = logging.getLogger(__name__)

PIPELINE_NAME = "lakehouse_bronze_silver_sync"
_DEFAULT_STORAGE = object()

BRONZE_EMPLOYEES_SCHEMA: dict[str, pl.DataType] = {
    "employee_key": pl.String,
    "birth_year": pl.Int64,
    "business_unit": pl.String,
    "hire_date": pl.Date,
    "annual_gross_salary_eur": pl.Float64,
    "contract_type": pl.String,
    "paid_leave_days": pl.Int64,
    "declared_commute_mode": pl.String,
    "computed_distance_km": pl.Float64,
    "max_allowed_distance_km": pl.Float64,
    "is_commute_anomaly": pl.Boolean,
    "is_mobility_bonus_eligible": pl.Boolean,
    "anomaly_reason": pl.String,
    "source_updated_at": pl.Datetime("us", "UTC"),
    "ingested_at": pl.Datetime("us", "UTC"),
}

BRONZE_ACTIVITIES_SCHEMA: dict[str, pl.DataType] = {
    "activity_id": pl.Int64,
    "employee_key": pl.String,
    "sport_type": pl.String,
    "activity_start": pl.Datetime("us", "UTC"),
    "activity_end": pl.Datetime("us", "UTC"),
    "elapsed_seconds": pl.Int64,
    "distance_m": pl.Int64,
    "has_comment": pl.Boolean,
    "ingestion_mode": pl.String,
    "created_at": pl.Datetime("us", "UTC"),
    "ingested_at": pl.Datetime("us", "UTC"),
}


def pseudonymize_employee_id(employee_id: int, salt: str) -> str:
    """Genere un identifiant analytique deterministe et irreversible sans exposer l'ID RH brut."""
    cleaned_salt = salt.strip()
    if not cleaned_salt:
        raise ValueError("Le sel de pseudonymisation RGPD (SDS_PII_HASH_SALT) est vide")
    digest = hashlib.sha256(f"{cleaned_salt}:employee:{int(employee_id)}".encode()).hexdigest()
    return f"emp_{digest[:16]}"


def sanitize_anomaly_reason(
    reason: str | None,
    home_address: str | None = None,
) -> str | None:
    """Garantit qu'aucun motif d'anomalie ne vehicule d'adresse postale brute dans le Lakehouse."""
    if reason is None:
        return None
    cleaned = reason.strip()
    if not cleaned:
        return None
    if home_address and home_address.strip():
        addr = home_address.strip()
        if addr in cleaned:
            cleaned = cleaned.replace(addr, "[ADRESSE_MASQUEE_RGPD]")
    return cleaned


def _ensure_utc(value: datetime | None, fallback: datetime) -> datetime:
    if value is None:
        return fallback
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def write_delta_table(
    uri: str,
    df: pl.DataFrame,
    storage_options: dict[str, str] | None = None,
) -> int:
    """Ecrit un DataFrame Polars au format Delta Lake sans pyarrow et retourne la version."""
    opts = storage_options or None
    kwargs: dict[str, Any] = {"mode": "overwrite"}
    if opts is not None:
        kwargs["storage_options"] = opts
    if DeltaTable.is_deltatable(uri, storage_options=opts):
        kwargs["schema_mode"] = "overwrite"

    write_deltalake(uri, df, **kwargs)
    return int(DeltaTable(uri, storage_options=opts).version())


def read_delta_table(
    uri: str,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    if storage_options:
        return pl.read_delta(uri, storage_options=storage_options)
    return pl.read_delta(uri)


def extract_bronze_employees_df(
    conn: pg8000.dbapi.Connection,
    salt: str,
    *,
    ingested_at: datetime | None = None,
) -> pl.DataFrame:
    """Extrait l'audit RH en appliquant la minimisation RGPD des l'entree en couche Bronze."""
    now_utc = _ensure_utc(ingested_at, datetime.now(UTC))
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                employee_id,
                birth_date,
                business_unit,
                hire_date,
                annual_gross_salary_eur,
                contract_type,
                paid_leave_days,
                home_address,
                declared_commute_mode,
                computed_distance_km,
                max_allowed_distance_km,
                is_commute_anomaly,
                is_mobility_bonus_eligible,
                anomaly_reason,
                updated_at
            FROM hr_restricted.employees_audit
            ORDER BY employee_id ASC;
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    records: list[dict[str, Any]] = []
    for row in rows:
        (
            emp_id,
            birth_dt,
            bu,
            hire_dt,
            salary_eur,
            contract_type,
            leave_days,
            raw_address,
            commute_mode,
            dist_km,
            max_dist_km,
            is_anomaly,
            is_eligible,
            reason,
            updated_at,
        ) = row
        records.append(
            {
                "employee_key": pseudonymize_employee_id(int(emp_id), salt),
                "birth_year": int(birth_dt.year),
                "business_unit": str(bu),
                "hire_date": hire_dt,
                "annual_gross_salary_eur": round(float(salary_eur), 2),
                "contract_type": str(contract_type),
                "paid_leave_days": int(leave_days),
                "declared_commute_mode": str(commute_mode),
                "computed_distance_km": float(dist_km) if dist_km is not None else None,
                "max_allowed_distance_km": float(max_dist_km) if max_dist_km is not None else None,
                "is_commute_anomaly": bool(is_anomaly),
                "is_mobility_bonus_eligible": bool(is_eligible),
                "anomaly_reason": sanitize_anomaly_reason(reason, raw_address),
                "source_updated_at": _ensure_utc(updated_at, now_utc),
                "ingested_at": now_utc,
            }
        )

    return pl.DataFrame(records, schema=BRONZE_EMPLOYEES_SCHEMA)


def extract_bronze_activities_df(
    conn: pg8000.dbapi.Connection,
    salt: str,
    *,
    ingested_at: datetime | None = None,
) -> pl.DataFrame:
    """Extrait les activites OLTP en pseudonymisant le collaborateur et les commentaires."""
    now_utc = _ensure_utc(ingested_at, datetime.now(UTC))
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                activity_id,
                employee_id,
                sport_type,
                activity_start,
                activity_end,
                elapsed_seconds,
                distance_m,
                comment,
                ingestion_mode,
                created_at
            FROM oltp_sports.sport_activities
            ORDER BY activity_id ASC;
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    records: list[dict[str, Any]] = []
    for row in rows:
        (
            act_id,
            emp_id,
            sport_type,
            act_start,
            act_end,
            elapsed_secs,
            dist_m,
            raw_comment,
            ing_mode,
            created_at,
        ) = row
        records.append(
            {
                "activity_id": int(act_id),
                "employee_key": pseudonymize_employee_id(int(emp_id), salt),
                "sport_type": str(sport_type),
                "activity_start": _ensure_utc(act_start, now_utc),
                "activity_end": _ensure_utc(act_end, now_utc),
                "elapsed_seconds": int(elapsed_secs),
                "distance_m": int(dist_m) if dist_m is not None else None,
                "has_comment": bool(raw_comment and str(raw_comment).strip()),
                "ingestion_mode": str(ing_mode),
                "created_at": _ensure_utc(created_at, now_utc),
                "ingested_at": now_utc,
            }
        )

    return pl.DataFrame(records, schema=BRONZE_ACTIVITIES_SCHEMA)


def transform_silver_employees(
    bronze_employees_df: pl.DataFrame,
    *,
    as_of_date: date | None = None,
    processed_at: datetime | None = None,
) -> pl.DataFrame:
    now_utc = _ensure_utc(processed_at, datetime.now(UTC))
    ref_date = as_of_date or now_utc.date()

    return (
        bronze_employees_df.sort(["employee_key", "source_updated_at"])
        .unique(subset=["employee_key"], keep="last", maintain_order=True)
        .with_columns(
            [
                pl.col("business_unit").str.strip_chars(),
                pl.col("contract_type").str.strip_chars(),
                pl.col("declared_commute_mode").str.strip_chars(),
                pl.col("annual_gross_salary_eur").round(2),
                pl.col("hire_date").dt.year().cast(pl.Int64).alias("hire_year"),
                (
                    (pl.lit(ref_date, dtype=pl.Date) - pl.col("hire_date")).dt.total_days()
                    / 365.25
                )
                .clip(lower_bound=0.0)
                .round(2)
                .alias("seniority_years"),
                (pl.col("annual_gross_salary_eur") > 0.0).alias("is_salary_valid"),
                (
                    pl.col("computed_distance_km").is_null()
                    | (pl.col("computed_distance_km") >= 0.0)
                ).alias("is_distance_non_negative"),
                pl.lit(now_utc, dtype=pl.Datetime("us", "UTC")).alias("processed_at"),
            ]
        )
        .sort("employee_key")
    )


def transform_silver_activities(
    bronze_activities_df: pl.DataFrame,
    silver_employees_df: pl.DataFrame,
    rules: BusinessRules,
    *,
    processed_at: datetime | None = None,
) -> pl.DataFrame:
    now_utc = _ensure_utc(processed_at, datetime.now(UTC))
    norm_map = rules.sports_catalog.normalization_map
    distance_sports = rules.sports_catalog.distance_tracked_sports
    all_known_sports = distance_sports + rules.sports_catalog.non_distance_sports

    sport_expr = pl.col("sport_type").str.strip_chars()
    if norm_map:
        sport_expr = sport_expr.replace(norm_map)

    emp_lookup = silver_employees_df.select(
        ["employee_key", "business_unit", "contract_type", "hire_date"]
    )

    enriched = (
        bronze_activities_df.sort(["activity_id", "created_at"])
        .unique(subset=["activity_id"], keep="last", maintain_order=True)
        .with_columns(
            [
                sport_expr.alias("sport_type"),
                pl.col("ingestion_mode").str.strip_chars(),
                pl.col("activity_start").dt.date().alias("activity_date"),
                pl.col("activity_start").dt.strftime("%Y-%m").alias("activity_month"),
                (pl.col("elapsed_seconds").cast(pl.Float64) / 60.0)
                .round(2)
                .alias("elapsed_minutes"),
                pl.when(pl.col("distance_m").is_not_null())
                .then((pl.col("distance_m").cast(pl.Float64) / 1000.0).round(3))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("distance_km"),
            ]
        )
        .with_columns(
            [
                pl.col("sport_type").is_in(distance_sports).alias("is_distance_sport"),
                pl.col("sport_type").is_in(all_known_sports).alias("is_catalog_sport"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("is_distance_sport"))
                .then(pl.lit("Endurance metrique"))
                .otherwise(pl.lit("Sans distance"))
                .alias("sport_category"),
            ]
        )
        .join(emp_lookup, on="employee_key", how="left")
        .with_columns(
            [
                pl.col("business_unit").is_not_null().alias("has_valid_employee_ref"),
                (
                    (pl.col("activity_end") >= pl.col("activity_start"))
                    & (pl.col("elapsed_seconds") > 0)
                ).alias("is_chronology_valid"),
                pl.when(pl.col("hire_date").is_not_null())
                .then(pl.col("activity_date") >= pl.col("hire_date"))
                .otherwise(pl.lit(False))
                .alias("is_after_hire_date"),
                pl.when(pl.col("is_distance_sport"))
                .then(
                    pl.col("distance_m").is_not_null() & (pl.col("distance_m") >= 0)
                )
                .otherwise(pl.col("distance_m").is_null())
                .alias("is_distance_coherent"),
            ]
        )
        .with_columns(
            [
                (
                    pl.col("is_catalog_sport")
                    & pl.col("has_valid_employee_ref")
                    & pl.col("is_chronology_valid")
                    & pl.col("is_after_hire_date")
                    & pl.col("is_distance_coherent")
                ).alias("is_valid_activity"),
                pl.lit(now_utc, dtype=pl.Datetime("us", "UTC")).alias("processed_at"),
            ]
        )
        .sort(["activity_start", "activity_id"])
    )
    return enriched


def _record_lakehouse_run(
    conn: pg8000.dbapi.Connection,
    *,
    started_at: datetime,
    status: str,
    rows_processed: int,
    anomalies_detected: int,
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
                rows_processed,
                anomalies_detected,
                duration_ms,
                json.dumps(details, ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        cur.close()


def run_bronze_silver_pipeline(
    settings: EnvSettings | None = None,
    rules: BusinessRules | None = None,
    *,
    pg_conn: pg8000.dbapi.Connection | None = None,
    uri_overrides: dict[str, str] | None = None,
    storage_options: Any = _DEFAULT_STORAGE,
    as_of_date: date | None = None,
    record_monitoring: bool = True,
) -> dict[str, Any]:
    """Synchronise les couches Delta Lake Bronze et Silver depuis PostgreSQL."""
    cfg = settings or EnvSettings()
    biz_rules = rules or load_business_rules()
    started_at = datetime.now(UTC)

    resolved_storage: dict[str, str] | None = (
        cfg.delta_storage_options() if storage_options is _DEFAULT_STORAGE else storage_options
    )
    uris = {
        "bronze_employees": cfg.lakehouse_table_uri("bronze", cfg.bronze_employees_table),
        "bronze_activities": cfg.lakehouse_table_uri("bronze", cfg.bronze_activities_table),
        "silver_employees": cfg.lakehouse_table_uri("silver", cfg.silver_employees_table),
        "silver_activities": cfg.lakehouse_table_uri("silver", cfg.silver_activities_table),
    }
    if uri_overrides:
        uris.update(uri_overrides)

    owns_conn = pg_conn is None
    conn = pg_conn or pg8000.dbapi.connect(**cfg.pg8000_connect_kwargs("pipeline"))
    salt = cfg.sds_pii_hash_salt.get_secret_value()

    try:
        bronze_emp_df = extract_bronze_employees_df(conn, salt, ingested_at=started_at)
        bronze_act_df = extract_bronze_activities_df(conn, salt, ingested_at=started_at)

        v_bronze_emp = write_delta_table(
            uris["bronze_employees"], bronze_emp_df, resolved_storage
        )
        v_bronze_act = write_delta_table(
            uris["bronze_activities"], bronze_act_df, resolved_storage
        )

        reloaded_bronze_emp = read_delta_table(uris["bronze_employees"], resolved_storage)
        reloaded_bronze_act = read_delta_table(uris["bronze_activities"], resolved_storage)

        silver_emp_df = transform_silver_employees(
            reloaded_bronze_emp,
            as_of_date=as_of_date,
            processed_at=started_at,
        )
        silver_act_df = transform_silver_activities(
            reloaded_bronze_act,
            silver_emp_df,
            biz_rules,
            processed_at=started_at,
        )

        v_silver_emp = write_delta_table(
            uris["silver_employees"], silver_emp_df, resolved_storage
        )
        v_silver_act = write_delta_table(
            uris["silver_activities"], silver_act_df, resolved_storage
        )

        invalid_activities = int(
            silver_act_df.filter(~pl.col("is_valid_activity")).height
        )
        commute_anomalies = int(
            silver_emp_df.filter(pl.col("is_commute_anomaly")).height
        )
        distinct_sports = int(silver_act_df.select(pl.col("sport_type").n_unique()).item())

        summary = {
            "bronze": {
                "employees_uri": uris["bronze_employees"],
                "employees_rows": bronze_emp_df.height,
                "employees_delta_version": v_bronze_emp,
                "activities_uri": uris["bronze_activities"],
                "activities_rows": bronze_act_df.height,
                "activities_delta_version": v_bronze_act,
            },
            "silver": {
                "employees_uri": uris["silver_employees"],
                "employees_rows": silver_emp_df.height,
                "employees_delta_version": v_silver_emp,
                "commute_anomalies_count": commute_anomalies,
                "activities_uri": uris["silver_activities"],
                "activities_rows": silver_act_df.height,
                "activities_delta_version": v_silver_act,
                "valid_activities_rows": silver_act_df.height - invalid_activities,
                "invalid_activities_rows": invalid_activities,
                "distinct_normalized_sports": distinct_sports,
            },
        }

        if record_monitoring:
            _record_lakehouse_run(
                conn,
                started_at=started_at,
                status="SUCCESS",
                rows_processed=silver_emp_df.height + silver_act_df.height,
                anomalies_detected=commute_anomalies + invalid_activities,
                details=summary,
            )
        return summary
    except Exception as exc:
        if record_monitoring:
            try:
                _record_lakehouse_run(
                    conn,
                    started_at=started_at,
                    status="FAILED",
                    rows_processed=0,
                    anomalies_detected=1,
                    details={
                        "error_type": type(exc).__name__,
                        "error_message": str(exc)[:500],
                    },
                )
            except Exception:
                logger.warning("Echec de journalisation de l'erreur Bronze/Silver")
        raise
    finally:
        if owns_conn:
            conn.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    summary = run_bronze_silver_pipeline()
    logger.info("Synchronisation Bronze/Silver terminee : %s", json.dumps(summary))


if __name__ == "__main__":
    main()