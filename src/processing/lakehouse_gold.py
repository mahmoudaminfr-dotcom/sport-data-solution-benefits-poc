import calendar
import json
import logging
import re
import tempfile
import time
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pg8000.dbapi
import polars as pl

from src.processing.lakehouse_bronze_silver import (
    read_delta_table,
    write_delta_table,
)
from src.settings import BusinessRules, EnvSettings, load_business_rules

logger = logging.getLogger(__name__)

PIPELINE_NAME = "lakehouse_gold_recompute"
_DEFAULT_STORAGE = object()
_SAFE_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

GOLD_EMPLOYEE_BENEFITS_SCHEMA: dict[str, pl.DataType] = {
    "employee_key": pl.String,
    "business_unit": pl.String,
    "contract_type": pl.String,
    "birth_year": pl.Int64,
    "hire_date": pl.Date,
    "seniority_years": pl.Float64,
    "annual_gross_salary_eur": pl.Float64,
    "declared_commute_mode": pl.String,
    "computed_distance_km": pl.Float64,
    "effective_max_distance_km": pl.Float64,
    "is_commute_anomaly": pl.Boolean,
    "is_mobility_bonus_eligible": pl.Boolean,
    "applied_prime_rate": pl.Float64,
    "mobility_bonus_eur": pl.Float64,
    "valid_activities_12m": pl.Int64,
    "distinct_sports_12m": pl.Int64,
    "primary_sport": pl.String,
    "total_elapsed_hours_12m": pl.Float64,
    "total_distance_km_12m": pl.Float64,
    "is_wellbeing_eligible": pl.Boolean,
    "wellbeing_days_granted": pl.Int64,
    "wellbeing_imputed_value_eur": pl.Float64,
    "reference_date": pl.Date,
    "computed_at": pl.Datetime("us", "UTC"),
}

GOLD_BU_SUMMARY_SCHEMA: dict[str, pl.DataType] = {
    "business_unit": pl.String,
    "headcount": pl.Int64,
    "active_sport_employees": pl.Int64,
    "sport_participation_rate_pct": pl.Float64,
    "mobility_eligible_count": pl.Int64,
    "commute_anomaly_count": pl.Int64,
    "wellbeing_eligible_count": pl.Int64,
    "total_payroll_eur": pl.Float64,
    "total_mobility_bonus_eur": pl.Float64,
    "avg_mobility_bonus_eligible_eur": pl.Float64,
    "total_wellbeing_days": pl.Int64,
    "total_activities_12m": pl.Int64,
    "total_distance_km_12m": pl.Float64,
    "applied_prime_rate": pl.Float64,
    "computed_at": pl.Datetime("us", "UTC"),
}

GOLD_SPORTS_MONTHLY_SCHEMA: dict[str, pl.DataType] = {
    "activity_month": pl.String,
    "sport_type": pl.String,
    "sport_category": pl.String,
    "activities_count": pl.Int64,
    "active_employees_count": pl.Int64,
    "total_elapsed_hours": pl.Float64,
    "total_distance_km": pl.Float64,
    "commented_sessions_count": pl.Int64,
    "computed_at": pl.Datetime("us", "UTC"),
}

GOLD_EXECUTIVE_KPIS_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_id": pl.Int64,
    "reference_date": pl.Date,
    "total_employees": pl.Int64,
    "active_sport_employees": pl.Int64,
    "sport_participation_rate_pct": pl.Float64,
    "mobility_eligible_employees": pl.Int64,
    "mobility_eligibility_rate_pct": pl.Float64,
    "commute_anomalies_count": pl.Int64,
    "wellbeing_eligible_employees": pl.Int64,
    "wellbeing_eligibility_rate_pct": pl.Float64,
    "total_wellbeing_days_granted": pl.Int64,
    "total_payroll_eur": pl.Float64,
    "total_mobility_bonus_eur": pl.Float64,
    "mobility_cost_share_of_payroll_pct": pl.Float64,
    "total_valid_activities_12m": pl.Int64,
    "distinct_sports_count": pl.Int64,
    "applied_prime_rate": pl.Float64,
    "max_walk_run_km": pl.Float64,
    "max_bike_micro_km": pl.Float64,
    "min_annual_activities": pl.Int64,
    "wellbeing_days_per_employee": pl.Int64,
    "computed_at": pl.Datetime("us", "UTC"),
}

GOLD_MART_DDL = """
CREATE TABLE IF NOT EXISTS gold_mart.fact_employee_benefits (
    employee_key VARCHAR(40) PRIMARY KEY,
    business_unit VARCHAR(60) NOT NULL,
    contract_type VARCHAR(20) NOT NULL,
    birth_year INTEGER NOT NULL,
    hire_date DATE NOT NULL,
    seniority_years NUMERIC(6, 2) NOT NULL CHECK (seniority_years >= 0),
    annual_gross_salary_eur NUMERIC(12, 2) NOT NULL CHECK (annual_gross_salary_eur > 0),
    declared_commute_mode VARCHAR(80) NOT NULL,
    computed_distance_km NUMERIC(8, 3) CHECK (
        computed_distance_km IS NULL OR computed_distance_km >= 0
    ),
    effective_max_distance_km NUMERIC(6, 2),
    is_commute_anomaly BOOLEAN NOT NULL,
    is_mobility_bonus_eligible BOOLEAN NOT NULL,
    applied_prime_rate NUMERIC(5, 4) NOT NULL CHECK (applied_prime_rate >= 0),
    mobility_bonus_eur NUMERIC(12, 2) NOT NULL CHECK (mobility_bonus_eur >= 0),
    valid_activities_12m INTEGER NOT NULL CHECK (valid_activities_12m >= 0),
    distinct_sports_12m INTEGER NOT NULL CHECK (distinct_sports_12m >= 0),
    primary_sport VARCHAR(80),
    total_elapsed_hours_12m NUMERIC(10, 2) NOT NULL CHECK (total_elapsed_hours_12m >= 0),
    total_distance_km_12m NUMERIC(10, 2) NOT NULL CHECK (total_distance_km_12m >= 0),
    is_wellbeing_eligible BOOLEAN NOT NULL,
    wellbeing_days_granted INTEGER NOT NULL CHECK (wellbeing_days_granted >= 0),
    wellbeing_imputed_value_eur NUMERIC(12, 2) NOT NULL CHECK (wellbeing_imputed_value_eur >= 0),
    reference_date DATE NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_mart.agg_benefits_by_bu (
    business_unit VARCHAR(60) PRIMARY KEY,
    headcount INTEGER NOT NULL CHECK (headcount >= 0),
    active_sport_employees INTEGER NOT NULL CHECK (active_sport_employees >= 0),
    sport_participation_rate_pct NUMERIC(6, 2) NOT NULL,
    mobility_eligible_count INTEGER NOT NULL CHECK (mobility_eligible_count >= 0),
    commute_anomaly_count INTEGER NOT NULL CHECK (commute_anomaly_count >= 0),
    wellbeing_eligible_count INTEGER NOT NULL CHECK (wellbeing_eligible_count >= 0),
    total_payroll_eur NUMERIC(14, 2) NOT NULL CHECK (total_payroll_eur >= 0),
    total_mobility_bonus_eur NUMERIC(14, 2) NOT NULL CHECK (total_mobility_bonus_eur >= 0),
    avg_mobility_bonus_eligible_eur NUMERIC(12, 2) NOT NULL,
    total_wellbeing_days INTEGER NOT NULL CHECK (total_wellbeing_days >= 0),
    total_activities_12m INTEGER NOT NULL CHECK (total_activities_12m >= 0),
    total_distance_km_12m NUMERIC(12, 2) NOT NULL CHECK (total_distance_km_12m >= 0),
    applied_prime_rate NUMERIC(5, 4) NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_mart.agg_sports_monthly (
    activity_month VARCHAR(7) NOT NULL,
    sport_type VARCHAR(80) NOT NULL,
    sport_category VARCHAR(40) NOT NULL,
    activities_count INTEGER NOT NULL CHECK (activities_count >= 0),
    active_employees_count INTEGER NOT NULL CHECK (active_employees_count >= 0),
    total_elapsed_hours NUMERIC(10, 2) NOT NULL CHECK (total_elapsed_hours >= 0),
    total_distance_km NUMERIC(12, 2) CHECK (total_distance_km IS NULL OR total_distance_km >= 0),
    commented_sessions_count INTEGER NOT NULL CHECK (commented_sessions_count >= 0),
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (activity_month, sport_type)
);

CREATE TABLE IF NOT EXISTS gold_mart.kpi_executive_snapshot (
    snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
    reference_date DATE NOT NULL,
    total_employees INTEGER NOT NULL CHECK (total_employees >= 0),
    active_sport_employees INTEGER NOT NULL CHECK (active_sport_employees >= 0),
    sport_participation_rate_pct NUMERIC(6, 2) NOT NULL,
    mobility_eligible_employees INTEGER NOT NULL CHECK (mobility_eligible_employees >= 0),
    mobility_eligibility_rate_pct NUMERIC(6, 2) NOT NULL,
    commute_anomalies_count INTEGER NOT NULL CHECK (commute_anomalies_count >= 0),
    wellbeing_eligible_employees INTEGER NOT NULL CHECK (wellbeing_eligible_employees >= 0),
    wellbeing_eligibility_rate_pct NUMERIC(6, 2) NOT NULL,
    total_wellbeing_days_granted INTEGER NOT NULL CHECK (total_wellbeing_days_granted >= 0),
    total_payroll_eur NUMERIC(14, 2) NOT NULL CHECK (total_payroll_eur >= 0),
    total_mobility_bonus_eur NUMERIC(14, 2) NOT NULL CHECK (total_mobility_bonus_eur >= 0),
    mobility_cost_share_of_payroll_pct NUMERIC(6, 3) NOT NULL,
    total_valid_activities_12m INTEGER NOT NULL CHECK (total_valid_activities_12m >= 0),
    distinct_sports_count INTEGER NOT NULL CHECK (distinct_sports_count >= 0),
    applied_prime_rate NUMERIC(5, 4) NOT NULL,
    max_walk_run_km NUMERIC(6, 2) NOT NULL,
    max_bike_micro_km NUMERIC(6, 2) NOT NULL,
    min_annual_activities INTEGER NOT NULL,
    wellbeing_days_per_employee INTEGER NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL
);
"""


def _without_computed_at(schema: dict[str, pl.DataType]) -> dict[str, pl.DataType]:
    return {col: dtype for col, dtype in schema.items() if col != "computed_at"}


def _attach_computed_at(df: pl.DataFrame, now_utc: datetime) -> pl.DataFrame:
    return df.with_columns(
        pl.lit(now_utc, dtype=pl.Datetime("us", "UTC")).alias("computed_at")
    )


def subtract_calendar_months(ref_date: date, months: int) -> date:
    if months < 0:
        raise ValueError(f"Nombre de mois invalide : {months}")
    total_months = (ref_date.year * 12 + (ref_date.month - 1)) - months
    target_year, zero_month = divmod(total_months, 12)
    target_month = zero_month + 1
    max_day = calendar.monthrange(target_year, target_month)[1]
    return date(target_year, target_month, min(ref_date.day, max_day))


def _pg_json_default(obj: Any) -> str:
    if isinstance(obj, datetime | date):
        return obj.isoformat()
    raise TypeError(f"Type non serialisable pour JSONB : {type(obj)!r}")


def _register_polars_in_duckdb(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    df: pl.DataFrame,
    tmp_dir: Path,
) -> None:
    """Enregistre un DataFrame Polars dans DuckDB en zero-copie via arro3 sans pyarrow."""
    try:
        import arro3.core

        arrow_tbl = arro3.core.Table.from_arrow(df)
        con.register(view_name, arrow_tbl)
    except Exception:
        parquet_file = tmp_dir / f"{view_name}.parquet"
        df.write_parquet(parquet_file)
        escaped = str(parquet_file).replace("'", "''")
        con.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{escaped}')"
        ).fetchall()


def _reevaluate_commute_rules(
    silver_employees_df: pl.DataFrame,
    rules: BusinessRules,
) -> pl.DataFrame:
    """Reevalue dynamiquement les plafonds kilometriques selon les regles metier actives."""
    modes = silver_employees_df["declared_commute_mode"].to_list()
    distances = silver_employees_df["computed_distance_km"].to_list()

    eff_max_list: list[float | None] = []
    elig_list: list[bool] = []
    anom_list: list[bool] = []

    for mode_raw, dist_val in zip(modes, distances, strict=True):
        mode = str(mode_raw).strip()
        rule_cfg = rules.mobility_bonus.eligible_modes.get(mode)
        eff_max = float(rule_cfg.max_distance_km) if rule_cfg is not None else None
        is_elig, is_anom, _ = rules.evaluate_commute_declaration(mode, dist_val)
        eff_max_list.append(eff_max)
        elig_list.append(is_elig)
        anom_list.append(is_anom)

    return silver_employees_df.with_columns(
        [
            pl.Series("effective_max_distance_km", eff_max_list, dtype=pl.Float64),
            pl.Series("is_mobility_bonus_eligible", elig_list, dtype=pl.Boolean),
            pl.Series("is_commute_anomaly", anom_list, dtype=pl.Boolean),
        ]
    )


def compute_gold_tables(
    silver_employees_df: pl.DataFrame,
    silver_activities_df: pl.DataFrame,
    rules: BusinessRules,
    *,
    as_of_date: date | None = None,
    computed_at: datetime | None = None,
) -> dict[str, pl.DataFrame]:
    """Calcule les 4 tables Gold via DuckDB et Polars sans JVM, pyarrow ni pytz."""
    now_utc = computed_at.astimezone(UTC) if computed_at else datetime.now(UTC)

    if as_of_date is not None:
        ref_date = as_of_date
    elif silver_activities_df.height > 0:
        max_act_date = silver_activities_df.select(pl.col("activity_date").max()).item()
        ref_date = max_act_date if isinstance(max_act_date, date) else now_utc.date()
    else:
        ref_date = now_utc.date()

    window_start = subtract_calendar_months(
        ref_date, rules.wellbeing_days.rolling_window_months
    )
    window_start_iso = window_start.isoformat()
    ref_date_iso = ref_date.isoformat()

    prime_rate = float(rules.mobility_bonus.annual_gross_salary_rate)
    min_acts = int(rules.wellbeing_days.min_activities_per_year)
    granted_days = int(rules.wellbeing_days.granted_days_count)
    max_walk_km = float(
        rules.mobility_bonus.eligible_modes["Marche/running"].max_distance_km
    )
    max_bike_km = float(
        rules.mobility_bonus.eligible_modes["Vélo/Trottinette/Autres"].max_distance_km
    )

    evaluated_emp_df = _reevaluate_commute_rules(silver_employees_df, rules)

    emp_for_sql = evaluated_emp_df.select(
        [
            "employee_key",
            "business_unit",
            "contract_type",
            "birth_year",
            "hire_date",
            "seniority_years",
            "annual_gross_salary_eur",
            "declared_commute_mode",
            "computed_distance_km",
            "effective_max_distance_km",
            "is_commute_anomaly",
            "is_mobility_bonus_eligible",
        ]
    )
    act_for_sql = silver_activities_df.select(
        [
            "employee_key",
            "sport_type",
            "sport_category",
            "is_distance_sport",
            "elapsed_seconds",
            "distance_m",
            "has_comment",
            "is_valid_activity",
            "activity_date",
            "activity_month",
        ]
    )

    with tempfile.TemporaryDirectory() as tmp_str:
        tmp_dir = Path(tmp_str)
        con = duckdb.connect(":memory:")
        try:
            _register_polars_in_duckdb(con, "silver_employees", emp_for_sql, tmp_dir)
            _register_polars_in_duckdb(con, "silver_activities", act_for_sql, tmp_dir)

            fact_rows = con.execute(
                """
                WITH filtered_activities AS (
                    SELECT
                        employee_key,
                        sport_type,
                        elapsed_seconds,
                        distance_m
                    FROM silver_activities
                    WHERE is_valid_activity = TRUE
                      AND activity_date >= CAST(? AS DATE)
                      AND activity_date <= CAST(? AS DATE)
                ),
                sport_ranking AS (
                    SELECT
                        employee_key,
                        sport_type,
                        ROW_NUMBER() OVER (
                            PARTITION BY employee_key
                            ORDER BY COUNT(*) DESC, SUM(elapsed_seconds) DESC, sport_type ASC
                        ) AS rn
                    FROM filtered_activities
                    GROUP BY employee_key, sport_type
                ),
                emp_activity_agg AS (
                    SELECT
                        employee_key,
                        CAST(COUNT(*) AS BIGINT) AS valid_activities_12m,
                        CAST(COUNT(DISTINCT sport_type) AS BIGINT) AS distinct_sports_12m,
                        ROUND(SUM(elapsed_seconds) / 3600.0, 2) AS total_elapsed_hours_12m,
                        ROUND(COALESCE(SUM(distance_m), 0) / 1000.0, 2) AS total_distance_km_12m
                    FROM filtered_activities
                    GROUP BY employee_key
                )
                SELECT
                    e.employee_key,
                    e.business_unit,
                    e.contract_type,
                    CAST(e.birth_year AS BIGINT) AS birth_year,
                    e.hire_date,
                    ROUND(e.seniority_years, 2) AS seniority_years,
                    CAST(
                        ROUND(CAST(e.annual_gross_salary_eur AS DECIMAL(14, 2)), 2) AS DOUBLE
                    ) AS annual_gross_salary_eur,
                    e.declared_commute_mode,
                    CASE
                        WHEN e.computed_distance_km IS NOT NULL
                        THEN ROUND(e.computed_distance_km, 3)
                        ELSE NULL
                    END AS computed_distance_km,
                    CASE
                        WHEN e.effective_max_distance_km IS NOT NULL
                        THEN ROUND(e.effective_max_distance_km, 2)
                        ELSE NULL
                    END AS effective_max_distance_km,
                    e.is_commute_anomaly,
                    e.is_mobility_bonus_eligible,
                    ROUND(?, 4) AS applied_prime_rate,
                    CAST(
                        CASE
                            WHEN e.is_mobility_bonus_eligible
                            THEN ROUND(
                                CAST(e.annual_gross_salary_eur AS DECIMAL(14, 2)) *
                                CAST(? AS DECIMAL(8, 4)),
                                2
                            )
                            ELSE CAST(0.00 AS DECIMAL(14, 2))
                        END AS DOUBLE
                    ) AS mobility_bonus_eur,
                    COALESCE(a.valid_activities_12m, 0) AS valid_activities_12m,
                    COALESCE(a.distinct_sports_12m, 0) AS distinct_sports_12m,
                    sr.sport_type AS primary_sport,
                    COALESCE(a.total_elapsed_hours_12m, 0.0) AS total_elapsed_hours_12m,
                    COALESCE(a.total_distance_km_12m, 0.0) AS total_distance_km_12m,
                    (COALESCE(a.valid_activities_12m, 0) >= ?) AS is_wellbeing_eligible,
                    CASE
                        WHEN COALESCE(a.valid_activities_12m, 0) >= ? THEN CAST(? AS BIGINT)
                        ELSE 0
                    END AS wellbeing_days_granted,
                    CAST(
                        CASE
                            WHEN COALESCE(a.valid_activities_12m, 0) >= ?
                            THEN ROUND(
                                (CAST(e.annual_gross_salary_eur AS DECIMAL(14, 2)) / 218.0) *
                                CAST(? AS DECIMAL(8, 2)),
                                2
                            )
                            ELSE CAST(0.00 AS DECIMAL(14, 2))
                        END AS DOUBLE
                    ) AS wellbeing_imputed_value_eur,
                    CAST(? AS DATE) AS reference_date
                FROM silver_employees e
                LEFT JOIN emp_activity_agg a ON e.employee_key = a.employee_key
                LEFT JOIN sport_ranking sr ON e.employee_key = sr.employee_key AND sr.rn = 1
                ORDER BY e.employee_key ASC;
                """,
                [
                    window_start_iso,
                    ref_date_iso,
                    prime_rate,
                    prime_rate,
                    min_acts,
                    min_acts,
                    granted_days,
                    min_acts,
                    granted_days,
                    ref_date_iso,
                ],
            ).fetchall()

            fact_sql_df = pl.DataFrame(
                fact_rows,
                schema=_without_computed_at(GOLD_EMPLOYEE_BENEFITS_SCHEMA),
                orient="row",
            )
            _register_polars_in_duckdb(con, "gold_fact_emp", fact_sql_df, tmp_dir)

            bu_rows = con.execute(
                """
                SELECT
                    business_unit,
                    CAST(COUNT(*) AS BIGINT) AS headcount,
                    CAST(
                        SUM(CASE WHEN valid_activities_12m > 0 THEN 1 ELSE 0 END) AS BIGINT
                    ) AS active_sport_employees,
                    COALESCE(
                        ROUND(
                            100.0 * SUM(CASE WHEN valid_activities_12m > 0 THEN 1 ELSE 0 END)
                            / NULLIF(COUNT(*), 0),
                            2
                        ),
                        0.0
                    ) AS sport_participation_rate_pct,
                    CAST(
                        SUM(CASE WHEN is_mobility_bonus_eligible THEN 1 ELSE 0 END) AS BIGINT
                    ) AS mobility_eligible_count,
                    CAST(
                        SUM(CASE WHEN is_commute_anomaly THEN 1 ELSE 0 END) AS BIGINT
                    ) AS commute_anomaly_count,
                    CAST(
                        SUM(CASE WHEN is_wellbeing_eligible THEN 1 ELSE 0 END) AS BIGINT
                    ) AS wellbeing_eligible_count,
                    CAST(
                        ROUND(SUM(CAST(annual_gross_salary_eur AS DECIMAL(14, 2))), 2) AS DOUBLE
                    ) AS total_payroll_eur,
                    CAST(
                        ROUND(SUM(CAST(mobility_bonus_eur AS DECIMAL(14, 2))), 2) AS DOUBLE
                    ) AS total_mobility_bonus_eur,
                    CAST(
                        COALESCE(
                            ROUND(
                                AVG(
                                    CASE
                                        WHEN is_mobility_bonus_eligible
                                        THEN CAST(mobility_bonus_eur AS DECIMAL(14, 2))
                                    END
                                ),
                                2
                            ),
                            0.00
                        ) AS DOUBLE
                    ) AS avg_mobility_bonus_eligible_eur,
                    CAST(SUM(wellbeing_days_granted) AS BIGINT) AS total_wellbeing_days,
                    CAST(SUM(valid_activities_12m) AS BIGINT) AS total_activities_12m,
                    ROUND(SUM(total_distance_km_12m), 2) AS total_distance_km_12m,
                    ROUND(?, 4) AS applied_prime_rate
                FROM gold_fact_emp
                GROUP BY business_unit
                ORDER BY total_mobility_bonus_eur DESC, business_unit ASC;
                """,
                [prime_rate],
            ).fetchall()

            bu_sql_df = pl.DataFrame(
                bu_rows,
                schema=_without_computed_at(GOLD_BU_SUMMARY_SCHEMA),
                orient="row",
            )

            sports_rows = con.execute(
                """
                SELECT
                    activity_month,
                    sport_type,
                    sport_category,
                    CAST(COUNT(*) AS BIGINT) AS activities_count,
                    CAST(COUNT(DISTINCT employee_key) AS BIGINT) AS active_employees_count,
                    ROUND(SUM(elapsed_seconds) / 3600.0, 2) AS total_elapsed_hours,
                    CASE
                        WHEN BOOL_OR(is_distance_sport)
                        THEN ROUND(COALESCE(SUM(distance_m), 0) / 1000.0, 2)
                        ELSE NULL
                    END AS total_distance_km,
                    CAST(
                        SUM(CASE WHEN has_comment THEN 1 ELSE 0 END) AS BIGINT
                    ) AS commented_sessions_count
                FROM silver_activities
                WHERE is_valid_activity = TRUE
                  AND activity_date >= CAST(? AS DATE)
                  AND activity_date <= CAST(? AS DATE)
                GROUP BY activity_month, sport_type, sport_category
                ORDER BY activity_month ASC, activities_count DESC, sport_type ASC;
                """,
                [window_start_iso, ref_date_iso],
            ).fetchall()

            sports_sql_df = pl.DataFrame(
                sports_rows,
                schema=_without_computed_at(GOLD_SPORTS_MONTHLY_SCHEMA),
                orient="row",
            )

            distinct_sports_row = con.execute(
                """
                SELECT CAST(COUNT(DISTINCT sport_type) AS BIGINT)
                FROM silver_activities
                WHERE is_valid_activity = TRUE
                  AND activity_date >= CAST(? AS DATE)
                  AND activity_date <= CAST(? AS DATE);
                """,
                [window_start_iso, ref_date_iso],
            ).fetchall()
            distinct_sports_cnt = int(distinct_sports_row[0][0]) if distinct_sports_row else 0

            kpi_rows = con.execute(
                """
                SELECT
                    CAST(1 AS BIGINT) AS snapshot_id,
                    CAST(? AS DATE) AS reference_date,
                    CAST(COUNT(*) AS BIGINT) AS total_employees,
                    CAST(
                        COALESCE(SUM(CASE WHEN valid_activities_12m > 0 THEN 1 ELSE 0 END), 0)
                        AS BIGINT
                    ) AS active_sport_employees,
                    COALESCE(
                        ROUND(
                            100.0 * SUM(CASE WHEN valid_activities_12m > 0 THEN 1 ELSE 0 END)
                            / NULLIF(COUNT(*), 0),
                            2
                        ),
                        0.0
                    ) AS sport_participation_rate_pct,
                    CAST(
                        COALESCE(SUM(CASE WHEN is_mobility_bonus_eligible THEN 1 ELSE 0 END), 0)
                        AS BIGINT
                    ) AS mobility_eligible_employees,
                    COALESCE(
                        ROUND(
                            100.0 * SUM(CASE WHEN is_mobility_bonus_eligible THEN 1 ELSE 0 END)
                            / NULLIF(COUNT(*), 0),
                            2
                        ),
                        0.0
                    ) AS mobility_eligibility_rate_pct,
                    CAST(
                        COALESCE(SUM(CASE WHEN is_commute_anomaly THEN 1 ELSE 0 END), 0)
                        AS BIGINT
                    ) AS commute_anomalies_count,
                    CAST(
                        COALESCE(SUM(CASE WHEN is_wellbeing_eligible THEN 1 ELSE 0 END), 0)
                        AS BIGINT
                    ) AS wellbeing_eligible_employees,
                    COALESCE(
                        ROUND(
                            100.0 * SUM(CASE WHEN is_wellbeing_eligible THEN 1 ELSE 0 END)
                            / NULLIF(COUNT(*), 0),
                            2
                        ),
                        0.0
                    ) AS wellbeing_eligibility_rate_pct,
                    CAST(COALESCE(SUM(wellbeing_days_granted), 0) AS BIGINT)
                        AS total_wellbeing_days_granted,
                    CAST(
                        COALESCE(
                            ROUND(SUM(CAST(annual_gross_salary_eur AS DECIMAL(14, 2))), 2),
                            0.00
                        ) AS DOUBLE
                    ) AS total_payroll_eur,
                    CAST(
                        COALESCE(
                            ROUND(SUM(CAST(mobility_bonus_eur AS DECIMAL(14, 2))), 2),
                            0.00
                        ) AS DOUBLE
                    ) AS total_mobility_bonus_eur,
                    CAST(
                        COALESCE(
                            ROUND(
                                100.0 * SUM(CAST(mobility_bonus_eur AS DECIMAL(14, 2)))
                                / NULLIF(SUM(CAST(annual_gross_salary_eur AS DECIMAL(14, 2))), 0),
                                3
                            ),
                            0.000
                        ) AS DOUBLE
                    ) AS mobility_cost_share_of_payroll_pct,
                    CAST(COALESCE(SUM(valid_activities_12m), 0) AS BIGINT)
                        AS total_valid_activities_12m,
                    CAST(? AS BIGINT) AS distinct_sports_count,
                    ROUND(?, 4) AS applied_prime_rate,
                    ROUND(?, 2) AS max_walk_run_km,
                    ROUND(?, 2) AS max_bike_micro_km,
                    CAST(? AS BIGINT) AS min_annual_activities,
                    CAST(? AS BIGINT) AS wellbeing_days_per_employee
                FROM gold_fact_emp;
                """,
                [
                    ref_date_iso,
                    distinct_sports_cnt,
                    prime_rate,
                    max_walk_km,
                    max_bike_km,
                    min_acts,
                    granted_days,
                ],
            ).fetchall()

            kpi_sql_df = pl.DataFrame(
                kpi_rows,
                schema=_without_computed_at(GOLD_EXECUTIVE_KPIS_SCHEMA),
                orient="row",
            )
        finally:
            con.close()

    return {
        "fact_employee_benefits": _attach_computed_at(fact_sql_df, now_utc),
        "agg_benefits_by_bu": _attach_computed_at(bu_sql_df, now_utc),
        "agg_sports_monthly": _attach_computed_at(sports_sql_df, now_utc),
        "kpi_executive_snapshot": _attach_computed_at(kpi_sql_df, now_utc),
    }


def sync_gold_to_postgres(
    conn: pg8000.dbapi.Connection,
    gold_tables: dict[str, pl.DataFrame],
    rules: BusinessRules,
    *,
    bi_reader_role: str = "sds_bi_reader",
    computed_at: datetime | None = None,
) -> dict[str, int]:
    """Synchronise de maniere transactionnelle les tables Gold et parametres dans gold_mart."""
    if not _SAFE_IDENT_RE.match(bi_reader_role):
        raise ValueError(f"Identifiant de role SQL invalide : {bi_reader_role!r}")

    now_utc = computed_at.astimezone(UTC) if computed_at else datetime.now(UTC)
    prime_rate = float(rules.mobility_bonus.annual_gross_salary_rate)
    max_walk_km = float(
        rules.mobility_bonus.eligible_modes["Marche/running"].max_distance_km
    )
    max_bike_km = float(
        rules.mobility_bonus.eligible_modes["Vélo/Trottinette/Autres"].max_distance_km
    )
    min_acts = int(rules.wellbeing_days.min_activities_per_year)
    granted_days = int(rules.wellbeing_days.granted_days_count)

    cur = conn.cursor()
    try:
        for ddl_stmt in (s.strip() for s in GOLD_MART_DDL.split(";") if s.strip()):
            cur.execute(f"{ddl_stmt};")

        cur.execute(f"GRANT USAGE ON SCHEMA gold_mart TO {bi_reader_role};")
        cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA gold_mart TO {bi_reader_role};")

        cur.execute(
            """
            INSERT INTO gold_mart.dim_business_parameters (
                parameter_set_id,
                prime_rate,
                max_walk_run_km,
                max_bike_micro_km,
                min_annual_activities,
                wellbeing_days_granted,
                last_recomputed_at
            )
            VALUES (1, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (parameter_set_id) DO UPDATE SET
                prime_rate = EXCLUDED.prime_rate,
                max_walk_run_km = EXCLUDED.max_walk_run_km,
                max_bike_micro_km = EXCLUDED.max_bike_micro_km,
                min_annual_activities = EXCLUDED.min_annual_activities,
                wellbeing_days_granted = EXCLUDED.wellbeing_days_granted,
                last_recomputed_at = EXCLUDED.last_recomputed_at;
            """,
            (prime_rate, max_walk_km, max_bike_km, min_acts, granted_days, now_utc),
        )

        cur.execute(
            """
            TRUNCATE TABLE
                gold_mart.fact_employee_benefits,
                gold_mart.agg_benefits_by_bu,
                gold_mart.agg_sports_monthly,
                gold_mart.kpi_executive_snapshot;
            """
        )

        fact_df = gold_tables["fact_employee_benefits"]
        if fact_df.height > 0:
            fact_json = json.dumps(
                fact_df.to_dicts(), default=_pg_json_default, ensure_ascii=False
            )
            cur.execute(
                """
                INSERT INTO gold_mart.fact_employee_benefits (
                    employee_key, business_unit, contract_type, birth_year, hire_date,
                    seniority_years, annual_gross_salary_eur, declared_commute_mode,
                    computed_distance_km, effective_max_distance_km, is_commute_anomaly,
                    is_mobility_bonus_eligible, applied_prime_rate, mobility_bonus_eur,
                    valid_activities_12m, distinct_sports_12m, primary_sport,
                    total_elapsed_hours_12m, total_distance_km_12m, is_wellbeing_eligible,
                    wellbeing_days_granted, wellbeing_imputed_value_eur,
                    reference_date, computed_at
                )
                SELECT
                    r.employee_key, r.business_unit, r.contract_type, r.birth_year, r.hire_date,
                    r.seniority_years, r.annual_gross_salary_eur, r.declared_commute_mode,
                    r.computed_distance_km, r.effective_max_distance_km, r.is_commute_anomaly,
                    r.is_mobility_bonus_eligible, r.applied_prime_rate, r.mobility_bonus_eur,
                    r.valid_activities_12m, r.distinct_sports_12m, r.primary_sport,
                    r.total_elapsed_hours_12m, r.total_distance_km_12m, r.is_wellbeing_eligible,
                    r.wellbeing_days_granted, r.wellbeing_imputed_value_eur,
                    r.reference_date, r.computed_at
                FROM jsonb_to_recordset(CAST(%s AS jsonb)) AS r(
                    employee_key VARCHAR(40),
                    business_unit VARCHAR(60),
                    contract_type VARCHAR(20),
                    birth_year INTEGER,
                    hire_date DATE,
                    seniority_years NUMERIC(6, 2),
                    annual_gross_salary_eur NUMERIC(12, 2),
                    declared_commute_mode VARCHAR(80),
                    computed_distance_km NUMERIC(8, 3),
                    effective_max_distance_km NUMERIC(6, 2),
                    is_commute_anomaly BOOLEAN,
                    is_mobility_bonus_eligible BOOLEAN,
                    applied_prime_rate NUMERIC(5, 4),
                    mobility_bonus_eur NUMERIC(12, 2),
                    valid_activities_12m INTEGER,
                    distinct_sports_12m INTEGER,
                    primary_sport VARCHAR(80),
                    total_elapsed_hours_12m NUMERIC(10, 2),
                    total_distance_km_12m NUMERIC(10, 2),
                    is_wellbeing_eligible BOOLEAN,
                    wellbeing_days_granted INTEGER,
                    wellbeing_imputed_value_eur NUMERIC(12, 2),
                    reference_date DATE,
                    computed_at TIMESTAMPTZ
                );
                """,
                (fact_json,),
            )

        bu_df = gold_tables["agg_benefits_by_bu"]
        if bu_df.height > 0:
            bu_json = json.dumps(
                bu_df.to_dicts(), default=_pg_json_default, ensure_ascii=False
            )
            cur.execute(
                """
                INSERT INTO gold_mart.agg_benefits_by_bu (
                    business_unit, headcount, active_sport_employees,
                    sport_participation_rate_pct, mobility_eligible_count,
                    commute_anomaly_count, wellbeing_eligible_count,
                    total_payroll_eur, total_mobility_bonus_eur,
                    avg_mobility_bonus_eligible_eur, total_wellbeing_days,
                    total_activities_12m, total_distance_km_12m,
                    applied_prime_rate, computed_at
                )
                SELECT
                    r.business_unit, r.headcount, r.active_sport_employees,
                    r.sport_participation_rate_pct, r.mobility_eligible_count,
                    r.commute_anomaly_count, r.wellbeing_eligible_count,
                    r.total_payroll_eur, r.total_mobility_bonus_eur,
                    r.avg_mobility_bonus_eligible_eur, r.total_wellbeing_days,
                    r.total_activities_12m, r.total_distance_km_12m,
                    r.applied_prime_rate, r.computed_at
                FROM jsonb_to_recordset(CAST(%s AS jsonb)) AS r(
                    business_unit VARCHAR(60),
                    headcount INTEGER,
                    active_sport_employees INTEGER,
                    sport_participation_rate_pct NUMERIC(6, 2),
                    mobility_eligible_count INTEGER,
                    commute_anomaly_count INTEGER,
                    wellbeing_eligible_count INTEGER,
                    total_payroll_eur NUMERIC(14, 2),
                    total_mobility_bonus_eur NUMERIC(14, 2),
                    avg_mobility_bonus_eligible_eur NUMERIC(12, 2),
                    total_wellbeing_days INTEGER,
                    total_activities_12m INTEGER,
                    total_distance_km_12m NUMERIC(12, 2),
                    applied_prime_rate NUMERIC(5, 4),
                    computed_at TIMESTAMPTZ
                );
                """,
                (bu_json,),
            )

        sports_df = gold_tables["agg_sports_monthly"]
        if sports_df.height > 0:
            sports_json = json.dumps(
                sports_df.to_dicts(), default=_pg_json_default, ensure_ascii=False
            )
            cur.execute(
                """
                INSERT INTO gold_mart.agg_sports_monthly (
                    activity_month, sport_type, sport_category,
                    activities_count, active_employees_count,
                    total_elapsed_hours, total_distance_km,
                    commented_sessions_count, computed_at
                )
                SELECT
                    r.activity_month, r.sport_type, r.sport_category,
                    r.activities_count, r.active_employees_count,
                    r.total_elapsed_hours, r.total_distance_km,
                    r.commented_sessions_count, r.computed_at
                FROM jsonb_to_recordset(CAST(%s AS jsonb)) AS r(
                    activity_month VARCHAR(7),
                    sport_type VARCHAR(80),
                    sport_category VARCHAR(40),
                    activities_count INTEGER,
                    active_employees_count INTEGER,
                    total_elapsed_hours NUMERIC(10, 2),
                    total_distance_km NUMERIC(12, 2),
                    commented_sessions_count INTEGER,
                    computed_at TIMESTAMPTZ
                );
                """,
                (sports_json,),
            )

        kpi_df = gold_tables["kpi_executive_snapshot"]
        if kpi_df.height > 0:
            kpi_json = json.dumps(
                kpi_df.to_dicts(), default=_pg_json_default, ensure_ascii=False
            )
            cur.execute(
                """
                INSERT INTO gold_mart.kpi_executive_snapshot (
                    snapshot_id, reference_date, total_employees,
                    active_sport_employees, sport_participation_rate_pct,
                    mobility_eligible_employees, mobility_eligibility_rate_pct,
                    commute_anomalies_count, wellbeing_eligible_employees,
                    wellbeing_eligibility_rate_pct, total_wellbeing_days_granted,
                    total_payroll_eur, total_mobility_bonus_eur,
                    mobility_cost_share_of_payroll_pct, total_valid_activities_12m,
                    distinct_sports_count, applied_prime_rate,
                    max_walk_run_km, max_bike_micro_km,
                    min_annual_activities, wellbeing_days_per_employee, computed_at
                )
                SELECT
                    r.snapshot_id, r.reference_date, r.total_employees,
                    r.active_sport_employees, r.sport_participation_rate_pct,
                    r.mobility_eligible_employees, r.mobility_eligibility_rate_pct,
                    r.commute_anomalies_count, r.wellbeing_eligible_employees,
                    r.wellbeing_eligibility_rate_pct, r.total_wellbeing_days_granted,
                    r.total_payroll_eur, r.total_mobility_bonus_eur,
                    r.mobility_cost_share_of_payroll_pct, r.total_valid_activities_12m,
                    r.distinct_sports_count, r.applied_prime_rate,
                    r.max_walk_run_km, r.max_bike_micro_km,
                    r.min_annual_activities, r.wellbeing_days_per_employee, r.computed_at
                FROM jsonb_to_recordset(CAST(%s AS jsonb)) AS r(
                    snapshot_id INTEGER,
                    reference_date DATE,
                    total_employees INTEGER,
                    active_sport_employees INTEGER,
                    sport_participation_rate_pct NUMERIC(6, 2),
                    mobility_eligible_employees INTEGER,
                    mobility_eligibility_rate_pct NUMERIC(6, 2),
                    commute_anomalies_count INTEGER,
                    wellbeing_eligible_employees INTEGER,
                    wellbeing_eligibility_rate_pct NUMERIC(6, 2),
                    total_wellbeing_days_granted INTEGER,
                    total_payroll_eur NUMERIC(14, 2),
                    total_mobility_bonus_eur NUMERIC(14, 2),
                    mobility_cost_share_of_payroll_pct NUMERIC(6, 3),
                    total_valid_activities_12m INTEGER,
                    distinct_sports_count INTEGER,
                    applied_prime_rate NUMERIC(5, 4),
                    max_walk_run_km NUMERIC(6, 2),
                    max_bike_micro_km NUMERIC(6, 2),
                    min_annual_activities INTEGER,
                    wellbeing_days_per_employee INTEGER,
                    computed_at TIMESTAMPTZ
                );
                """,
                (kpi_json,),
            )

        conn.commit()
        return {name: df.height for name, df in gold_tables.items()}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _record_gold_run(
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
                json.dumps(details, default=_pg_json_default, ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        cur.close()


def run_gold_pipeline(
    settings: EnvSettings | None = None,
    rules: BusinessRules | None = None,
    *,
    pg_conn: pg8000.dbapi.Connection | None = None,
    uri_overrides: dict[str, str] | None = None,
    storage_options: Any = _DEFAULT_STORAGE,
    as_of_date: date | None = None,
    sync_postgres: bool = True,
    record_monitoring: bool = True,
) -> dict[str, Any]:
    """Calcule la couche Gold depuis Silver Delta Lake et synchronise gold_mart."""
    cfg = settings or EnvSettings()
    biz_rules = rules or load_business_rules()
    started_at = datetime.now(UTC)
    t0 = time.perf_counter()

    resolved_storage: dict[str, str] | None = (
        cfg.delta_storage_options() if storage_options is _DEFAULT_STORAGE else storage_options
    )
    uris = {
        "silver_employees": cfg.lakehouse_table_uri("silver", cfg.silver_employees_table),
        "silver_activities": cfg.lakehouse_table_uri("silver", cfg.silver_activities_table),
        "fact_employee_benefits": cfg.lakehouse_table_uri(
            "gold", cfg.gold_employee_benefits_table
        ),
        "agg_benefits_by_bu": cfg.lakehouse_table_uri("gold", cfg.gold_bu_summary_table),
        "agg_sports_monthly": cfg.lakehouse_table_uri(
            "gold", cfg.gold_sports_monthly_table
        ),
        "kpi_executive_snapshot": cfg.lakehouse_table_uri(
            "gold", cfg.gold_executive_kpis_table
        ),
    }
    if uri_overrides:
        uris.update(uri_overrides)

    need_db = sync_postgres or record_monitoring
    owns_conn = pg_conn is None and need_db
    conn = (
        pg8000.dbapi.connect(**cfg.pg8000_connect_kwargs("pipeline"))
        if owns_conn
        else pg_conn
    )

    try:
        silver_emp_df = read_delta_table(uris["silver_employees"], resolved_storage)
        silver_act_df = read_delta_table(uris["silver_activities"], resolved_storage)

        t_compute_start = time.perf_counter()
        gold_tables = compute_gold_tables(
            silver_emp_df,
            silver_act_df,
            biz_rules,
            as_of_date=as_of_date,
            computed_at=started_at,
        )
        compute_engine_ms = int((time.perf_counter() - t_compute_start) * 1000)

        delta_versions: dict[str, int] = {}
        for table_key, table_df in gold_tables.items():
            delta_versions[table_key] = write_delta_table(
                uris[table_key], table_df, resolved_storage
            )

        pg_synced_rows: dict[str, int] = {}
        if sync_postgres and conn is not None:
            pg_synced_rows = sync_gold_to_postgres(
                conn,
                gold_tables,
                biz_rules,
                bi_reader_role=cfg.sds_bi_reader_user,
                computed_at=started_at,
            )

        total_ms = int((time.perf_counter() - t0) * 1000)
        kpi_dict = gold_tables["kpi_executive_snapshot"].to_dicts()[0]
        total_rows = sum(df.height for df in gold_tables.values())
        anomalies = int(kpi_dict["commute_anomalies_count"])

        summary = {
            "compute_engine_ms": compute_engine_ms,
            "total_pipeline_ms": total_ms,
            "delta_versions": delta_versions,
            "postgres_synced_rows": pg_synced_rows,
            "kpis": {
                "reference_date": str(kpi_dict["reference_date"]),
                "total_employees": int(kpi_dict["total_employees"]),
                "active_sport_employees": int(kpi_dict["active_sport_employees"]),
                "mobility_eligible_employees": int(kpi_dict["mobility_eligible_employees"]),
                "commute_anomalies_count": anomalies,
                "wellbeing_eligible_employees": int(kpi_dict["wellbeing_eligible_employees"]),
                "total_wellbeing_days_granted": int(kpi_dict["total_wellbeing_days_granted"]),
                "total_payroll_eur": float(kpi_dict["total_payroll_eur"]),
                "total_mobility_bonus_eur": float(kpi_dict["total_mobility_bonus_eur"]),
                "applied_prime_rate": float(kpi_dict["applied_prime_rate"]),
                "max_walk_run_km": float(kpi_dict["max_walk_run_km"]),
                "max_bike_micro_km": float(kpi_dict["max_bike_micro_km"]),
                "min_annual_activities": int(kpi_dict["min_annual_activities"]),
                "wellbeing_days_per_employee": int(kpi_dict["wellbeing_days_per_employee"]),
            },
        }

        if record_monitoring and conn is not None:
            _record_gold_run(
                conn,
                started_at=started_at,
                status="SUCCESS",
                rows_processed=total_rows,
                anomalies_detected=anomalies,
                details=summary,
            )
        return summary
    except Exception as exc:
        if record_monitoring and conn is not None:
            try:
                _record_gold_run(
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
                logger.warning("Echec de journalisation de l'erreur Gold dans pipeline_runs")
        raise
    finally:
        if owns_conn and conn is not None:
            conn.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    summary = run_gold_pipeline()
    logger.info("Synchronisation Gold terminee : %s", json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()