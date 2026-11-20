import os
from datetime import UTC, date, datetime
from typing import Any

import polars as pl
import pytest
from pydantic import SecretStr

from src.processing.lakehouse_bronze_silver import (
    BRONZE_ACTIVITIES_SCHEMA,
    BRONZE_EMPLOYEES_SCHEMA,
    transform_silver_activities,
    transform_silver_employees,
)
from src.processing.lakehouse_gold import compute_gold_tables
from src.quality.soda_runner import DataQualityGateError, parse_args, run_quality_gate
from src.settings import EnvSettings, load_business_rules


class FakeCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return []

    def close(self) -> None:
        pass


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        pass


def _build_valid_medallion_datasets() -> dict[str, pl.DataFrame]:
    rules = load_business_rules()
    now_utc = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)

    hr_df = pl.DataFrame(
        [
            {
                "employee_key": "emp_alpha",
                "business_unit": "Finance",
                "annual_gross_salary_eur": 48000.0,
                "declared_commute_mode": "Vélo/Trottinette/Autres",
                "computed_distance_km": 14.2,
                "max_allowed_distance_km": 25.0,
                "is_commute_anomaly": False,
                "is_mobility_bonus_eligible": True,
            },
            {
                "employee_key": "emp_beta",
                "business_unit": "Marketing",
                "annual_gross_salary_eur": 39000.0,
                "declared_commute_mode": "Marche/running",
                "computed_distance_km": 19.8,
                "max_allowed_distance_km": 15.0,
                "is_commute_anomaly": True,
                "is_mobility_bonus_eligible": False,
            },
        ]
    )

    bronze_emp = pl.DataFrame(
        [
            {
                "employee_key": "emp_alpha",
                "birth_year": 1989,
                "business_unit": "Finance",
                "hire_date": date(2022, 2, 1),
                "annual_gross_salary_eur": 48000.0,
                "contract_type": "CDI",
                "paid_leave_days": 25,
                "declared_commute_mode": "Vélo/Trottinette/Autres",
                "computed_distance_km": 14.2,
                "max_allowed_distance_km": 25.0,
                "is_commute_anomaly": False,
                "is_mobility_bonus_eligible": True,
                "anomaly_reason": None,
                "source_updated_at": now_utc,
                "ingested_at": now_utc,
            },
            {
                "employee_key": "emp_beta",
                "birth_year": 1993,
                "business_unit": "Marketing",
                "hire_date": date(2023, 5, 10),
                "annual_gross_salary_eur": 39000.0,
                "contract_type": "CDI",
                "paid_leave_days": 25,
                "declared_commute_mode": "Marche/running",
                "computed_distance_km": 19.8,
                "max_allowed_distance_km": 15.0,
                "is_commute_anomaly": True,
                "is_mobility_bonus_eligible": False,
                "anomaly_reason": "Distance superieure au plafond",
                "source_updated_at": now_utc,
                "ingested_at": now_utc,
            },
        ],
        schema=BRONZE_EMPLOYEES_SCHEMA,
    )

    bronze_act = pl.DataFrame(
        [
            {
                "activity_id": 501,
                "employee_key": "emp_alpha",
                "sport_type": "Runing",
                "activity_start": datetime(2026, 10, 15, 12, 0, tzinfo=UTC),
                "activity_end": datetime(2026, 10, 15, 12, 50, tzinfo=UTC),
                "elapsed_seconds": 3000,
                "distance_m": 9200,
                "has_comment": True,
                "ingestion_mode": "historical_seed",
                "created_at": now_utc,
                "ingested_at": now_utc,
            },
            {
                "activity_id": 502,
                "employee_key": "emp_beta",
                "sport_type": "Badminton",
                "activity_start": datetime(2026, 11, 5, 18, 0, tzinfo=UTC),
                "activity_end": datetime(2026, 11, 5, 19, 0, tzinfo=UTC),
                "elapsed_seconds": 3600,
                "distance_m": None,
                "has_comment": False,
                "ingestion_mode": "historical_seed",
                "created_at": now_utc,
                "ingested_at": now_utc,
            },
        ],
        schema=BRONZE_ACTIVITIES_SCHEMA,
    )

    silver_emp = transform_silver_employees(
        bronze_emp, as_of_date=date(2026, 11, 20), processed_at=now_utc
    )
    silver_act = transform_silver_activities(
        bronze_act, silver_emp, rules, processed_at=now_utc
    )
    gold_map = compute_gold_tables(
        silver_emp,
        silver_act,
        rules.with_overrides(min_activities_per_year=1),
        as_of_date=date(2026, 11, 20),
        computed_at=now_utc,
    )

    return {
        "hr_employees_audit": hr_df,
        "bronze_employees_audit": bronze_emp,
        "bronze_sport_activities": bronze_act,
        "silver_employees_cleansed": silver_emp,
        "silver_sport_activities": silver_act,
        "gold_fact_employee_benefits": gold_map["fact_employee_benefits"],
        "gold_agg_benefits_by_bu": gold_map["agg_benefits_by_bu"],
        "gold_kpi_executive_snapshot": gold_map["kpi_executive_snapshot"],
    }


def test_soda_quality_gate_passes_on_compliant_medallion() -> None:
    cfg = EnvSettings(_env_file=None, sds_pii_hash_salt=SecretStr("unit_test_salt_32"))
    fake_conn = FakeConnection()
    datasets = _build_valid_medallion_datasets()
    env_before = os.environ.copy()

    report = run_quality_gate(
        cfg,
        pg_conn=fake_conn,  # type: ignore[arg-type]
        tables_override=datasets,
        record_monitoring=True,
        raise_on_failure=True,
    )

    assert os.environ == env_before
    assert report["status"] == "SUCCESS"
    assert report["total_checks"] == 22
    assert report["passed_checks"] == 22
    assert report["failed_checks"] == 0
    assert fake_conn.commits == 1
    assert len(fake_conn.cursor_instance.executed) == 2


def test_soda_quality_gate_catches_injected_corruptions() -> None:
    assert parse_args(["--no-fail-exit"]).no_fail_exit is True

    cfg = EnvSettings(_env_file=None, sds_pii_hash_salt=SecretStr("unit_test_salt_32"))
    fake_conn = FakeConnection()
    datasets = _build_valid_medallion_datasets()

    datasets["hr_employees_audit"] = datasets["hr_employees_audit"].with_columns(
        pl.lit(True).alias("is_mobility_bonus_eligible")
    )
    datasets["silver_sport_activities"] = datasets["silver_sport_activities"].with_columns(
        [
            pl.lit("emp_ghost_orphan").alias("employee_key"),
            pl.lit(-250, dtype=pl.Int64).alias("distance_m"),
            pl.lit("Runing").alias("sport_type"),
        ]
    )
    datasets["gold_kpi_executive_snapshot"] = datasets[
        "gold_kpi_executive_snapshot"
    ].with_columns(pl.lit(999999.99).alias("total_mobility_bonus_eur"))

    with pytest.raises(DataQualityGateError, match="Echec du portail qualite Soda Core"):
        run_quality_gate(
            cfg,
            pg_conn=fake_conn,  # type: ignore[arg-type]
            tables_override=datasets,
            record_monitoring=True,
            raise_on_failure=True,
        )

    assert fake_conn.commits == 1

    non_blocking_report = run_quality_gate(
        cfg,
        pg_conn=fake_conn,  # type: ignore[arg-type]
        tables_override=datasets,
        record_monitoring=False,
        raise_on_failure=False,
    )
    assert non_blocking_report["status"] == "FAILED"
    failed_set = set(non_blocking_report["failed_check_names"])
    assert "hr_audit_maps_anomalies_excluded_from_bonus" in failed_set
    assert "silver_activities_valid_employee_fk" in failed_set
    assert "silver_activities_non_negative_distance" in failed_set
    assert "silver_activities_normalized_sports_catalog" in failed_set
    assert "gold_kpi_budget_reconciled_with_bu_summary" in failed_set