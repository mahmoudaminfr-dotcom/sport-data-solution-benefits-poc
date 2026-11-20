from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from pydantic import SecretStr

from scripts.recompute_gold_kpis import build_effective_rules, parse_args
from src.processing.lakehouse_bronze_silver import (
    BRONZE_ACTIVITIES_SCHEMA,
    BRONZE_EMPLOYEES_SCHEMA,
    pseudonymize_employee_id,
    read_delta_table,
    run_bronze_silver_pipeline,
    sanitize_anomaly_reason,
    transform_silver_activities,
    transform_silver_employees,
)
from src.processing.lakehouse_gold import (
    compute_gold_tables,
    run_gold_pipeline,
    subtract_calendar_months,
    sync_gold_to_postgres,
)
from src.settings import EnvSettings, load_business_rules


class FakeCursor:
    def __init__(self, responses: list[list[tuple[Any, ...]]]) -> None:
        self._responses = list(responses)
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))

    def fetchall(self) -> list[tuple[Any, ...]]:
        if not self._responses:
            return []
        return self._responses.pop(0)

    def close(self) -> None:
        pass


class FakeConnection:
    def __init__(self, responses: list[list[tuple[Any, ...]]]) -> None:
        self.cursor_instance = FakeCursor(responses)
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


def test_pseudonymization_and_pii_sanitization() -> None:
    key_a1 = pseudonymize_employee_id(1024, "unit_salt_secret")
    key_a2 = pseudonymize_employee_id(1024, "unit_salt_secret")
    key_b = pseudonymize_employee_id(1024, "different_salt")

    assert key_a1 == key_a2
    assert key_a1.startswith("emp_")
    assert len(key_a1) == 20
    assert key_a1 != key_b

    with pytest.raises(ValueError, match="sel de pseudonymisation"):
        pseudonymize_employee_id(1024, "   ")

    masked = sanitize_anomaly_reason(
        "Echec geocodage pour 12 Rue de la Loge 34000 Montpellier",
        "12 Rue de la Loge 34000 Montpellier",
    )
    assert masked == "Echec geocodage pour [ADRESSE_MASQUEE_RGPD]"
    assert sanitize_anomaly_reason(None, None) is None


def test_silver_cleaning_and_quality_flags() -> None:
    rules = load_business_rules()
    now_utc = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)

    bronze_emp = pl.DataFrame(
        [
            {
                "employee_key": "emp_0001",
                "birth_year": 1990,
                "business_unit": " IT ",
                "hire_date": date(2024, 6, 1),
                "annual_gross_salary_eur": 40000.0,
                "contract_type": " CDI ",
                "paid_leave_days": 25,
                "declared_commute_mode": "Vélo/Trottinette/Autres",
                "computed_distance_km": 12.4,
                "max_allowed_distance_km": 25.0,
                "is_commute_anomaly": False,
                "is_mobility_bonus_eligible": True,
                "anomaly_reason": None,
                "source_updated_at": now_utc,
                "ingested_at": now_utc,
            }
        ],
        schema=BRONZE_EMPLOYEES_SCHEMA,
    )

    bronze_act = pl.DataFrame(
        [
            {
                "activity_id": 1,
                "employee_key": "emp_0001",
                "sport_type": "Runing",
                "activity_start": datetime(2026, 10, 10, 12, 0, tzinfo=UTC),
                "activity_end": datetime(2026, 10, 10, 12, 45, tzinfo=UTC),
                "elapsed_seconds": 2700,
                "distance_m": 8500,
                "has_comment": True,
                "ingestion_mode": "historical_seed",
                "created_at": now_utc,
                "ingested_at": now_utc,
            },
            {
                "activity_id": 2,
                "employee_key": "emp_0001",
                "sport_type": "Badminton",
                "activity_start": datetime(2026, 10, 12, 18, 0, tzinfo=UTC),
                "activity_end": datetime(2026, 10, 12, 19, 0, tzinfo=UTC),
                "elapsed_seconds": 3600,
                "distance_m": 1500,
                "has_comment": False,
                "ingestion_mode": "historical_seed",
                "created_at": now_utc,
                "ingested_at": now_utc,
            },
            {
                "activity_id": 3,
                "employee_key": "emp_0001",
                "sport_type": "Natation",
                "activity_start": datetime(2023, 5, 1, 8, 0, tzinfo=UTC),
                "activity_end": datetime(2023, 5, 1, 8, 30, tzinfo=UTC),
                "elapsed_seconds": 1800,
                "distance_m": 1200,
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
    assert silver_emp["business_unit"][0] == "IT"
    assert silver_emp["contract_type"][0] == "CDI"
    assert silver_emp["is_salary_valid"][0] is True

    silver_act = transform_silver_activities(
        bronze_act, silver_emp, rules, processed_at=now_utc
    )
    by_id = {row["activity_id"]: row for row in silver_act.to_dicts()}

    assert by_id[1]["sport_type"] == "Course à pied"
    assert by_id[1]["is_valid_activity"] is True
    assert by_id[2]["is_distance_coherent"] is False
    assert by_id[2]["is_valid_activity"] is False
    assert by_id[3]["is_after_hire_date"] is False
    assert by_id[3]["is_valid_activity"] is False


def test_offline_medallion_end_to_end_and_parametric_recompute(tmp_path: Path) -> None:
    assert subtract_calendar_months(date(2028, 2, 29), 12) == date(2027, 2, 28)
    with pytest.raises(ValueError, match="Nombre de mois invalide"):
        subtract_calendar_months(date(2026, 11, 18), -1)

    rules = load_business_rules()
    cfg = EnvSettings(
        _env_file=None,
        sds_pii_hash_salt=SecretStr("offline_medallion_salt_32_chars_ok"),
    )
    now_utc = datetime(2026, 11, 18, 9, 0, tzinfo=UTC)

    emp_rows = [
        (
            10,
            date(1988, 4, 12),
            "Marketing",
            date(2022, 1, 10),
            50000.00,
            "CDI",
            25,
            "10 Rue Foch 34000 Montpellier",
            "Vélo/Trottinette/Autres",
            18.5,
            25.0,
            False,
            True,
            None,
            now_utc,
        ),
        (
            20,
            date(1994, 9, 3),
            "Marketing",
            date(2023, 3, 15),
            36000.00,
            "CDI",
            25,
            "Adresse incomplete sans CP",
            "Marche/running",
            None,
            15.0,
            True,
            False,
            "Distance domicile-travail indisponible pour Adresse incomplete sans CP",
            now_utc,
        ),
        (
            30,
            date(1985, 11, 22),
            "Finance",
            date(2021, 9, 1),
            60000.00,
            "CDI",
            25,
            "5 Avenue de la Mer 34970 Lattes",
            "Transports en commun",
            6.0,
            None,
            False,
            False,
            None,
            now_utc,
        ),
    ]

    act_rows = [
        (
            101,
            10,
            "Runing",
            datetime(2026, 10, 5, 12, 0, tzinfo=UTC),
            datetime(2026, 10, 5, 12, 50, tzinfo=UTC),
            3000,
            9000,
            "Sortie rapide",
            "historical_seed",
            now_utc,
        ),
        (
            102,
            10,
            "Badminton",
            datetime(2026, 11, 2, 18, 0, tzinfo=UTC),
            datetime(2026, 11, 2, 19, 0, tzinfo=UTC),
            3600,
            None,
            None,
            "historical_seed",
            now_utc,
        ),
        (
            103,
            30,
            "Natation",
            datetime(2026, 11, 10, 7, 30, tzinfo=UTC),
            datetime(2026, 11, 10, 8, 15, tzinfo=UTC),
            2700,
            2000,
            "Bassin olympique",
            "historical_seed",
            now_utc,
        ),
    ]

    uris = {
        "bronze_employees": str(tmp_path / "bronze_emp"),
        "bronze_activities": str(tmp_path / "bronze_act"),
        "silver_employees": str(tmp_path / "silver_emp"),
        "silver_activities": str(tmp_path / "silver_act"),
        "fact_employee_benefits": str(tmp_path / "gold_fact"),
        "agg_benefits_by_bu": str(tmp_path / "gold_bu"),
        "agg_sports_monthly": str(tmp_path / "gold_sports"),
        "kpi_executive_snapshot": str(tmp_path / "gold_kpi"),
    }

    extract_conn = FakeConnection([emp_rows, act_rows])
    bs_report = run_bronze_silver_pipeline(
        cfg,
        rules,
        pg_conn=extract_conn,  # type: ignore[arg-type]
        uri_overrides=uris,
        storage_options=None,
        as_of_date=date(2026, 11, 18),
    )
    assert bs_report["silver"]["employees_rows"] == 3
    assert bs_report["silver"]["valid_activities_rows"] == 3

    sync_conn = FakeConnection([])
    nominal_rules = rules.with_overrides(min_activities_per_year=2)
    gold_nominal = run_gold_pipeline(
        cfg,
        nominal_rules,
        pg_conn=sync_conn,  # type: ignore[arg-type]
        uri_overrides=uris,
        storage_options=None,
        as_of_date=date(2026, 11, 18),
    )
    kpis_nom = gold_nominal["kpis"]
    assert kpis_nom["total_employees"] == 3
    assert kpis_nom["mobility_eligible_employees"] == 1
    assert kpis_nom["commute_anomalies_count"] == 1
    assert kpis_nom["total_mobility_bonus_eur"] == 2500.00
    assert kpis_nom["wellbeing_eligible_employees"] == 1
    assert kpis_nom["total_wellbeing_days_granted"] == 5
    assert sync_conn.commits == 2

    cli_args = parse_args(
        [
            "--prime-rate",
            "0.08",
            "--max-bike-km",
            "15.0",
            "--min-activities",
            "1",
            "--wellbeing-days",
            "6",
        ]
    )
    sim_rules = build_effective_rules(cli_args, rules)
    reset_args = parse_args(["--prime-rate", "0.12", "--reset-defaults"])
    assert (
        build_effective_rules(reset_args, rules).mobility_bonus.annual_gross_salary_rate
        == 0.05
    )

    silver_emp_df = read_delta_table(uris["silver_employees"])
    silver_act_df = read_delta_table(uris["silver_activities"])
    sim_gold = compute_gold_tables(
        silver_emp_df,
        silver_act_df,
        sim_rules,
        as_of_date=date(2026, 11, 18),
    )
    sim_kpi = sim_gold["kpi_executive_snapshot"].to_dicts()[0]
    assert sim_kpi["mobility_eligible_employees"] == 0
    assert sim_kpi["commute_anomalies_count"] == 2
    assert sim_kpi["total_mobility_bonus_eur"] == 0.0
    assert sim_kpi["wellbeing_eligible_employees"] == 2
    assert sim_kpi["total_wellbeing_days_granted"] == 12

    monthly_by_sport = {
        row["sport_type"]: row for row in sim_gold["agg_sports_monthly"].to_dicts()
    }
    assert monthly_by_sport["Badminton"]["total_distance_km"] is None
    assert monthly_by_sport["Course à pied"]["total_distance_km"] == 9.0

    with pytest.raises(ValueError, match="Identifiant de role SQL invalide"):
        sync_gold_to_postgres(
            sync_conn,  # type: ignore[arg-type]
            sim_gold,
            rules,
            bi_reader_role="invalid-role;DROP",
        )