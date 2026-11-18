import argparse
import io
import json
import logging
import random
import time
import unicodedata
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, Protocol

import openpyxl
import pg8000.dbapi

from src.ingestion.commute_distance import CommuteDistanceResolver
from src.ingestion.minio_loader import build_s3_client, sync_raw_referentials
from src.settings import BusinessRules, EnvSettings, load_business_rules

logger = logging.getLogger(__name__)

PIPELINE_NAME = "postgres_oltp_ingestion"
INSERT_BATCH_SIZE = 200

IngestionMode = Literal["historical_seed", "live_event"]

SPORT_PROFILES: dict[str, tuple[int, int, float, float]] = {
    "Course à pied": (1600, 4500, 2.55, 3.65),
    "Randonnée": (5400, 14400, 1.05, 1.45),
    "Triathlon": (4500, 10800, 4.10, 5.90),
    "Natation": (1800, 3900, 0.70, 1.15),
    "Voile": (5400, 12600, 2.20, 4.10),
    "Équitation": (2700, 6600, 1.80, 2.85),
    "Cyclisme": (2700, 9000, 5.40, 8.10),
    "Escalade": (3600, 7200, 0.0, 0.0),
    "Tennis": (3000, 6600, 0.0, 0.0),
    "Badminton": (2700, 5400, 0.0, 0.0),
    "Tennis de table": (2100, 4500, 0.0, 0.0),
    "Football": (3600, 6300, 0.0, 0.0),
    "Rugby": (3900, 6600, 0.0, 0.0),
    "Basketball": (3000, 5700, 0.0, 0.0),
    "Judo": (3000, 5400, 0.0, 0.0),
    "Boxe": (2700, 5100, 0.0, 0.0),
}

ACTIVITY_COMMENTS: tuple[str, ...] = (
    "Séance régulière dans de bonnes conditions météo.",
    "Sortie matinale avant la prise de poste.",
    "Entraînement collectif avec les collègues de l'équipe.",
    "Séance d'endurance à allure maîtrisée.",
    "Reprise progressive avec de très bonnes sensations.",
    "Sortie de fin de journée sous le soleil.",
    "Séance soutenue, objectif hebdomadaire atteint.",
)


class S3ObjectReaderProtocol(Protocol):
    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]: ...


@dataclass(frozen=True)
class EmployeeAuditRecord:
    employee_id: int
    last_name: str
    first_name: str
    birth_date: date
    business_unit: str
    hire_date: date
    annual_gross_salary_eur: Decimal
    contract_type: str
    paid_leave_days: int
    home_address: str
    declared_commute_mode: str
    computed_distance_km: float | None
    max_allowed_distance_km: float | None
    is_commute_anomaly: bool
    is_mobility_bonus_eligible: bool
    anomaly_reason: str | None


@dataclass(frozen=True)
class SportActivityRecord:
    employee_id: int
    employee_full_name: str
    sport_type: str
    activity_start: datetime
    activity_end: datetime
    elapsed_seconds: int
    distance_m: int | None
    comment: str | None
    ingestion_mode: IngestionMode


@dataclass(frozen=True)
class PostgresIngestionSummary:
    run_id: str
    employees_upserted: int
    eligible_mobility_count: int
    commute_anomalies_count: int
    active_sport_declared_count: int
    seeded_activities_inserted: int
    skipped_existing_seed: bool
    live_events_inserted: int


def _norm_str(value: object) -> str:
    return unicodedata.normalize("NFC", " ".join(str(value).strip().split()))


def _parse_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(_norm_str(value)[:10])


def read_xlsx_records_from_bytes(payload: bytes) -> list[dict[str, Any]]:
    wb = openpyxl.load_workbook(io.BytesIO(payload), read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if not rows:
        return []

    headers = [
        _norm_str(col) if col is not None else f"col_{idx}"
        for idx, col in enumerate(rows[0])
    ]
    records: list[dict[str, Any]] = []
    for row in rows[1:]:
        if any(cell is not None and str(cell).strip() != "" for cell in row):
            records.append(dict(zip(headers, row, strict=False)))
    return records


def _fetch_s3_xlsx_records(
    s3_client: S3ObjectReaderProtocol,
    *,
    bucket: str,
    s3_key: str,
) -> list[dict[str, Any]]:
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    payload = response["Body"].read()
    return read_xlsx_records_from_bytes(payload)


def build_employee_audit_records(
    hr_rows: Sequence[dict[str, Any]],
    resolver: CommuteDistanceResolver,
) -> list[EmployeeAuditRecord]:
    seen_ids: set[int] = set()
    employees: list[EmployeeAuditRecord] = []

    for row in hr_rows:
        emp_id = int(round(float(row["ID salarié"])))
        if emp_id in seen_ids:
            raise ValueError(f"Doublon detecte sur ID salarie RH : {emp_id}")
        seen_ids.add(emp_id)

        home_address = _norm_str(row["Adresse du domicile"])
        declared_mode = _norm_str(row["Moyen de déplacement"])
        audit = resolver.audit_employee_commute(home_address, declared_mode)
        gross_salary = Decimal(f"{float(row['Salaire brut']):.2f}")

        employees.append(
            EmployeeAuditRecord(
                employee_id=emp_id,
                last_name=_norm_str(row["Nom"]),
                first_name=_norm_str(row["Prénom"]),
                birth_date=_parse_date(row["Date de naissance"]),
                business_unit=_norm_str(row["BU"]),
                hire_date=_parse_date(row["Date d'embauche"]),
                annual_gross_salary_eur=gross_salary,
                contract_type=_norm_str(row["Type de contrat"]),
                paid_leave_days=int(round(float(row["Nombre de jours de CP"]))),
                home_address=home_address,
                declared_commute_mode=audit.declared_mode,
                computed_distance_km=audit.computed_distance_km,
                max_allowed_distance_km=audit.max_allowed_distance_km,
                is_commute_anomaly=audit.is_commute_anomaly,
                is_mobility_bonus_eligible=audit.is_mobility_bonus_eligible,
                anomaly_reason=audit.anomaly_reason,
            )
        )

    employees.sort(key=lambda item: item.employee_id)
    return employees


def extract_declared_sports_map(
    sports_rows: Sequence[dict[str, Any]],
    rules: BusinessRules,
) -> dict[int, str | None]:
    sports_map: dict[int, str | None] = {}
    for row in sports_rows:
        emp_id = int(round(float(row["ID salarié"])))
        if emp_id in sports_map:
            raise ValueError(f"Doublon detecte sur ID salarie Sport : {emp_id}")

        raw_sport = row.get("Pratique d'un sport")
        if raw_sport is None or str(raw_sport).strip() == "":
            sports_map[emp_id] = None
            continue

        normalized = rules.normalize_sport(_norm_str(raw_sport))
        rules.requires_distance(normalized)
        sports_map[emp_id] = normalized
    return sports_map


def _simulate_single_activity(
    emp: EmployeeAuditRecord,
    sport_type: str,
    activity_start: datetime,
    rules: BusinessRules,
    rng: random.Random,
    ingestion_mode: IngestionMode,
) -> SportActivityRecord:
    min_sec, max_sec, min_speed, max_speed = SPORT_PROFILES.get(
        sport_type,
        (2400, 5400, 2.50, 3.50),
    )
    elapsed_seconds = rng.randint(min_sec, max_sec)
    activity_end = activity_start + timedelta(seconds=elapsed_seconds)

    distance_m: int | None = None
    if rules.requires_distance(sport_type):
        speed_mps = rng.uniform(min_speed, max_speed)
        distance_m = max(100, int(round(elapsed_seconds * speed_mps)))

    comment = rng.choice(ACTIVITY_COMMENTS) if rng.random() < 0.75 else None
    return SportActivityRecord(
        employee_id=emp.employee_id,
        employee_full_name=f"{emp.first_name} {emp.last_name}",
        sport_type=sport_type,
        activity_start=activity_start,
        activity_end=activity_end,
        elapsed_seconds=elapsed_seconds,
        distance_m=distance_m,
        comment=comment,
        ingestion_mode=ingestion_mode,
    )


def generate_historical_activities(
    employees: Sequence[EmployeeAuditRecord],
    sports_by_employee: dict[int, str | None],
    rules: BusinessRules,
    *,
    seed: int,
    reference_utc: datetime | None = None,
) -> list[SportActivityRecord]:
    rng = random.Random(seed)
    ref_time = (reference_utc or datetime.now(UTC)).astimezone(UTC)
    end_anchor = ref_time - timedelta(hours=2)
    span_days = min(350, rules.wellbeing_days.rolling_window_months * 29)
    window_start_date = (end_anchor - timedelta(days=span_days)).date()
    threshold = rules.wellbeing_days.min_activities_per_year

    activities: list[SportActivityRecord] = []
    for emp in employees:
        sport_type = sports_by_employee.get(emp.employee_id)
        if sport_type is None:
            continue

        effective_start_date = max(window_start_date, emp.hire_date)
        available_days = (end_anchor.date() - effective_start_date).days
        if available_days < 1:
            continue

        is_assiduous = rng.random() < 0.62
        if is_assiduous:
            annual_target = rng.randint(threshold, threshold + 20)
        else:
            upper_occasional = max(4, threshold - 1)
            annual_target = rng.randint(4, upper_occasional)

        scaled_count = max(1, int(round(annual_target * min(1.0, available_days / span_days))))
        for _ in range(scaled_count):
            day_offset = rng.randint(0, available_days - 1)
            act_date = effective_start_date + timedelta(days=day_offset)
            hour = rng.choice((6, 7, 8, 12, 17, 18, 19, 20))
            minute = rng.randint(0, 59)
            start_dt = datetime(
                act_date.year,
                act_date.month,
                act_date.day,
                hour,
                minute,
                tzinfo=UTC,
            )
            activities.append(
                _simulate_single_activity(
                    emp=emp,
                    sport_type=sport_type,
                    activity_start=start_dt,
                    rules=rules,
                    rng=rng,
                    ingestion_mode="historical_seed",
                )
            )

    activities.sort(key=lambda item: (item.activity_start, item.employee_id))
    return activities


def build_live_activity_event(
    employees: Sequence[EmployeeAuditRecord],
    sports_by_employee: dict[int, str | None],
    rules: BusinessRules,
    *,
    rng: random.Random,
    event_time: datetime | None = None,
) -> SportActivityRecord:
    sporty = [emp for emp in employees if sports_by_employee.get(emp.employee_id) is not None]
    if not sporty:
        raise ValueError("Aucun collaborateur sportif disponible pour generer un evenement.")
    chosen = rng.choice(sporty)
    sport_type = sports_by_employee[chosen.employee_id]
    assert sport_type is not None

    end_ts = (event_time or datetime.now(UTC)).astimezone(UTC)
    temp_record = _simulate_single_activity(
        emp=chosen,
        sport_type=sport_type,
        activity_start=end_ts,
        rules=rules,
        rng=rng,
        ingestion_mode="live_event",
    )
    shifted_start = end_ts - timedelta(seconds=temp_record.elapsed_seconds)
    return SportActivityRecord(
        employee_id=temp_record.employee_id,
        employee_full_name=temp_record.employee_full_name,
        sport_type=temp_record.sport_type,
        activity_start=shifted_start,
        activity_end=end_ts,
        elapsed_seconds=temp_record.elapsed_seconds,
        distance_m=temp_record.distance_m,
        comment=temp_record.comment,
        ingestion_mode="live_event",
    )


def _upsert_business_parameters(cur: Any, rules: BusinessRules) -> None:
    walk_max = rules.mobility_bonus.eligible_modes["Marche/running"].max_distance_km
    bike_max = rules.mobility_bonus.eligible_modes["Vélo/Trottinette/Autres"].max_distance_km
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
        ) VALUES (1, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (parameter_set_id) DO UPDATE SET
            prime_rate = EXCLUDED.prime_rate,
            max_walk_run_km = EXCLUDED.max_walk_run_km,
            max_bike_micro_km = EXCLUDED.max_bike_micro_km,
            min_annual_activities = EXCLUDED.min_annual_activities,
            wellbeing_days_granted = EXCLUDED.wellbeing_days_granted,
            last_recomputed_at = NOW();
        """,
        (
            rules.mobility_bonus.annual_gross_salary_rate,
            walk_max,
            bike_max,
            rules.wellbeing_days.min_activities_per_year,
            rules.wellbeing_days.granted_days_count,
        ),
    )


def _upsert_employees_audit(cur: Any, employees: Sequence[EmployeeAuditRecord]) -> None:
    sql = """
        INSERT INTO hr_restricted.employees_audit (
            employee_id,
            last_name,
            first_name,
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
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
        )
        ON CONFLICT (employee_id) DO UPDATE SET
            last_name = EXCLUDED.last_name,
            first_name = EXCLUDED.first_name,
            birth_date = EXCLUDED.birth_date,
            business_unit = EXCLUDED.business_unit,
            hire_date = EXCLUDED.hire_date,
            annual_gross_salary_eur = EXCLUDED.annual_gross_salary_eur,
            contract_type = EXCLUDED.contract_type,
            paid_leave_days = EXCLUDED.paid_leave_days,
            home_address = EXCLUDED.home_address,
            declared_commute_mode = EXCLUDED.declared_commute_mode,
            computed_distance_km = EXCLUDED.computed_distance_km,
            max_allowed_distance_km = EXCLUDED.max_allowed_distance_km,
            is_commute_anomaly = EXCLUDED.is_commute_anomaly,
            is_mobility_bonus_eligible = EXCLUDED.is_mobility_bonus_eligible,
            anomaly_reason = EXCLUDED.anomaly_reason,
            updated_at = NOW();
    """
    for emp in employees:
        cur.execute(
            sql,
            (
                emp.employee_id,
                emp.last_name,
                emp.first_name,
                emp.birth_date,
                emp.business_unit,
                emp.hire_date,
                emp.annual_gross_salary_eur,
                emp.contract_type,
                emp.paid_leave_days,
                emp.home_address,
                emp.declared_commute_mode,
                emp.computed_distance_km,
                emp.max_allowed_distance_km,
                emp.is_commute_anomaly,
                emp.is_mobility_bonus_eligible,
                emp.anomaly_reason,
            ),
        )


def _insert_activities_batches(cur: Any, activities: Sequence[SportActivityRecord]) -> int:
    if not activities:
        return 0

    prefix = """
        INSERT INTO oltp_sports.sport_activities (
            employee_id,
            employee_full_name,
            sport_type,
            activity_start,
            activity_end,
            elapsed_seconds,
            distance_m,
            comment,
            ingestion_mode
        ) VALUES
    """
    row_placeholder = "(%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    inserted = 0
    for start_idx in range(0, len(activities), INSERT_BATCH_SIZE):
        batch = activities[start_idx : start_idx + INSERT_BATCH_SIZE]
        values_sql = ", ".join(row_placeholder for _ in batch)
        params: list[object] = []
        for act in batch:
            params.extend(
                (
                    act.employee_id,
                    act.employee_full_name,
                    act.sport_type,
                    act.activity_start,
                    act.activity_end,
                    act.elapsed_seconds,
                    act.distance_m,
                    act.comment,
                    act.ingestion_mode,
                )
            )
        cur.execute(f"{prefix} {values_sql};", tuple(params))
        inserted += len(batch)
    return inserted


def sync_postgres_oltp(
    *,
    settings: EnvSettings | None = None,
    rules: BusinessRules | None = None,
    s3_client: Any | None = None,
    resolver: CommuteDistanceResolver | None = None,
    db_connect: Callable[[], Any] | None = None,
    force_reseed: bool = False,
    live_events_count: int = 0,
    reference_utc: datetime | None = None,
) -> PostgresIngestionSummary:
    active_settings = settings or EnvSettings()
    active_rules = rules or load_business_rules()
    client = s3_client or build_s3_client(active_settings)

    if s3_client is None:
        sync_raw_referentials(settings=active_settings, s3_client=client)

    hr_rows = _fetch_s3_xlsx_records(
        client,
        bucket=active_settings.minio_bucket_raw,
        s3_key=active_settings.hr_referential_s3_key,
    )
    sports_rows = _fetch_s3_xlsx_records(
        client,
        bucket=active_settings.minio_bucket_raw,
        s3_key=active_settings.sports_referential_s3_key,
    )

    owns_resolver = resolver is None
    active_resolver = resolver or CommuteDistanceResolver(
        settings=active_settings,
        rules=active_rules,
    )
    try:
        employees = build_employee_audit_records(hr_rows, active_resolver)
    finally:
        if owns_resolver:
            active_resolver.close()

    sports_map = extract_declared_sports_map(sports_rows, active_rules)
    emp_ids = {emp.employee_id for emp in employees}
    if emp_ids != set(sports_map.keys()):
        diff = sorted(emp_ids ^ set(sports_map.keys()))
        raise ValueError(f"Ecart d'identifiants RH vs Sport detecte : {diff[:10]}")

    connect_fn = db_connect or (
        lambda: pg8000.dbapi.connect(**active_settings.pg8000_connect_kwargs("pipeline"))
    )
    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)
    t0 = time.perf_counter()

    anomalies_count = sum(1 for emp in employees if emp.is_commute_anomaly)
    eligible_count = sum(1 for emp in employees if emp.is_mobility_bonus_eligible)
    active_sport_count = sum(1 for val in sports_map.values() if val is not None)

    conn = connect_fn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO ops_monitoring.pipeline_runs (
                run_id, pipeline_name, started_at, status, rows_processed, anomalies_detected
            ) VALUES (CAST(%s AS UUID), %s, %s, 'RUNNING', 0, 0);
            """,
            (run_id, PIPELINE_NAME, started_at),
        )
        conn.commit()

        _upsert_business_parameters(cur, active_rules)
        _upsert_employees_audit(cur, employees)

        cur.execute(
            """
            SELECT COUNT(*)
            FROM oltp_sports.sport_activities
            WHERE ingestion_mode = 'historical_seed';
            """
        )
        existing_seed_count = int(cur.fetchone()[0])

        seeded_inserted = 0
        skipped_seed = False
        if existing_seed_count > 0 and not force_reseed:
            skipped_seed = True
            logger.info(
                "Historique sportif deja present (%d lignes) : conservation sans doublon.",
                existing_seed_count,
            )
        else:
            if force_reseed and existing_seed_count > 0:
                cur.execute(
                    """
                    DELETE FROM oltp_sports.sport_activities
                    WHERE ingestion_mode = 'historical_seed';
                    """
                )
            historical_records = generate_historical_activities(
                employees=employees,
                sports_by_employee=sports_map,
                rules=active_rules,
                seed=active_settings.historical_random_seed,
                reference_utc=reference_utc,
            )
            seeded_inserted = _insert_activities_batches(cur, historical_records)

        live_inserted = 0
        if live_events_count > 0:
            live_rng = random.Random()
            live_batch = [
                build_live_activity_event(
                    employees=employees,
                    sports_by_employee=sports_map,
                    rules=active_rules,
                    rng=live_rng,
                )
                for _ in range(live_events_count)
            ]
            live_inserted = _insert_activities_batches(cur, live_batch)

        duration_ms = int(round((time.perf_counter() - t0) * 1000))
        total_rows = len(employees) + seeded_inserted + live_inserted
        details_payload = json.dumps(
            {
                "employees_upserted": len(employees),
                "eligible_mobility_count": eligible_count,
                "commute_anomalies_count": anomalies_count,
                "active_sport_declared_count": active_sport_count,
                "seeded_activities_inserted": seeded_inserted,
                "skipped_existing_seed": skipped_seed,
                "live_events_inserted": live_inserted,
            },
            ensure_ascii=True,
        )
        cur.execute(
            """
            UPDATE ops_monitoring.pipeline_runs
            SET finished_at = %s,
                status = 'SUCCESS',
                rows_processed = %s,
                anomalies_detected = %s,
                duration_ms = %s,
                details = CAST(%s AS JSONB)
            WHERE run_id = CAST(%s AS UUID);
            """,
            (
                datetime.now(UTC),
                total_rows,
                anomalies_count,
                duration_ms,
                details_payload,
                run_id,
            ),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        duration_ms = int(round((time.perf_counter() - t0) * 1000))
        err_cur = conn.cursor()
        err_payload = json.dumps({"error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=True)
        err_cur.execute(
            """
            UPDATE ops_monitoring.pipeline_runs
            SET finished_at = %s,
                status = 'FAILED',
                duration_ms = %s,
                details = CAST(%s AS JSONB)
            WHERE run_id = CAST(%s AS UUID);
            """,
            (datetime.now(UTC), duration_ms, err_payload, run_id),
        )
        conn.commit()
        raise
    finally:
        conn.close()

    return PostgresIngestionSummary(
        run_id=run_id,
        employees_upserted=len(employees),
        eligible_mobility_count=eligible_count,
        commute_anomalies_count=anomalies_count,
        active_sport_declared_count=active_sport_count,
        seeded_activities_inserted=seeded_inserted,
        skipped_existing_seed=skipped_seed,
        live_events_inserted=live_inserted,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingestion PostgreSQL OLTP et audit RH depuis MinIO S3."
    )
    parser.add_argument(
        "--force-reseed",
        action="store_true",
        help="Reinitialise l'historique sportif sur 12 mois glissants.",
    )
    parser.add_argument(
        "--live-events",
        type=int,
        default=0,
        help="Nombre d'evenements sportifs temps reel a inserer.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    summary = sync_postgres_oltp(
        force_reseed=args.force_reseed,
        live_events_count=args.live_events,
    )
    logger.info(
        "Ingestion PostgreSQL terminee (run_id=%s) : salaries=%d, eligibles_prime=%d, "
        "anomalies_trajet=%d, sportifs_declares=%d, activites_seed=%d (skip=%s), live=%d",
        summary.run_id,
        summary.employees_upserted,
        summary.eligible_mobility_count,
        summary.commute_anomalies_count,
        summary.active_sport_declared_count,
        summary.seeded_activities_inserted,
        summary.skipped_existing_seed,
        summary.live_events_inserted,
    )


if __name__ == "__main__":
    main()
