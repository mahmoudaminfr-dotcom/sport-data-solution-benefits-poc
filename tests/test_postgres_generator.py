import io
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import openpyxl

from src.ingestion.commute_distance import CommuteDistanceResolver
from src.ingestion.postgres_generator import (
    build_employee_audit_records,
    extract_declared_sports_map,
    generate_historical_activities,
    read_xlsx_records_from_bytes,
    sync_postgres_oltp,
)
from src.settings import EnvSettings, load_business_rules


def _build_xlsx_bytes(headers: list[str], rows: list[list[object]]) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "Feuille 1"
    ws.append(headers)
    for row in rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    wb.close()
    return buf.getvalue()


class FakeS3Reader:
    def __init__(self, payloads: dict[tuple[str, str], bytes]) -> None:
        self._payloads = payloads

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        return {"Body": io.BytesIO(self._payloads[(Bucket, Key)])}


class FakeDbState:
    def __init__(self) -> None:
        self.seeded_rows: int = 0
        self.live_rows: int = 0
        self.employees_upserts: int = 0
        self.pipeline_statuses: list[str] = []


class FakeCursor:
    def __init__(self, state: FakeDbState) -> None:
        self._state = state
        self._next_one: tuple[object, ...] | None = None

    def execute(self, sql: str, params: tuple[object, ...] = ()) -> None:
        compact = " ".join(sql.split())
        if "SELECT COUNT(*) FROM oltp_sports.sport_activities" in compact:
            self._next_one = (self._state.seeded_rows,)
        elif "DELETE FROM oltp_sports.sport_activities" in compact:
            self._state.seeded_rows = 0
        elif "INSERT INTO oltp_sports.sport_activities" in compact:
            batch_rows = len(params) // 9
            if "live_event" in params:
                self._state.live_rows += batch_rows
            else:
                self._state.seeded_rows += batch_rows
        elif "INSERT INTO hr_restricted.employees_audit" in compact:
            self._state.employees_upserts += 1
        elif "INSERT INTO ops_monitoring.pipeline_runs" in compact:
            self._state.pipeline_statuses.append("RUNNING")
        elif "UPDATE ops_monitoring.pipeline_runs" in compact and "SUCCESS" in compact:
            self._state.pipeline_statuses.append("SUCCESS")

    def fetchone(self) -> tuple[object, ...]:
        assert self._next_one is not None
        return self._next_one


class FakeConnection:
    def __init__(self, state: FakeDbState) -> None:
        self._state = state

    def cursor(self) -> FakeCursor:
        return FakeCursor(self._state)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def _sample_workbooks() -> tuple[bytes, bytes]:
    hr_headers = [
        "ID salarié",
        "Nom",
        "Prénom",
        "Date de naissance",
        "BU",
        "Date d'embauche",
        "Salaire brut",
        "Type de contrat",
        "Nombre de jours de CP",
        "Adresse du domicile",
        "Moyen de déplacement",
    ]
    hr_rows: list[list[object]] = [
        [
            1001.0,
            "Martin",
            "Claire",
            datetime(1991, 5, 12),
            "R&D",
            datetime(2022, 9, 1),
            48000.0,
            "CDI",
            25.0,
            "10 Rue de la Loge, 34000 Montpellier",
            "Marche/running",
        ],
        [
            1002.0,
            "Bernard",
            "Lucas",
            datetime(1988, 11, 3),
            "Finance",
            datetime(2023, 2, 15),
            52000.0,
            "CDI",
            25.0,
            "20 Boulevard Gambetta, 30000 Nîmes",
            "Vélo/Trottinette/Autres",
        ],
        [
            1003.0,
            "Petit",
            "Nora",
            datetime(1995, 3, 22),
            "Marketing",
            datetime(2024, 1, 10),
            39000.0,
            "CDD",
            25.0,
            "5 Avenue Jean Jaurès, 34170 Castelnau-le-Lez",
            "Transports en commun",
        ],
    ]
    sports_headers = ["ID salarié", "Pratique d'un sport"]
    sports_rows: list[list[object]] = [
        [1001.0, "Runing"],
        [1002.0, "Tennis"],
        [1003.0, None],
    ]
    return _build_xlsx_bytes(hr_headers, hr_rows), _build_xlsx_bytes(sports_headers, sports_rows)


def _mock_resolver(tmp_path: Path) -> CommuteDistanceResolver:
    def handler(request: httpx.Request) -> httpx.Response:
        origins = request.url.params.get("origins", "")
        dist_m = 6200 if "Montpellier" in origins else 48000
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "rows": [{"elements": [{"status": "OK", "distance": {"value": dist_m}}]}],
            },
        )

    settings = EnvSettings(
        _env_file=None,
        cache_data_dir=tmp_path,
        google_maps_api_key="AIzaSyUnitTestingValidKey0001",
    )
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return CommuteDistanceResolver(settings=settings, http_client=client)


def test_deterministic_activities_and_sport_distance_rules(tmp_path: Path) -> None:
    hr_bytes, sp_bytes = _sample_workbooks()
    rules = load_business_rules()
    with _mock_resolver(tmp_path) as resolver:
        employees = build_employee_audit_records(read_xlsx_records_from_bytes(hr_bytes), resolver)

    sports_map = extract_declared_sports_map(read_xlsx_records_from_bytes(sp_bytes), rules)
    assert sports_map == {1001: "Course à pied", 1002: "Tennis", 1003: None}

    ref_utc = datetime(2026, 11, 18, 8, 0, tzinfo=UTC)
    run_a = generate_historical_activities(
        employees,
        sports_map,
        rules,
        seed=42,
        reference_utc=ref_utc,
    )
    run_b = generate_historical_activities(
        employees,
        sports_map,
        rules,
        seed=42,
        reference_utc=ref_utc,
    )
    assert run_a == run_b
    assert len(run_a) > 0

    emp_hire = {emp.employee_id: emp.hire_date for emp in employees}
    for act in run_a:
        assert act.employee_id in {1001, 1002}
        assert act.activity_end > act.activity_start
        assert act.activity_start.date() >= emp_hire[act.employee_id]
        if act.sport_type == "Course à pied":
            assert act.distance_m is not None and act.distance_m > 0
        elif act.sport_type == "Tennis":
            assert act.distance_m is None


def test_sync_postgres_oltp_idempotency_and_live_events(tmp_path: Path) -> None:
    hr_bytes, sp_bytes = _sample_workbooks()
    settings = EnvSettings(_env_file=None, cache_data_dir=tmp_path)
    rules = load_business_rules()
    fake_s3 = FakeS3Reader(
        {
            (settings.minio_bucket_raw, settings.hr_referential_s3_key): hr_bytes,
            (settings.minio_bucket_raw, settings.sports_referential_s3_key): sp_bytes,
        }
    )
    db_state = FakeDbState()
    ref_utc = datetime(2026, 11, 18, 8, 0, tzinfo=UTC)

    with _mock_resolver(tmp_path) as resolver:
        first = sync_postgres_oltp(
            settings=settings,
            rules=rules,
            s3_client=fake_s3,
            resolver=resolver,
            db_connect=lambda: FakeConnection(db_state),
            reference_utc=ref_utc,
        )
        second = sync_postgres_oltp(
            settings=settings,
            rules=rules,
            s3_client=fake_s3,
            resolver=resolver,
            db_connect=lambda: FakeConnection(db_state),
            live_events_count=2,
            reference_utc=ref_utc,
        )

    assert first.employees_upserted == 3
    assert first.eligible_mobility_count == 1
    assert first.commute_anomalies_count == 1
    assert first.active_sport_declared_count == 2
    assert first.seeded_activities_inserted > 0
    assert first.skipped_existing_seed is False

    assert second.seeded_activities_inserted == 0
    assert second.skipped_existing_seed is True
    assert second.live_events_inserted == 2
    assert db_state.live_rows == 2
    assert date(2026, 1, 1) < ref_utc.date()
    assert db_state.pipeline_statuses == ["RUNNING", "SUCCESS", "RUNNING", "SUCCESS"]
