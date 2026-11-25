import json
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from scripts.inject_live_activity import (
    insert_unit_live_activity,
    prepare_live_activity_parameters,
)
from src.settings import EnvSettings, load_business_rules
from src.streaming.cdc_consumer import (
    consume_activity_stream,
    evaluate_cdc_payload,
    parse_args,
)
from src.streaming.debezium_connector import (
    build_debezium_connector_config,
    deploy_postgres_cdc_connector,
    mask_connector_config,
    verify_cdc_prerequisites,
    wait_for_connector_running,
)
from src.streaming.slack_notifier import (
    ActivityEvent,
    SlackNotifier,
    format_distance_km_fr,
    format_duration_fr,
    format_slack_activity_message,
)


class FakeCursor:
    def __init__(self, responses: list[list[tuple[Any, ...]]]) -> None:
        self._responses = list(responses)
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))

    def fetchone(self) -> tuple[Any, ...] | None:
        if not self._responses:
            return None
        batch = self._responses.pop(0)
        return batch[0] if batch else None

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

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        pass


class FakeKafkaMessage:
    def __init__(self, payload: dict[str, Any] | bytes | None, err: Any = None) -> None:
        if isinstance(payload, dict):
            self._raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        else:
            self._raw = payload
        self._err = err

    def value(self) -> bytes | None:
        return self._raw

    def error(self) -> Any:
        return self._err


class FakeKafkaConsumer:
    def __init__(self, messages: list[FakeKafkaMessage | None]) -> None:
        self._messages = list(messages)
        self.subscribed_topics: list[str] = []
        self.committed_count = 0
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed_topics = list(topics)

    def poll(self, timeout: float = 0.5) -> FakeKafkaMessage | None:
        if self._messages:
            return self._messages.pop(0)
        return None

    def commit(self, message: Any = None, asynchronous: bool = False) -> None:
        self.committed_count += 1

    def close(self) -> None:
        self.closed = True


def _build_test_settings(**overrides: Any) -> EnvSettings:
    defaults: dict[str, Any] = {
        "postgres_host": "127.0.0.1",
        "postgres_port": 5433,
        "sds_debezium_user": "sds_debezium",
        "sds_debezium_password": SecretStr("unit_test_secret"),
        "slack_webhook_url": SecretStr(
            "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
        ),
        "slack_channel_name": "#defis-sportifs-sds",
        "slack_notifications_enabled": True,
        "http_max_retries": 2,
        "http_retry_backoff_seconds": 0.01,
    }
    defaults.update(overrides)
    return EnvSettings(_env_file=None, **defaults)


def test_slack_webhook_validation_detects_placeholders() -> None:
    assert _build_test_settings().has_valid_slack_webhook() is False

    cfg_disabled = _build_test_settings(
        slack_webhook_url=SecretStr("https://hooks.slack.com/services/T12345/B67890/realToken99"),
        slack_notifications_enabled=False,
    )
    assert cfg_disabled.has_valid_slack_webhook() is False

    cfg_valid = _build_test_settings(
        slack_webhook_url=SecretStr("https://hooks.slack.com/services/T12345/B67890/realToken99"),
        slack_notifications_enabled=True,
    )
    assert cfg_valid.has_valid_slack_webhook() is True


def test_french_formatting_with_and_without_distance() -> None:
    with pytest.raises(ValueError, match="Duree d'activite invalide"):
        format_duration_fr(0)

    assert format_duration_fr(45) == "45 s"
    assert format_duration_fr(3665) == "1 h 01 min 05 s"
    assert format_duration_fr(3720) == "1 h 02 min"
    assert format_duration_fr(2715) == "45 min 15 s"
    assert format_distance_km_fr(None) is None
    assert format_distance_km_fr(10500) == "10,5 km"
    assert format_distance_km_fr(10250) == "10,25 km"

    run_event = ActivityEvent(
        activity_id=2001,
        employee_id=101,
        employee_full_name="Julien Moreau",
        sport_type="Course à pied",
        elapsed_seconds=3120,
        distance_m=10250,
        comment="Sortie le long du Lez",
        ingestion_mode="live_event",
    )
    run_msg = format_slack_activity_message(run_event)
    assert "*Julien Moreau*" in run_msg
    assert "*Course à pied*" in run_msg
    assert "*10,25 km*" in run_msg
    assert "*52 min*" in run_msg
    assert "Commentaire : « Sortie le long du Lez »" in run_msg
    assert all(ord(ch) < 0x1F300 for ch in run_msg)

    badminton_event = ActivityEvent(
        activity_id=2002,
        employee_id=102,
        employee_full_name="Camille Laurent",
        sport_type="Badminton",
        elapsed_seconds=4500,
        distance_m=None,
        comment=None,
        ingestion_mode="live_event",
    )
    badminton_msg = format_slack_activity_message(badminton_event)
    assert "*Camille Laurent*" in badminton_msg
    assert "*Badminton*" in badminton_msg
    assert "*1 h 15 min*" in badminton_msg
    assert "km" not in badminton_msg
    assert "Commentaire" not in badminton_msg


def test_slack_notifier_modes_and_error_handling() -> None:
    event = ActivityEvent(
        activity_id=3001,
        employee_id=201,
        employee_full_name="Claire Bernard",
        sport_type="Vélo de route",
        elapsed_seconds=3600,
        distance_m=26000,
        comment="Boucle vers Palavas",
        ingestion_mode="live_event",
    )

    disabled_cfg = _build_test_settings(slack_notifications_enabled=False)
    with SlackNotifier(disabled_cfg) as disabled_notifier:
        disabled_res = disabled_notifier.notify_activity(event)
    assert disabled_res.mode == "disabled"
    assert disabled_res.delivered is False

    dry_cfg = _build_test_settings()
    with SlackNotifier(dry_cfg) as dry_notifier:
        dry_res = dry_notifier.notify_activity(event)
    assert dry_res.delivered is False
    assert dry_res.mode == "dry_run"

    attempts = 0

    def _mock_retry_ok(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, text="rate_limited")
        return httpx.Response(200, text="ok")

    live_cfg = _build_test_settings(
        slack_webhook_url=SecretStr("https://hooks.slack.com/services/T12345/B67890/realToken99")
    )
    with httpx.Client(transport=httpx.MockTransport(_mock_retry_ok)) as client:
        notifier = SlackNotifier(live_cfg, http_client=client, sleep_fn=lambda _: None)
        live_res = notifier.notify_activity(event)
        assert live_res.delivered is True
        assert live_res.mode == "webhook_http"
        assert attempts == 2

    def _mock_400(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, text="invalid_payload")

    with httpx.Client(transport=httpx.MockTransport(_mock_400)) as client_400:
        notifier_400 = SlackNotifier(live_cfg, http_client=client_400, sleep_fn=lambda _: None)
        with pytest.raises(RuntimeError, match="Rejet HTTP 400"):
            notifier_400.notify_activity(event)

    def _mock_net_fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("network_down", request=request)

    with httpx.Client(transport=httpx.MockTransport(_mock_net_fail)) as client_err:
        notifier_err = SlackNotifier(live_cfg, http_client=client_err, sleep_fn=lambda _: None)
        with pytest.raises(RuntimeError, match="Echec reseau"):
            notifier_err.notify_activity(event)


def test_cdc_anti_spam_circuit_breaker_and_consumer_loop() -> None:
    snapshot_msg = {
        "op": "r",
        "source": {"snapshot": "first", "schema": "oltp_sports", "table": "sport_activities"},
        "after": {
            "activity_id": 1,
            "employee_id": 10,
            "employee_full_name": "Alice Martin",
            "sport_type": "Badminton",
            "elapsed_seconds": 3600,
            "distance_m": None,
            "comment": None,
            "ingestion_mode": "historical_seed",
        },
    }
    reseed_insert_msg = {
        "op": "c",
        "source": {"snapshot": "false", "schema": "oltp_sports", "table": "sport_activities"},
        "after": {
            "activity_id": 2,
            "employee_id": 11,
            "employee_full_name": "Bruno Petit",
            "sport_type": "Natation",
            "elapsed_seconds": 2400,
            "distance_m": 1800,
            "comment": None,
            "ingestion_mode": "historical_seed",
        },
    }
    update_msg = {
        "op": "u",
        "source": {"snapshot": "false"},
        "after": {"activity_id": 3, "ingestion_mode": "live_event"},
    }
    wrapped_live_msg = {
        "payload": {
            "op": "c",
            "source": {"snapshot": "false"},
            "after": {
                "activity_id": 9001,
                "employee_id": 42,
                "employee_full_name": "Thomas Chevalier",
                "sport_type": "Course à pied",
                "activity_start": 1763547300000000,
                "elapsed_seconds": 2700,
                "distance_m": 8400,
                "comment": "Pause midi dynamique",
                "ingestion_mode": "live_event",
            },
        }
    }

    assert evaluate_cdc_payload(b"   ") == ("skip_tombstone", None)
    assert evaluate_cdc_payload(b"{invalid_json") == ("skip_malformed", None)
    assert evaluate_cdc_payload(b'["not_a_dict"]') == ("skip_malformed", None)
    assert evaluate_cdc_payload(json.dumps({"op": "c", "after": {}}))[0] == "skip_historical_seed"
    assert (
        evaluate_cdc_payload(
            json.dumps({"op": "c", "after": {"ingestion_mode": "live_event"}})
        )[0]
        == "skip_malformed"
    )
    assert evaluate_cdc_payload(json.dumps(snapshot_msg))[0] == "skip_snapshot"
    assert evaluate_cdc_payload(json.dumps(reseed_insert_msg))[0] == "skip_historical_seed"
    assert evaluate_cdc_payload(json.dumps(update_msg))[0] == "skip_non_create"

    decision, parsed = evaluate_cdc_payload(json.dumps(wrapped_live_msg))
    assert decision == "notify_live_event"
    assert parsed is not None and parsed.activity_id == 9001

    cli_args = parse_args(["--group-id", "g1", "--max-live-events", "2"])
    assert cli_args.group_id == "g1"
    assert cli_args.max_live_events == 2

    fake_consumer = FakeKafkaConsumer(
        [
            FakeKafkaMessage(snapshot_msg),
            FakeKafkaMessage(reseed_insert_msg),
            FakeKafkaMessage(update_msg),
            FakeKafkaMessage(b"{malformed"),
            FakeKafkaMessage(None),
            FakeKafkaMessage(wrapped_live_msg),
            FakeKafkaMessage(wrapped_live_msg),
        ]
    )
    fake_conn = FakeConnection([])
    cfg = _build_test_settings()

    with SlackNotifier(cfg) as dry_notifier:
        metrics = consume_activity_stream(
            cfg,
            consumer=fake_consumer,
            notifier=dry_notifier,
            pg_conn=fake_conn,
            max_idle_seconds=0.0,
        )

    assert metrics.messages_consumed == 7
    assert metrics.snapshot_skipped == 1
    assert metrics.historical_seed_skipped == 1
    assert metrics.non_create_skipped == 2
    assert metrics.malformed_skipped == 1
    assert metrics.duplicate_skipped == 1
    assert metrics.live_events_notified == 1
    assert metrics.slack_dry_run_logged == 1
    assert fake_consumer.committed_count == 7
    assert fake_conn.commits == 1


def test_debezium_rgpd_guard_and_lifecycle() -> None:
    cfg = _build_test_settings()
    built_cfg = build_debezium_connector_config(cfg)
    masked = mask_connector_config(built_cfg)
    assert masked["database.password"] == "***REDACTED***"

    bad_wal = FakeConnection([[("replica",)]])
    with pytest.raises(RuntimeError, match="wal_level"):
        verify_cdc_prerequisites(bad_wal, cfg)  # type: ignore[arg-type]

    bad_repl = FakeConnection([[("logical",)], [("oltp_sports", "sport_activities", "d")]])
    with pytest.raises(RuntimeError, match="REPLICA IDENTITY FULL"):
        verify_cdc_prerequisites(bad_repl, cfg)  # type: ignore[arg-type]

    breach_conn = FakeConnection(
        [
            [("logical",)],
            [("oltp_sports", "sport_activities", "f")],
            [(True, True, False)],
            [(True, False, True, True)],
        ]
    )
    with pytest.raises(PermissionError, match="Violation RGPD"):
        verify_cdc_prerequisites(breach_conn, cfg)  # type: ignore[arg-type]

    def _mock_failed_task(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "connector": {"state": "RUNNING"},
                "tasks": [{"id": 0, "state": "FAILED", "trace": "boom"}],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(_mock_failed_task)) as fail_http:
        with pytest.raises(RuntimeError, match="Tache Debezium en echec"):
            wait_for_connector_running(
                fail_http,
                "http://localhost:8083",
                "c1",
                timeout_seconds=0.1,
                poll_interval_seconds=0.01,
            )

    valid_conn = FakeConnection(
        [
            [("logical",)],
            [("oltp_sports", "sport_activities", "f")],
            [(True, True, False)],
            [(False, False, True, True)],
        ]
    )

    def _mock_create_api(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/config") and request.method == "GET":
            return httpx.Response(404)
        if request.url.path.endswith("/config") and request.method == "PUT":
            return httpx.Response(201, json=built_cfg)
        if request.url.path.endswith("/status") and request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "name": cfg.debezium_connector_name,
                    "connector": {"state": "RUNNING"},
                    "tasks": [{"id": 0, "state": "RUNNING"}],
                },
            )
        return httpx.Response(404)

    with httpx.Client(transport=httpx.MockTransport(_mock_create_api)) as create_http:
        created_report = deploy_postgres_cdc_connector(
            cfg,
            http_client=create_http,
            pg_conn=valid_conn,  # type: ignore[arg-type]
        )
        assert created_report["action"] == "created"
        assert valid_conn.commits == 1


def test_live_activity_parameters_and_unit_insert() -> None:
    rules = load_business_rules()

    sport, secs, dist_m, comment = prepare_live_activity_parameters(
        rules,
        raw_sport="Runing",
        duration_minutes=50.0,
        distance_km=9.5,
        comment=" Fractionné piste ",
    )
    assert sport == "Course à pied"
    assert secs == 3000
    assert dist_m == 9500
    assert comment == "Fractionné piste"

    with pytest.raises(ValueError, match="ne prend pas en charge de distance"):
        prepare_live_activity_parameters(
            rules,
            raw_sport="Badminton",
            duration_minutes=60.0,
            distance_km=5.0,
        )

    fake_conn = FakeConnection(
        [
            [(66386, "Sophie Mercier", "Badminton")],
            [(9999,)],
        ]
    )
    inserted = insert_unit_live_activity(
        fake_conn,  # type: ignore[arg-type]
        rules,
        employee_id=66386,
        sport="Badminton",
        duration_minutes=60.0,
        comment="Tournoi interne",
    )
    assert inserted["activity_id"] == 9999
    assert "employee_id" not in inserted
    assert inserted["sport_type"] == "Badminton"
    assert inserted["distance_m"] is None
    assert inserted["ingestion_mode"] == "live_event"
    log_payload = json.loads(fake_conn.cursor_instance.executed[-1][1][5])
    assert "employee_id" not in log_payload
    assert fake_conn.commits == 1