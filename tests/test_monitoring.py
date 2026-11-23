from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from src.monitoring import (
    MONITORING_VIEW_NAMES,
    LayerFreshnessMetric,
    PipelineHealthMetric,
    QualityLayerSummary,
    classify_pipeline_layer,
    consolidate_monitoring,
    ensure_monitoring_views,
    evaluate_platform_health,
    format_health_report_text,
    send_pipeline_slack_alert,
)
from src.monitoring.pipeline_monitor import main as monitor_main
from src.monitoring.pipeline_monitor import sanitize_monitoring_text
from src.settings import EnvSettings


class _FakeCursor:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn
        self._rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        normalized = " ".join(sql.split())
        self._conn.executed_sql.append((normalized, params))
        if "FROM ops_monitoring.v_pipeline_health_summary" in normalized:
            self._rows = list(self._conn.pipeline_rows)
        elif "FROM ops_monitoring.v_layer_freshness_status" in normalized:
            self._rows = list(self._conn.layer_rows)
        elif "FROM ops_monitoring.v_quality_layer_summary" in normalized:
            self._rows = list(self._conn.quality_rows)
        else:
            self._rows = []

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def close(self) -> None:
        return None


class _FakeConnection:
    def __init__(
        self,
        *,
        pipeline_rows: list[tuple[Any, ...]] | None = None,
        layer_rows: list[tuple[Any, ...]] | None = None,
        quality_rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self.pipeline_rows = pipeline_rows or []
        self.layer_rows = layer_rows or []
        self.quality_rows = quality_rows or []
        self.executed_sql: list[tuple[str, tuple[Any, ...] | None]] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None


def _sample_pipeline(
    name: str,
    layer: str,
    *,
    status: str = "SUCCESS",
    age_min: float = 15.0,
    is_fresh: bool = True,
    anomalies: int = 0,
    rows: int = 100,
) -> PipelineHealthMetric:
    return PipelineHealthMetric(
        pipeline_name=name,
        layer_group=layer,
        total_runs=2,
        succeeded_runs=2 if status == "SUCCESS" else 1,
        failed_runs=0 if status == "SUCCESS" else 1,
        success_rate_pct=100.0 if status == "SUCCESS" else 50.0,
        latest_run_id="00000000-0000-0000-0000-000000000001",
        latest_status=status,
        latest_started_at="2026-11-20T12:00:00+00:00",
        latest_finished_at="2026-11-20T12:00:02+00:00",
        last_success_at="2026-11-20T12:00:02+00:00",
        freshness_age_minutes=age_min,
        is_fresh=is_fresh,
        latest_rows_processed=rows,
        cumulative_rows_processed=rows * 2,
        latest_anomalies_detected=anomalies,
        latest_duration_ms=250,
        avg_duration_ms=240.0,
        max_duration_ms=260,
    )


def _sample_layer(layer: str, *, is_fresh: bool = True) -> LayerFreshnessMetric:
    return LayerFreshnessMetric(
        layer_group=layer,
        pipelines_count=1,
        total_runs=2,
        succeeded_runs=2,
        failed_runs=0,
        layer_health_status="HEALTHY",
        last_activity_at="2026-11-20T12:00:02+00:00",
        last_success_at="2026-11-20T12:00:02+00:00",
        freshness_age_minutes=15.0 if is_fresh else 3000.0,
        is_fresh=is_fresh,
        latest_rows_processed=100,
        max_anomalies_detected=1 if layer != "quality_gate" else 0,
        avg_duration_ms=240.0,
    )


def _sample_quality(layer: str, total: int, failed: int = 0) -> QualityLayerSummary:
    passed = total - failed
    return QualityLayerSummary(
        layer_name=layer,
        latest_run_id="00000000-0000-0000-0000-000000000099",
        total_checks=total,
        passed_checks=passed,
        warned_checks=0,
        failed_checks=failed,
        total_failed_rows=failed * 3,
        pass_rate_pct=round(100.0 * passed / total, 2),
        evaluated_at="2026-11-20T13:00:00+00:00",
    )


def test_classify_pipeline_layer_mappings() -> None:
    assert classify_pipeline_layer("postgres_oltp_ingestion") == "ingestion_oltp"
    assert classify_pipeline_layer("minio_raw_sync") == "ingestion_oltp"
    assert classify_pipeline_layer("cdc_debezium_provisioning") == "streaming_cdc"
    assert classify_pipeline_layer("cdc_slack_activity_consumer") == "streaming_cdc"
    assert classify_pipeline_layer("live_activity_injection") == "streaming_cdc"
    assert classify_pipeline_layer("lakehouse_bronze_silver_sync") == "lakehouse_bronze_silver"
    assert classify_pipeline_layer("lakehouse_gold_recompute") == "lakehouse_gold"
    assert classify_pipeline_layer("soda_data_quality_gate") == "quality_gate"
    assert classify_pipeline_layer("ops_monitoring_consolidation") == "orchestration"
    assert classify_pipeline_layer("kestra_daily_benefits") == "orchestration"
    assert classify_pipeline_layer("custom_adhoc_job") == "other"


def test_evaluate_platform_health_transitions() -> None:
    base_pipelines = [
        _sample_pipeline("postgres_oltp_ingestion", "ingestion_oltp", anomalies=1, rows=161),
        _sample_pipeline(
            "cdc_slack_activity_consumer",
            "streaming_cdc",
            age_min=1800.0,
            is_fresh=True,
            rows=2,
        ),
        _sample_pipeline(
            "lakehouse_bronze_silver_sync",
            "lakehouse_bronze_silver",
            anomalies=1,
            rows=1884,
        ),
        _sample_pipeline("lakehouse_gold_recompute", "lakehouse_gold", anomalies=1, rows=342),
        _sample_pipeline("soda_data_quality_gate", "quality_gate", rows=4096),
    ]
    base_layers = [
        _sample_layer("ingestion_oltp"),
        _sample_layer("streaming_cdc"),
        _sample_layer("lakehouse_bronze_silver"),
        _sample_layer("lakehouse_gold"),
        _sample_layer("quality_gate"),
    ]
    base_quality = [
        _sample_quality("hr_audit", 6),
        _sample_quality("bronze", 4),
        _sample_quality("silver", 7),
        _sample_quality("gold", 5),
    ]

    healthy = evaluate_platform_health(
        pipelines=base_pipelines,
        layers=base_layers,
        quality_layers=base_quality,
    )
    assert healthy.overall_status == "HEALTHY"
    assert healthy.quality_gate_passed is True
    assert healthy.total_quality_checks == 22
    assert healthy.passed_quality_checks == 22
    assert healthy.isolated_business_anomalies == 1
    rendered = format_health_report_text(healthy)
    assert "Statut global : HEALTHY" in rendered
    assert "22/22 PASS" in rendered

    stale_pipelines = [
        _sample_pipeline(
            "postgres_oltp_ingestion",
            "ingestion_oltp",
            age_min=3100.0,
            is_fresh=False,
            anomalies=1,
        ),
        *base_pipelines[1:],
    ]
    degraded = evaluate_platform_health(
        pipelines=stale_pipelines,
        layers=base_layers,
        quality_layers=base_quality,
    )
    assert degraded.overall_status == "DEGRADED"
    assert degraded.stale_pipelines == ["postgres_oltp_ingestion"]

    critical_quality = [*base_quality[:3], _sample_quality("gold", 5, failed=1)]
    critical = evaluate_platform_health(
        pipelines=base_pipelines,
        layers=base_layers,
        quality_layers=critical_quality,
    )
    assert critical.overall_status == "CRITICAL"
    assert critical.quality_gate_passed is False
    assert critical.failed_quality_checks == 1


def test_ensure_views_and_consolidate_monitoring_rbac_isolation() -> None:
    now_utc = datetime(2026, 11, 20, 14, 0, tzinfo=UTC)
    fake_conn = _FakeConnection(
        pipeline_rows=[
            (
                "lakehouse_bronze_silver_sync",
                "lakehouse_bronze_silver",
                2,
                2,
                0,
                Decimal("100.00"),
                "run-bs",
                "SUCCESS",
                now_utc,
                now_utc,
                now_utc,
                Decimal("10.00"),
                1884,
                3768,
                1,
                3200,
                Decimal("3100.00"),
                3300,
            ),
            (
                "lakehouse_gold_recompute",
                "lakehouse_gold",
                1,
                1,
                0,
                Decimal("100.00"),
                "run-gold",
                "SUCCESS",
                now_utc,
                now_utc,
                now_utc,
                Decimal("8.00"),
                342,
                342,
                1,
                2100,
                Decimal("2100.00"),
                2100,
            ),
            (
                "soda_data_quality_gate",
                "quality_gate",
                1,
                1,
                0,
                Decimal("100.00"),
                "run-soda",
                "SUCCESS",
                now_utc,
                now_utc,
                now_utc,
                Decimal("5.00"),
                4096,
                4096,
                0,
                4100,
                Decimal("4100.00"),
                4100,
            ),
        ],
        layer_rows=[
            (
                "lakehouse_bronze_silver",
                1,
                2,
                2,
                0,
                "HEALTHY",
                now_utc,
                now_utc,
                Decimal("10.00"),
                1884,
                1,
                Decimal("3100.00"),
            ),
            (
                "lakehouse_gold",
                1,
                1,
                1,
                0,
                "HEALTHY",
                now_utc,
                now_utc,
                Decimal("8.00"),
                342,
                1,
                Decimal("2100.00"),
            ),
            (
                "quality_gate",
                1,
                1,
                1,
                0,
                "HEALTHY",
                now_utc,
                now_utc,
                Decimal("5.00"),
                4096,
                0,
                Decimal("4100.00"),
            ),
        ],
        quality_rows=[
            ("gold", "run-soda", 22, 22, 0, 0, 0, Decimal("100.00"), now_utc),
        ],
    )

    with pytest.raises(ValueError, match="Identifiant de role SQL invalide"):
        ensure_monitoring_views(fake_conn, bi_reader_role="sds_bi_reader; DROP SCHEMA")

    settings = EnvSettings(_env_file=None)
    bi_report = consolidate_monitoring(
        settings=settings,
        conn=fake_conn,
        role="bi_reader",
        ensure_views=True,
        record_run=True,
    )
    assert bi_report.overall_status == "HEALTHY"
    assert not any("CREATE OR REPLACE VIEW" in sql for sql, _ in fake_conn.executed_sql)
    assert not any(
        "INSERT INTO ops_monitoring.pipeline_runs" in sql
        for sql, _ in fake_conn.executed_sql
    )

    pipe_report = consolidate_monitoring(
        settings=settings,
        conn=fake_conn,
        role="pipeline",
        ensure_views=True,
        record_run=True,
    )
    assert pipe_report.overall_status == "HEALTHY"
    grants = [sql for sql, _ in fake_conn.executed_sql if sql.startswith("GRANT SELECT")]
    assert len(grants) == len(MONITORING_VIEW_NAMES)
    inserts = [
        params
        for sql, params in fake_conn.executed_sql
        if "INSERT INTO ops_monitoring.pipeline_runs" in sql
    ]
    assert len(inserts) == 1


def test_slack_alert_sanitization_dry_run_and_http_retry() -> None:
    leaked = (
        "Erreur HTTP vers https://hooks.slack.com/services/T111/B222/SECRET999 "
        "avec symbole interdit \U0001f680"
    )
    sanitized = sanitize_monitoring_text(leaked)
    assert "SECRET999" not in sanitized
    assert "[REDACTED]" in sanitized
    assert "\U0001f680" not in sanitized

    dry_settings = EnvSettings(
        _env_file=None,
        slack_webhook_url=SecretStr("https://hooks.slack.com/services/T00000000/B00000000/XXXX"),
        slack_notifications_enabled=True,
    )
    dry_res = send_pipeline_slack_alert(
        failed_step="soda_quality_gate",
        execution_id="exec-01",
        error_summary="1 check en echec",
        settings=dry_settings,
    )
    assert dry_res.dry_run is True
    assert dry_res.delivered is False

    attempts = {"count": 0}

    def _handler(request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        if attempts["count"] == 1:
            return httpx.Response(503, text="temporary error")
        return httpx.Response(200, text="ok")

    live_settings = EnvSettings(
        _env_file=None,
        slack_webhook_url=SecretStr("https://hooks.slack.com/services/T12345678/B12345678/REALTOKEN99"),
        slack_notifications_enabled=True,
        http_max_retries=2,
        http_retry_backoff_seconds=0.0,
    )
    with httpx.Client(transport=httpx.MockTransport(_handler)) as mock_client:
        live_res = send_pipeline_slack_alert(
            failed_step="gold_recompute",
            execution_id="exec-02",
            error_summary="Timeout S3",
            settings=live_settings,
            http_client=mock_client,
        )
    assert attempts["count"] == 2
    assert live_res.delivered is True
    assert live_res.dry_run is False
    assert live_res.status_code == 200


def test_cli_failure_alert_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("SLACK_NOTIFICATIONS_ENABLED", "false")
    exit_code = monitor_main(
        [
            "--send-failure-alert",
            "--failed-step",
            "soda_gate",
            "--execution-id",
            "kestra-test-run",
            "--json",
        ]
    )
    captured = capsys.readouterr().out
    assert exit_code == 0
    assert '"dry_run": true' in captured

def test_cli_consolidate_critical_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    critical_report = evaluate_platform_health(
        pipelines=[
            _sample_pipeline(
                "soda_data_quality_gate",
                "quality_gate",
                status="FAILED",
            )
        ],
        layers=[_sample_layer("quality_gate")],
        quality_layers=[_sample_quality("gold", 5, failed=1)],
    )
    monkeypatch.setenv("SLACK_NOTIFICATIONS_ENABLED", "false")
    monkeypatch.setattr(
        "src.monitoring.pipeline_monitor.consolidate_monitoring",
        lambda **kwargs: critical_report,
    )
    exit_code = monitor_main(
        [
            "--skip-views",
            "--fail-on-critical",
            "--alert-on-critical",
            "--json",
        ]
    )
    payload = capsys.readouterr().out
    assert exit_code == 1
    assert '"overall_status": "CRITICAL"' in payload
