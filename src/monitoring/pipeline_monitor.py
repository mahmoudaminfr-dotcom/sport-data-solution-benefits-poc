from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

import httpx
import pg8000.dbapi

from src.settings import DbRole, EnvSettings

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

OverallHealthStatus = Literal["HEALTHY", "DEGRADED", "CRITICAL"]

MONITORING_PIPELINE_NAME = "ops_monitoring_consolidation"

SCHEDULED_BATCH_LAYERS: tuple[str, ...] = (
    "ingestion_oltp",
    "lakehouse_bronze_silver",
    "lakehouse_gold",
    "quality_gate",
    "orchestration",
)

MONITORING_VIEW_NAMES: tuple[str, ...] = (
    "v_pipeline_health_summary",
    "v_layer_freshness_status",
    "v_quality_latest_checks",
    "v_quality_layer_summary",
)

BI_READER_MONITORING_VIEWS: tuple[str, ...] = (
    "v_pipeline_health_summary",
    "v_quality_layer_summary",
)

MONITORING_VIEWS_DDL: tuple[tuple[str, str], ...] = (
    (
        "v_pipeline_health_summary",
        """
        CREATE OR REPLACE VIEW ops_monitoring.v_pipeline_health_summary AS
        WITH classified_runs AS (
            SELECT
                run_id,
                pipeline_name,
                CASE
                    WHEN pipeline_name ILIKE '%minio%'
                      OR pipeline_name ILIKE '%oltp%'
                      OR pipeline_name ILIKE '%postgres%'
                      OR pipeline_name ILIKE '%ingest%'
                      OR pipeline_name ILIKE '%commute%'
                      OR pipeline_name ILIKE '%seed%' THEN 'ingestion_oltp'
                    WHEN pipeline_name ILIKE '%cdc%'
                      OR pipeline_name ILIKE '%debezium%'
                      OR pipeline_name ILIKE '%slack%'
                      OR pipeline_name ILIKE '%stream%'
                      OR pipeline_name ILIKE '%live_activity%' THEN 'streaming_cdc'
                    WHEN pipeline_name ILIKE '%bronze%'
                      OR pipeline_name ILIKE '%silver%' THEN 'lakehouse_bronze_silver'
                    WHEN pipeline_name ILIKE '%gold%' THEN 'lakehouse_gold'
                    WHEN pipeline_name ILIKE '%soda%'
                      OR pipeline_name ILIKE '%quality%' THEN 'quality_gate'
                    WHEN pipeline_name ILIKE '%kestra%'
                      OR pipeline_name ILIKE '%orchestr%'
                      OR pipeline_name ILIKE '%monitoring%' THEN 'orchestration'
                    ELSE 'other'
                END AS layer_group,
                started_at,
                finished_at,
                status,
                rows_processed,
                anomalies_detected,
                duration_ms,
                ROW_NUMBER() OVER (
                    PARTITION BY pipeline_name
                    ORDER BY started_at DESC, COALESCE(finished_at, started_at) DESC, run_id DESC
                ) AS rn
            FROM ops_monitoring.pipeline_runs
        ),
        aggregated AS (
            SELECT
                pipeline_name,
                layer_group,
                COUNT(*)::bigint AS total_runs,
                COUNT(*) FILTER (WHERE status = 'SUCCESS')::bigint AS succeeded_runs,
                COUNT(*) FILTER (WHERE status <> 'SUCCESS')::bigint AS failed_runs,
                ROUND(
                    (
                        100.0 * COUNT(*) FILTER (WHERE status = 'SUCCESS')
                        / NULLIF(COUNT(*), 0)
                    )::numeric,
                    2
                ) AS success_rate_pct,
                MAX(COALESCE(finished_at, started_at))
                    FILTER (WHERE status = 'SUCCESS') AS last_success_at,
                COALESCE(
                    SUM(rows_processed) FILTER (WHERE status = 'SUCCESS'),
                    0
                )::bigint AS cumulative_rows_processed,
                ROUND(AVG(duration_ms)::numeric, 2) AS avg_duration_ms,
                MAX(duration_ms) AS max_duration_ms
            FROM classified_runs
            GROUP BY pipeline_name, layer_group
        )
        SELECT
            a.pipeline_name,
            a.layer_group,
            a.total_runs,
            a.succeeded_runs,
            a.failed_runs,
            COALESCE(a.success_rate_pct, 0.00) AS success_rate_pct,
            l.run_id::text AS latest_run_id,
            l.status AS latest_status,
            l.started_at AS latest_started_at,
            l.finished_at AS latest_finished_at,
            a.last_success_at,
            CASE
                WHEN a.last_success_at IS NULL THEN NULL
                ELSE ROUND(
                    GREATEST(
                        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - a.last_success_at)) / 60.0,
                        0.0
                    )::numeric,
                    2
                )
            END AS freshness_age_minutes,
            l.rows_processed AS latest_rows_processed,
            a.cumulative_rows_processed,
            l.anomalies_detected AS latest_anomalies_detected,
            l.duration_ms AS latest_duration_ms,
            a.avg_duration_ms,
            a.max_duration_ms
        FROM aggregated a
        INNER JOIN classified_runs l
            ON l.pipeline_name = a.pipeline_name
           AND l.rn = 1
        ORDER BY
            CASE a.layer_group
                WHEN 'ingestion_oltp' THEN 1
                WHEN 'streaming_cdc' THEN 2
                WHEN 'lakehouse_bronze_silver' THEN 3
                WHEN 'lakehouse_gold' THEN 4
                WHEN 'quality_gate' THEN 5
                WHEN 'orchestration' THEN 6
                ELSE 7
            END,
            a.pipeline_name
        """,
    ),
    (
        "v_layer_freshness_status",
        """
        CREATE OR REPLACE VIEW ops_monitoring.v_layer_freshness_status AS
        SELECT
            layer_group,
            COUNT(*)::bigint AS pipelines_count,
            SUM(total_runs)::bigint AS total_runs,
            SUM(succeeded_runs)::bigint AS succeeded_runs,
            SUM(failed_runs)::bigint AS failed_runs,
            CASE
                WHEN COUNT(*) FILTER (WHERE latest_status <> 'SUCCESS') > 0 THEN 'CRITICAL'
                WHEN COUNT(*) FILTER (WHERE last_success_at IS NULL) > 0 THEN 'DEGRADED'
                ELSE 'HEALTHY'
            END AS layer_health_status,
            MAX(COALESCE(latest_finished_at, latest_started_at)) AS last_activity_at,
            MAX(last_success_at) AS last_success_at,
            CASE
                WHEN MIN(last_success_at) IS NULL THEN NULL
                ELSE ROUND(
                    GREATEST(
                        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MIN(last_success_at))) / 60.0,
                        0.0
                    )::numeric,
                    2
                )
            END AS freshness_age_minutes,
            SUM(latest_rows_processed)::bigint AS latest_rows_processed,
            MAX(latest_anomalies_detected)::integer AS max_anomalies_detected,
            ROUND(AVG(avg_duration_ms)::numeric, 2) AS avg_duration_ms
        FROM ops_monitoring.v_pipeline_health_summary
        GROUP BY layer_group
        ORDER BY
            CASE layer_group
                WHEN 'ingestion_oltp' THEN 1
                WHEN 'streaming_cdc' THEN 2
                WHEN 'lakehouse_bronze_silver' THEN 3
                WHEN 'lakehouse_gold' THEN 4
                WHEN 'quality_gate' THEN 5
                WHEN 'orchestration' THEN 6
                ELSE 7
            END
        """,
    ),
    (
        "v_quality_latest_checks",
        """
        CREATE OR REPLACE VIEW ops_monitoring.v_quality_latest_checks AS
        WITH latest_batch AS (
            SELECT run_id, evaluated_at
            FROM ops_monitoring.quality_check_results
            ORDER BY evaluated_at DESC, check_id DESC
            LIMIT 1
        )
        SELECT
            q.check_id,
            q.run_id::text AS run_id,
            q.layer_name,
            q.check_name,
            q.outcome,
            CASE WHEN UPPER(q.outcome) = 'PASS' THEN 1 ELSE 0 END AS is_passed,
            q.failed_rows_count,
            q.evaluated_at
        FROM ops_monitoring.quality_check_results q
        INNER JOIN latest_batch b
            ON (b.run_id IS NOT NULL AND q.run_id = b.run_id)
            OR (b.run_id IS NULL AND q.evaluated_at = b.evaluated_at)
        ORDER BY
            CASE q.layer_name
                WHEN 'hr_audit' THEN 1
                WHEN 'bronze' THEN 2
                WHEN 'silver' THEN 3
                WHEN 'gold' THEN 4
                ELSE 5
            END,
            q.check_name
        """,
    ),
    (
        "v_quality_layer_summary",
        """
        CREATE OR REPLACE VIEW ops_monitoring.v_quality_layer_summary AS
        SELECT
            layer_name,
            MAX(run_id) AS latest_run_id,
            COUNT(*)::bigint AS total_checks,
            COUNT(*) FILTER (WHERE UPPER(outcome) = 'PASS')::bigint AS passed_checks,
            COUNT(*) FILTER (WHERE UPPER(outcome) = 'WARN')::bigint AS warned_checks,
            COUNT(*) FILTER (
                WHERE UPPER(outcome) NOT IN ('PASS', 'WARN')
            )::bigint AS failed_checks,
            COALESCE(SUM(failed_rows_count), 0)::bigint AS total_failed_rows,
            ROUND(
                (
                    100.0 * COUNT(*) FILTER (WHERE UPPER(outcome) = 'PASS')
                    / NULLIF(COUNT(*), 0)
                )::numeric,
                2
            ) AS pass_rate_pct,
            MAX(evaluated_at) AS evaluated_at
        FROM ops_monitoring.v_quality_latest_checks
        GROUP BY layer_name
        ORDER BY
            CASE layer_name
                WHEN 'hr_audit' THEN 1
                WHEN 'bronze' THEN 2
                WHEN 'silver' THEN 3
                WHEN 'gold' THEN 4
                ELSE 5
            END,
            layer_name
        """,
    ),
)

_SECRET_URL_RE = re.compile(r"https?://hooks\.slack\.com/services/\S+", re.IGNORECASE)
_NON_BMP_RE = re.compile(r"[\U00010000-\U0010FFFF]")


@dataclass(frozen=True)
class PipelineHealthMetric:
    pipeline_name: str
    layer_group: str
    total_runs: int
    succeeded_runs: int
    failed_runs: int
    success_rate_pct: float
    latest_run_id: str
    latest_status: str
    latest_started_at: str | None
    latest_finished_at: str | None
    last_success_at: str | None
    freshness_age_minutes: float | None
    is_fresh: bool
    latest_rows_processed: int
    cumulative_rows_processed: int
    latest_anomalies_detected: int
    latest_duration_ms: int | None
    avg_duration_ms: float | None
    max_duration_ms: int | None


@dataclass(frozen=True)
class LayerFreshnessMetric:
    layer_group: str
    pipelines_count: int
    total_runs: int
    succeeded_runs: int
    failed_runs: int
    layer_health_status: str
    last_activity_at: str | None
    last_success_at: str | None
    freshness_age_minutes: float | None
    is_fresh: bool
    latest_rows_processed: int
    max_anomalies_detected: int
    avg_duration_ms: float | None


@dataclass(frozen=True)
class QualityLayerSummary:
    layer_name: str
    latest_run_id: str | None
    total_checks: int
    passed_checks: int
    warned_checks: int
    failed_checks: int
    total_failed_rows: int
    pass_rate_pct: float
    evaluated_at: str | None


@dataclass(frozen=True)
class PlatformHealthReport:
    generated_at: str
    overall_status: OverallHealthStatus
    freshness_sla_hours: float
    pipelines: list[PipelineHealthMetric]
    layers: list[LayerFreshnessMetric]
    quality_layers: list[QualityLayerSummary]
    quality_gate_passed: bool
    total_quality_checks: int
    passed_quality_checks: int
    warned_quality_checks: int
    failed_quality_checks: int
    isolated_business_anomalies: int
    missing_required_layers: list[str]
    failing_pipelines: list[str]
    stale_pipelines: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PipelineAlertResult:
    delivered: bool
    dry_run: bool
    status_code: int | None
    channel: str
    message: str


def _to_float(value: Decimal | float | int | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)


def _to_int(value: int | Decimal | None) -> int | None:
    if value is None:
        return None
    return int(value)


def _to_iso_utc(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC).isoformat()
        return value.astimezone(UTC).isoformat()
    return str(value)


def sanitize_monitoring_text(raw: str, *, max_length: int = 280) -> str:
    cleaned = _SECRET_URL_RE.sub("https://hooks.slack.com/services/[REDACTED]", raw)
    cleaned = _NON_BMP_RE.sub("", cleaned)
    cleaned = " ".join(cleaned.split())
    if len(cleaned) > max_length:
        return f"{cleaned[: max_length - 3]}..."
    return cleaned


def classify_pipeline_layer(pipeline_name: str) -> str:
    lowered = pipeline_name.lower()
    if any(tok in lowered for tok in ("minio", "oltp", "postgres", "ingest", "commute", "seed")):
        return "ingestion_oltp"
    if any(tok in lowered for tok in ("cdc", "debezium", "slack", "stream", "live_activity")):
        return "streaming_cdc"
    if "bronze" in lowered or "silver" in lowered:
        return "lakehouse_bronze_silver"
    if "gold" in lowered:
        return "lakehouse_gold"
    if "soda" in lowered or "quality" in lowered:
        return "quality_gate"
    if any(tok in lowered for tok in ("kestra", "orchestr", "monitoring")):
        return "orchestration"
    return "other"


def _is_layer_fresh(
    layer_group: str,
    freshness_age_minutes: float | None,
    *,
    sla_minutes: float,
) -> bool:
    if layer_group not in SCHEDULED_BATCH_LAYERS:
        return freshness_age_minutes is not None
    if freshness_age_minutes is None:
        return False
    return freshness_age_minutes <= sla_minutes


def ensure_monitoring_views(
    conn: Any,
    *,
    bi_reader_role: str = "sds_bi_reader",
) -> list[str]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", bi_reader_role):
        raise ValueError(f"Identifiant de role SQL invalide : {bi_reader_role!r}")

    deployed: list[str] = []
    cur = conn.cursor()
    try:
        for view_name, ddl_sql in MONITORING_VIEWS_DDL:
            cur.execute(ddl_sql)
            if view_name in BI_READER_MONITORING_VIEWS:
                cur.execute(
                    f"GRANT SELECT ON ops_monitoring.{view_name} TO {bi_reader_role}"
                )
            else:
                cur.execute(
                    f"REVOKE ALL ON ops_monitoring.{view_name} FROM {bi_reader_role}"
                )
            deployed.append(f"ops_monitoring.{view_name}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
    return deployed


def fetch_pipeline_health_metrics(
    conn: Any,
    *,
    freshness_sla_hours: float = 26.0,
) -> list[PipelineHealthMetric]:
    sla_minutes = freshness_sla_hours * 60.0
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                pipeline_name,
                layer_group,
                total_runs,
                succeeded_runs,
                failed_runs,
                success_rate_pct,
                latest_run_id,
                latest_status,
                latest_started_at,
                latest_finished_at,
                last_success_at,
                freshness_age_minutes,
                latest_rows_processed,
                cumulative_rows_processed,
                latest_anomalies_detected,
                latest_duration_ms,
                avg_duration_ms,
                max_duration_ms
            FROM ops_monitoring.v_pipeline_health_summary
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    metrics: list[PipelineHealthMetric] = []
    for row in rows:
        layer_group = str(row[1])
        age_min = _to_float(row[11])
        metrics.append(
            PipelineHealthMetric(
                pipeline_name=str(row[0]),
                layer_group=layer_group,
                total_runs=int(row[2]),
                succeeded_runs=int(row[3]),
                failed_runs=int(row[4]),
                success_rate_pct=float(_to_float(row[5]) or 0.0),
                latest_run_id=str(row[6]),
                latest_status=str(row[7]),
                latest_started_at=_to_iso_utc(row[8]),
                latest_finished_at=_to_iso_utc(row[9]),
                last_success_at=_to_iso_utc(row[10]),
                freshness_age_minutes=age_min,
                is_fresh=_is_layer_fresh(layer_group, age_min, sla_minutes=sla_minutes),
                latest_rows_processed=int(row[12]),
                cumulative_rows_processed=int(row[13]),
                latest_anomalies_detected=int(row[14]),
                latest_duration_ms=_to_int(row[15]),
                avg_duration_ms=_to_float(row[16]),
                max_duration_ms=_to_int(row[17]),
            )
        )
    return metrics


def fetch_layer_freshness_metrics(
    conn: Any,
    *,
    freshness_sla_hours: float = 26.0,
) -> list[LayerFreshnessMetric]:
    sla_minutes = freshness_sla_hours * 60.0
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                layer_group,
                pipelines_count,
                total_runs,
                succeeded_runs,
                failed_runs,
                layer_health_status,
                last_activity_at,
                last_success_at,
                freshness_age_minutes,
                latest_rows_processed,
                max_anomalies_detected,
                avg_duration_ms
            FROM ops_monitoring.v_layer_freshness_status
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    layers: list[LayerFreshnessMetric] = []
    for row in rows:
        layer_group = str(row[0])
        age_min = _to_float(row[8])
        layers.append(
            LayerFreshnessMetric(
                layer_group=layer_group,
                pipelines_count=int(row[1]),
                total_runs=int(row[2]),
                succeeded_runs=int(row[3]),
                failed_runs=int(row[4]),
                layer_health_status=str(row[5]),
                last_activity_at=_to_iso_utc(row[6]),
                last_success_at=_to_iso_utc(row[7]),
                freshness_age_minutes=age_min,
                is_fresh=_is_layer_fresh(layer_group, age_min, sla_minutes=sla_minutes),
                latest_rows_processed=int(row[9]),
                max_anomalies_detected=int(row[10]),
                avg_duration_ms=_to_float(row[11]),
            )
        )
    return layers


def fetch_quality_layer_summaries(conn: Any) -> list[QualityLayerSummary]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                layer_name,
                latest_run_id,
                total_checks,
                passed_checks,
                warned_checks,
                failed_checks,
                total_failed_rows,
                pass_rate_pct,
                evaluated_at
            FROM ops_monitoring.v_quality_layer_summary
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    return [
        QualityLayerSummary(
            layer_name=str(row[0]),
            latest_run_id=str(row[1]) if row[1] is not None else None,
            total_checks=int(row[2]),
            passed_checks=int(row[3]),
            warned_checks=int(row[4]),
            failed_checks=int(row[5]),
            total_failed_rows=int(row[6]),
            pass_rate_pct=float(_to_float(row[7]) or 0.0),
            evaluated_at=_to_iso_utc(row[8]),
        )
        for row in rows
    ]


def evaluate_platform_health(
    *,
    pipelines: list[PipelineHealthMetric],
    layers: list[LayerFreshnessMetric],
    quality_layers: list[QualityLayerSummary],
    freshness_sla_hours: float = 26.0,
    required_layers: tuple[str, ...] = (
        "lakehouse_bronze_silver",
        "lakehouse_gold",
        "quality_gate",
    ),
    generated_at: datetime | None = None,
) -> PlatformHealthReport:
    timestamp = (generated_at or datetime.now(UTC)).astimezone(UTC).isoformat()

    present_layers = {layer.layer_group for layer in layers if layer.succeeded_runs > 0}
    missing_required_layers = [
        req for req in required_layers if req not in present_layers
    ]

    failing_pipelines = [
        p.pipeline_name for p in pipelines if p.latest_status.upper() != "SUCCESS"
    ]
    stale_pipelines = [
        p.pipeline_name
        for p in pipelines
        if p.layer_group in SCHEDULED_BATCH_LAYERS and not p.is_fresh
    ]

    total_quality_checks = sum(q.total_checks for q in quality_layers)
    passed_quality_checks = sum(q.passed_checks for q in quality_layers)
    warned_quality_checks = sum(q.warned_checks for q in quality_layers)
    failed_quality_checks = sum(q.failed_checks for q in quality_layers)
    quality_gate_passed = total_quality_checks > 0 and failed_quality_checks == 0

    isolated_business_anomalies = max(
        (
            p.latest_anomalies_detected
            for p in pipelines
            if p.layer_group in ("lakehouse_bronze_silver", "lakehouse_gold", "ingestion_oltp")
        ),
        default=0,
    )

    if failing_pipelines or missing_required_layers or not quality_gate_passed:
        overall_status: OverallHealthStatus = "CRITICAL"
    elif stale_pipelines or warned_quality_checks > 0:
        overall_status = "DEGRADED"
    else:
        overall_status = "HEALTHY"

    return PlatformHealthReport(
        generated_at=timestamp,
        overall_status=overall_status,
        freshness_sla_hours=freshness_sla_hours,
        pipelines=pipelines,
        layers=layers,
        quality_layers=quality_layers,
        quality_gate_passed=quality_gate_passed,
        total_quality_checks=total_quality_checks,
        passed_quality_checks=passed_quality_checks,
        warned_quality_checks=warned_quality_checks,
        failed_quality_checks=failed_quality_checks,
        isolated_business_anomalies=isolated_business_anomalies,
        missing_required_layers=missing_required_layers,
        failing_pipelines=failing_pipelines,
        stale_pipelines=stale_pipelines,
    )


def _derive_layers_from_pipelines(
    pipelines: list[PipelineHealthMetric],
    *,
    freshness_sla_hours: float,
) -> list[LayerFreshnessMetric]:
    sla_minutes = freshness_sla_hours * 60.0
    grouped: dict[str, list[PipelineHealthMetric]] = {}
    for item in pipelines:
        grouped.setdefault(item.layer_group, []).append(item)

    derived: list[LayerFreshnessMetric] = []
    for layer_group, items in grouped.items():
        ages = [p.freshness_age_minutes for p in items if p.freshness_age_minutes is not None]
        max_age = max(ages) if ages else None
        if any(p.latest_status.upper() != "SUCCESS" for p in items):
            status = "CRITICAL"
        elif any(p.last_success_at is None for p in items):
            status = "DEGRADED"
        else:
            status = "HEALTHY"
        durations = [p.avg_duration_ms for p in items if p.avg_duration_ms is not None]
        avg_dur = round(sum(durations) / len(durations), 2) if durations else None
        successes = [p.last_success_at for p in items if p.last_success_at is not None]
        activities = [
            p.latest_finished_at or p.latest_started_at
            for p in items
            if (p.latest_finished_at or p.latest_started_at) is not None
        ]
        derived.append(
            LayerFreshnessMetric(
                layer_group=layer_group,
                pipelines_count=len(items),
                total_runs=sum(p.total_runs for p in items),
                succeeded_runs=sum(p.succeeded_runs for p in items),
                failed_runs=sum(p.failed_runs for p in items),
                layer_health_status=status,
                last_activity_at=max(activities) if activities else None,
                last_success_at=max(successes) if successes else None,
                freshness_age_minutes=max_age,
                is_fresh=_is_layer_fresh(layer_group, max_age, sla_minutes=sla_minutes),
                latest_rows_processed=sum(p.latest_rows_processed for p in items),
                max_anomalies_detected=max(p.latest_anomalies_detected for p in items),
                avg_duration_ms=avg_dur,
            )
        )
    return derived


def build_platform_health_report(
    conn: Any,
    *,
    settings: EnvSettings | None = None,
    freshness_sla_hours: float | None = None,
) -> PlatformHealthReport:
    cfg = settings or EnvSettings()
    sla = (
        freshness_sla_hours
        if freshness_sla_hours is not None
        else cfg.monitoring_freshness_sla_hours
    )
    pipelines = fetch_pipeline_health_metrics(conn, freshness_sla_hours=sla)
    try:
        layers = fetch_layer_freshness_metrics(conn, freshness_sla_hours=sla)
    except pg8000.dbapi.DatabaseError as exc:
        if "42501" not in str(exc):
            raise
        conn.rollback()
        layers = _derive_layers_from_pipelines(pipelines, freshness_sla_hours=sla)
    quality_layers = fetch_quality_layer_summaries(conn)
    return evaluate_platform_health(
        pipelines=pipelines,
        layers=layers,
        quality_layers=quality_layers,
        freshness_sla_hours=sla,
        required_layers=cfg.monitoring_required_layers,
    )


def record_monitoring_run(
    conn: Any,
    *,
    report: PlatformHealthReport,
    started_at: datetime,
    duration_ms: int,
) -> str:
    run_id = str(uuid.uuid4())
    finished_at = datetime.now(UTC)
    run_status = "FAILED" if report.overall_status == "CRITICAL" else "SUCCESS"
    rows_processed = sum(
        p.latest_rows_processed
        for p in report.pipelines
        if p.pipeline_name != MONITORING_PIPELINE_NAME
    )
    details_payload = {
        "overall_status": report.overall_status,
        "freshness_sla_hours": report.freshness_sla_hours,
        "quality_gate_passed": report.quality_gate_passed,
        "total_quality_checks": report.total_quality_checks,
        "passed_quality_checks": report.passed_quality_checks,
        "failed_quality_checks": report.failed_quality_checks,
        "failing_pipelines": report.failing_pipelines,
        "stale_pipelines": report.stale_pipelines,
        "isolated_business_anomalies": report.isolated_business_anomalies,
    }

    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO ops_monitoring.pipeline_runs (
                run_id,
                pipeline_name,
                started_at,
                finished_at,
                status,
                rows_processed,
                anomalies_detected,
                duration_ms,
                details
            )
            VALUES (
                CAST(%s AS uuid),
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                CAST(%s AS jsonb)
            )
            """,
            (
                run_id,
                MONITORING_PIPELINE_NAME,
                started_at,
                finished_at,
                run_status,
                rows_processed,
                report.isolated_business_anomalies,
                max(duration_ms, 0),
                json.dumps(details_payload, ensure_ascii=False),
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
    return run_id


def consolidate_monitoring(
    *,
    settings: EnvSettings | None = None,
    conn: Any | None = None,
    role: DbRole = "pipeline",
    ensure_views: bool = True,
    record_run: bool = False,
    freshness_sla_hours: float | None = None,
) -> PlatformHealthReport:
    cfg = settings or EnvSettings()
    owns_conn = conn is None
    active_conn = (
        pg8000.dbapi.connect(**cfg.pg8000_connect_kwargs(role))
        if owns_conn
        else conn
    )
    started_at = datetime.now(UTC)
    t0 = time.perf_counter()

    try:
        if ensure_views and role != "bi_reader":
            ensure_monitoring_views(active_conn, bi_reader_role=cfg.sds_bi_reader_user)

        report = build_platform_health_report(
            active_conn,
            settings=cfg,
            freshness_sla_hours=freshness_sla_hours,
        )

        if record_run and role != "bi_reader":
            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            record_monitoring_run(
                active_conn,
                report=report,
                started_at=started_at,
                duration_ms=elapsed_ms,
            )
            report = build_platform_health_report(
                active_conn,
                settings=cfg,
                freshness_sla_hours=freshness_sla_hours,
            )
        return report
    finally:
        if owns_conn:
            active_conn.close()


def format_health_report_text(report: PlatformHealthReport) -> str:
    lines = [
        f"[SUPERVISION PLATEFORME SDS] Statut global : {report.overall_status}",
        (
            f"Horodatage UTC : {report.generated_at} | "
            f"SLA fraicheur batch : {report.freshness_sla_hours:.1f} h"
        ),
        (
            f"Portail Qualite Soda : {report.passed_quality_checks}/{report.total_quality_checks} "
            f"PASS (echecs={report.failed_quality_checks}, alertes={report.warned_quality_checks}) "
            f"| Anomalies declaratives isolees : {report.isolated_business_anomalies}"
        ),
        "--- Pipelines ---",
    ]
    for p in report.pipelines:
        age_txt = (
            f"{p.freshness_age_minutes:.1f} min"
            if p.freshness_age_minutes is not None
            else "N/A"
        )
        dur_txt = f"{p.latest_duration_ms} ms" if p.latest_duration_ms is not None else "N/A"
        lines.append(
            f"  - {p.pipeline_name} [{p.layer_group}] : dernier={p.latest_status}, "
            f"succes={p.succeeded_runs}/{p.total_runs} ({p.success_rate_pct:.1f}%), "
            f"lignes={p.latest_rows_processed}, latence={dur_txt}, age={age_txt}"
        )
    if report.failing_pipelines:
        lines.append(f"Pipelines en echec : {', '.join(report.failing_pipelines)}")
    if report.stale_pipelines:
        lines.append(f"Pipelines hors SLA fraicheur : {', '.join(report.stale_pipelines)}")
    if report.missing_required_layers:
        lines.append(f"Couches requises absentes : {', '.join(report.missing_required_layers)}")
    return "\n".join(lines)


def format_pipeline_alert_message(
    *,
    failed_step: str,
    execution_id: str,
    error_summary: str,
) -> str:
    safe_step = sanitize_monitoring_text(failed_step, max_length=80)
    safe_exec = sanitize_monitoring_text(execution_id, max_length=80)
    safe_err = sanitize_monitoring_text(error_summary, max_length=240)
    return (
        f"[ALERTE PIPELINE SDS] Echec detecte sur l'etape '{safe_step}' "
        f"(execution={safe_exec}). Detail technique : {safe_err}"
    )


def send_pipeline_slack_alert(
    *,
    failed_step: str,
    execution_id: str = "manual",
    error_summary: str = "Echec detecte lors de l'execution du pipeline quotidien.",
    settings: EnvSettings | None = None,
    http_client: httpx.Client | None = None,
) -> PipelineAlertResult:
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    cfg = settings or EnvSettings()
    message = format_pipeline_alert_message(
        failed_step=failed_step,
        execution_id=execution_id,
        error_summary=error_summary,
    )

    if not cfg.has_valid_slack_webhook():
        logger.info(
            "Alerte Slack en mode dry_run (%s) : %s",
            cfg.slack_channel_name,
            message,
        )
        return PipelineAlertResult(
            delivered=False,
            dry_run=True,
            status_code=None,
            channel=cfg.slack_channel_name,
            message=message,
        )

    webhook_url = cfg.slack_webhook_url.get_secret_value().strip()
    payload = {"text": message}
    owns_client = http_client is None
    client = (
        httpx.Client(timeout=cfg.http_timeout_seconds)
        if owns_client
        else http_client
    )

    last_status: int | None = None
    try:
        for attempt in range(1, cfg.http_max_retries + 1):
            try:
                response = client.post(webhook_url, json=payload)
                last_status = response.status_code
                if 200 <= response.status_code < 300:
                    return PipelineAlertResult(
                        delivered=True,
                        dry_run=False,
                        status_code=response.status_code,
                        channel=cfg.slack_channel_name,
                        message=message,
                    )
                if response.status_code != 429 and response.status_code < 500:
                    break
            except httpx.HTTPError as exc:
                logger.warning(
                    "Tentative %d/%d d'envoi d'alerte Slack echouee (%s).",
                    attempt,
                    cfg.http_max_retries,
                    type(exc).__name__,
                )
            if attempt < cfg.http_max_retries:
                time.sleep(cfg.http_retry_backoff_seconds * (2 ** (attempt - 1)))
    finally:
        if owns_client:
            client.close()

    return PipelineAlertResult(
        delivered=False,
        dry_run=False,
        status_code=last_status,
        channel=cfg.slack_channel_name,
        message=message,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Supervision consolidee et vues analytiques ops_monitoring (SDS)."
    )
    parser.add_argument(
        "--role",
        choices=("pipeline", "bi_reader", "admin"),
        default="pipeline",
        help="Role PostgreSQL utilise pour l'audit de supervision.",
    )
    parser.add_argument(
        "--skip-views",
        action="store_true",
        help="Ne pas redeployer les vues SQL dans ops_monitoring.",
    )
    parser.add_argument(
        "--record-run",
        action="store_true",
        help="Enregistre la consolidation dans ops_monitoring.pipeline_runs.",
    )
    parser.add_argument(
        "--sla-hours",
        type=float,
        default=None,
        help="Surcharge du SLA de fraicheur en heures pour les couches batch.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Affiche le rapport de sante au format JSON.",
    )
    parser.add_argument(
        "--fail-on-critical",
        action="store_true",
        help="Retourne un code de sortie non nul si le statut global est CRITICAL.",
    )
    parser.add_argument(
        "--alert-on-critical",
        action="store_true",
        help="Emet une alerte Slack (ou dry_run) si le statut global est CRITICAL.",
    )
    parser.add_argument(
        "--send-failure-alert",
        action="store_true",
        help="Emet directement une alerte Slack d'echec d'orchestration.",
    )
    parser.add_argument(
        "--failed-step",
        default="daily_benefits_pipeline",
        help="Nom de l'etape en echec pour la notification Slack.",
    )
    parser.add_argument(
        "--execution-id",
        default="manual",
        help="Identifiant d'execution d'orchestration associe a l'alerte.",
    )
    parser.add_argument(
        "--error-summary",
        default="Echec detecte lors de l'execution du pipeline quotidien.",
        help="Resume technique sans PII de l'erreur rencontree.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    args = parse_args(argv)
    settings = EnvSettings()

    if args.send_failure_alert:
        result = send_pipeline_slack_alert(
            failed_step=args.failed_step,
            execution_id=args.execution_id,
            error_summary=args.error_summary,
            settings=settings,
        )
        if args.as_json:
            print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
        else:
            mode = (
                "DRY_RUN"
                if result.dry_run
                else ("DELIVERED" if result.delivered else "FAILED")
            )
            print(
                f"[ALERTE SLACK] mode={mode} status_code={result.status_code} | "
                f"{result.message}"
            )
        return 0 if (result.delivered or result.dry_run) else 1

    report = consolidate_monitoring(
        settings=settings,
        role=args.role,
        ensure_views=not args.skip_views,
        record_run=args.record_run,
        freshness_sla_hours=args.sla_hours,
    )

    if args.as_json:
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    else:
        print(format_health_report_text(report))

    if args.alert_on_critical and report.overall_status == "CRITICAL":
        failing = ", ".join(report.failing_pipelines) or "qualite/couches"
        summary_msg = (
            f"Statut plateforme CRITICAL (echecs Soda={report.failed_quality_checks})"
        )
        send_pipeline_slack_alert(
            failed_step=failing,
            execution_id=args.execution_id,
            error_summary=summary_msg,
            settings=settings,
        )

    if args.fail_on_critical and report.overall_status == "CRITICAL":
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
