import argparse
import json
import logging
import signal
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

import pg8000.dbapi
from confluent_kafka import Consumer, KafkaError

from src.settings import EnvSettings
from src.streaming.slack_notifier import ActivityEvent, SlackNotifier

logger = logging.getLogger(__name__)

PIPELINE_NAME = "cdc_slack_activity_consumer"
SNAPSHOT_FLAGS = frozenset(
    {"true", "first", "first_in_data_collection", "last_in_data_collection", "last"}
)
FilterDecision = Literal[
    "notify_live_event",
    "skip_tombstone",
    "skip_snapshot",
    "skip_non_create",
    "skip_historical_seed",
    "skip_malformed",
]


@dataclass
class ConsumerMetrics:
    messages_consumed: int = 0
    snapshot_skipped: int = 0
    non_create_skipped: int = 0
    historical_seed_skipped: int = 0
    duplicate_skipped: int = 0
    malformed_skipped: int = 0
    live_events_notified: int = 0
    slack_http_delivered: int = 0
    slack_dry_run_logged: int = 0
    delivery_errors: int = 0
    notified_activity_ids: list[int] = field(default_factory=list)

    def to_monitoring_details(self, *, topic: str, consumer_group: str) -> dict[str, Any]:
        """Exporte uniquement des compteurs et identifiants techniques (zero PII)."""
        return {
            "topic": topic,
            "consumer_group": consumer_group,
            "messages_consumed": self.messages_consumed,
            "snapshot_skipped": self.snapshot_skipped,
            "non_create_skipped": self.non_create_skipped,
            "historical_seed_skipped": self.historical_seed_skipped,
            "duplicate_skipped": self.duplicate_skipped,
            "malformed_skipped": self.malformed_skipped,
            "live_events_notified": self.live_events_notified,
            "slack_http_delivered": self.slack_http_delivered,
            "slack_dry_run_logged": self.slack_dry_run_logged,
            "delivery_errors": self.delivery_errors,
            "notified_activity_ids": self.notified_activity_ids[-50:],
        }


def _parse_optional_timestamp(raw_val: Any) -> datetime | None:
    if raw_val is None:
        return None
    if isinstance(raw_val, int | float):
        # Debezium peut representer les horodatages en microsecondes ou millisecondes selon le mode
        seconds = raw_val / 1_000_000.0 if raw_val > 10_000_000_000_000 else raw_val / 1000.0
        return datetime.fromtimestamp(seconds, tz=UTC)
    if isinstance(raw_val, str):
        cleaned = raw_val.strip()
        if not cleaned:
            return None
        if cleaned.endswith("Z"):
            cleaned = f"{cleaned[:-1]}+00:00"
        return datetime.fromisoformat(cleaned)
    return None


def evaluate_cdc_payload(
    raw_value: bytes | str | None,
) -> tuple[FilterDecision, ActivityEvent | None]:
    """Decode une enveloppe Debezium et applique le coupe-circuit anti-spam sur l'historique."""
    if raw_value is None:
        return ("skip_tombstone", None)

    text_payload = (
        raw_value.decode("utf-8", errors="replace").strip()
        if isinstance(raw_value, bytes)
        else raw_value.strip()
    )
    if not text_payload:
        return ("skip_tombstone", None)

    try:
        envelope = json.loads(text_payload)
    except json.JSONDecodeError:
        return ("skip_malformed", None)

    if not isinstance(envelope, dict):
        return ("skip_malformed", None)

    if "op" not in envelope and isinstance(envelope.get("payload"), dict):
        envelope = envelope["payload"]

    op = str(envelope.get("op") or "")
    source = envelope.get("source")
    snapshot_marker = ""
    if isinstance(source, dict) and source.get("snapshot") is not None:
        snapshot_marker = str(source.get("snapshot")).strip().lower()

    if op == "r" or snapshot_marker in SNAPSHOT_FLAGS:
        return ("skip_snapshot", None)

    if op != "c":
        return ("skip_non_create", None)

    after = envelope.get("after")
    if not isinstance(after, dict):
        return ("skip_malformed", None)

    ingestion_mode = str(after.get("ingestion_mode") or "").strip()
    if ingestion_mode != "live_event":
        return ("skip_historical_seed", None)

    try:
        activity = ActivityEvent(
            activity_id=int(after["activity_id"]),
            employee_id=int(after["employee_id"]),
            employee_full_name=str(after["employee_full_name"]),
            sport_type=str(after["sport_type"]),
            activity_start=_parse_optional_timestamp(after.get("activity_start")),
            elapsed_seconds=int(after["elapsed_seconds"]),
            distance_m=(
                int(after["distance_m"]) if after.get("distance_m") is not None else None
            ),
            comment=str(after["comment"]) if after.get("comment") is not None else None,
            ingestion_mode=ingestion_mode,
        )
    except (KeyError, TypeError, ValueError):
        return ("skip_malformed", None)

    return ("notify_live_event", activity)


def _record_consumer_run(
    conn: pg8000.dbapi.Connection,
    *,
    started_at: datetime,
    status: str,
    metrics: ConsumerMetrics,
    topic: str,
    consumer_group: str,
) -> None:
    finished_at = datetime.now(UTC)
    duration_ms = max(0, int((finished_at - started_at).total_seconds() * 1000))
    anomalies = metrics.malformed_skipped + metrics.delivery_errors
    details = metrics.to_monitoring_details(topic=topic, consumer_group=consumer_group)

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
                metrics.messages_consumed,
                anomalies,
                duration_ms,
                json.dumps(details, ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        cur.close()


def build_kafka_consumer(settings: EnvSettings, group_id: str | None = None) -> Consumer:
    effective_group = group_id or settings.redpanda_consumer_group
    conf = {
        "bootstrap.servers": settings.redpanda_brokers,
        "group.id": effective_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "log_level": 2,
    }
    return Consumer(conf)


def consume_activity_stream(
    settings: EnvSettings | None = None,
    *,
    consumer: Any | None = None,
    notifier: SlackNotifier | None = None,
    pg_conn: pg8000.dbapi.Connection | None = None,
    record_monitoring: bool = True,
    group_id: str | None = None,
    poll_timeout_seconds: float = 0.5,
    max_idle_seconds: float | None = None,
    max_messages: int | None = None,
    max_live_events: int | None = None,
) -> ConsumerMetrics:
    """Consomme le topic CDC Redpanda, filtre l'historique et notifie les evenements live."""
    cfg = settings or EnvSettings()
    effective_group = group_id or cfg.redpanda_consumer_group
    topic = cfg.redpanda_activity_topic
    started_at = datetime.now(UTC)

    owns_consumer = consumer is None
    owns_notifier = notifier is None
    owns_conn = pg_conn is None and record_monitoring

    kafka_consumer = consumer or build_kafka_consumer(cfg, effective_group)
    slack_notifier = notifier or SlackNotifier(cfg)
    conn = (
        pg8000.dbapi.connect(**cfg.pg8000_connect_kwargs("pipeline"))
        if owns_conn
        else pg_conn
    )

    metrics = ConsumerMetrics()
    seen_activity_ids: set[int] = set()
    stop_requested = False

    def _handle_stop(signum, frame) -> None:
        nonlocal stop_requested
        stop_requested = True

    prev_sigint = signal.getsignal(signal.SIGINT)
    prev_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    run_status = "SUCCESS"
    try:
        kafka_consumer.subscribe([topic])
        last_message_ts = time.monotonic()

        while not stop_requested:
            if max_messages is not None and metrics.messages_consumed >= max_messages:
                break
            if max_live_events is not None and metrics.live_events_notified >= max_live_events:
                break

            msg = kafka_consumer.poll(poll_timeout_seconds)
            if msg is None:
                if (
                    max_idle_seconds is not None
                    and (time.monotonic() - last_message_ts) >= max_idle_seconds
                ):
                    break
                continue

            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Erreur consommateur Redpanda : {err}")

            last_message_ts = time.monotonic()
            metrics.messages_consumed += 1

            decision, activity = evaluate_cdc_payload(msg.value())
            if decision in ("skip_tombstone", "skip_non_create"):
                metrics.non_create_skipped += 1
                kafka_consumer.commit(message=msg, asynchronous=False)
                continue

            if decision == "skip_snapshot":
                metrics.snapshot_skipped += 1
                kafka_consumer.commit(message=msg, asynchronous=False)
                continue

            if decision == "skip_historical_seed":
                metrics.historical_seed_skipped += 1
                kafka_consumer.commit(message=msg, asynchronous=False)
                continue

            if decision == "skip_malformed" or activity is None:
                metrics.malformed_skipped += 1
                logger.warning("Enveloppe CDC malformee ignoree sur le topic %s", topic)
                kafka_consumer.commit(message=msg, asynchronous=False)
                continue

            if activity.activity_id in seen_activity_ids:
                metrics.duplicate_skipped += 1
                kafka_consumer.commit(message=msg, asynchronous=False)
                continue

            result = slack_notifier.notify_activity(activity)
            seen_activity_ids.add(activity.activity_id)
            metrics.live_events_notified += 1
            metrics.notified_activity_ids.append(activity.activity_id)
            if result.mode == "webhook_http" and result.delivered:
                metrics.slack_http_delivered += 1
            elif result.mode == "dry_run":
                metrics.slack_dry_run_logged += 1

            kafka_consumer.commit(message=msg, asynchronous=False)

        return metrics
    except Exception:
        run_status = "FAILED"
        metrics.delivery_errors += 1
        raise
    finally:
        signal.signal(signal.SIGINT, prev_sigint)
        signal.signal(signal.SIGTERM, prev_sigterm)

        if record_monitoring and conn is not None:
            try:
                _record_consumer_run(
                    conn,
                    started_at=started_at,
                    status=run_status,
                    metrics=metrics,
                    topic=topic,
                    consumer_group=effective_group,
                )
            except Exception:
                logger.warning("Echec de journalisation du run CDC dans pipeline_runs")

        if owns_consumer:
            kafka_consumer.close()
        if owns_notifier:
            slack_notifier.close()
        if owns_conn and conn is not None:
            conn.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consommateur CDC temps reel Redpanda vers Slack pour Sport Data Solution."
    )
    parser.add_argument(
        "--group-id",
        type=str,
        default=None,
        help="Identifiant du groupe de consommateurs Kafka/Redpanda.",
    )
    parser.add_argument(
        "--max-idle-seconds",
        type=float,
        default=None,
        help="Arret propre apres N secondes sans nouveau message (mode drain/recette).",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Plafond total de messages consommes avant arret.",
    )
    parser.add_argument(
        "--max-live-events",
        type=int,
        default=None,
        help="Arret automatique apres N notifications live_event traitees.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    args = parse_args(argv)
    cfg = EnvSettings()
    summary = consume_activity_stream(
        cfg,
        group_id=args.group_id,
        max_idle_seconds=args.max_idle_seconds,
        max_messages=args.max_messages,
        max_live_events=args.max_live_events,
    )
    details = summary.to_monitoring_details(
        topic=cfg.redpanda_activity_topic,
        consumer_group=args.group_id or cfg.redpanda_consumer_group,
    )
    logger.info("Bilan consommateur CDC : %s", json.dumps(details, ensure_ascii=False))


if __name__ == "__main__":
    main()