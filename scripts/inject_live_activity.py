import argparse
import json
import logging
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pg8000.dbapi

from src.settings import BusinessRules, EnvSettings, load_business_rules

logger = logging.getLogger(__name__)

PIPELINE_NAME = "live_activity_injection"
DEFAULT_SPEED_KMH: dict[str, float] = {
    "Course à pied": 10.8,
    "Randonnée": 4.5,
    "Vélo de route": 26.0,
    "VTT": 17.5,
    "Natation": 2.6,
    "Marche nordique": 5.8,
    "Trottinette": 14.0,
}


def resolve_employee_identity(
    conn: pg8000.dbapi.Connection,
    employee_id: int | None = None,
) -> tuple[int, str, str | None]:
    """Resout l'identite et le sport habituel sans exposer les colonnes salariales ou d'adresse."""
    cur = conn.cursor()
    try:
        if employee_id is not None:
            cur.execute(
                """
                SELECT employee_id, employee_full_name, sport_type
                FROM oltp_sports.sport_activities
                WHERE employee_id = %s
                ORDER BY activity_start DESC
                LIMIT 1;
                """,
                (employee_id,),
            )
            existing = cur.fetchone()
            if existing:
                return (int(existing[0]), str(existing[1]), str(existing[2]))

            cur.execute(
                """
                SELECT employee_id, first_name, last_name
                FROM hr_restricted.employees_audit
                WHERE employee_id = %s;
                """,
                (employee_id,),
            )
            hr_row = cur.fetchone()
            if not hr_row:
                raise ValueError(f"Collaborateur introuvable pour employee_id={employee_id}")
            full_name = f"{hr_row[1]} {hr_row[2]}".strip()
            return (int(hr_row[0]), full_name, None)

        cur.execute(
            """
            SELECT employee_id, employee_full_name, sport_type
            FROM oltp_sports.sport_activities
            ORDER BY employee_id ASC, activity_start DESC
            LIMIT 1;
            """
        )
        first_sport_row = cur.fetchone()
        if first_sport_row:
            return (
                int(first_sport_row[0]),
                str(first_sport_row[1]),
                str(first_sport_row[2]),
            )

        cur.execute(
            """
            SELECT employee_id, first_name, last_name
            FROM hr_restricted.employees_audit
            ORDER BY employee_id ASC
            LIMIT 1;
            """
        )
        fallback_row = cur.fetchone()
        if not fallback_row:
            raise RuntimeError("Aucun collaborateur disponible en base pour l'injection live")
        return (
            int(fallback_row[0]),
            f"{fallback_row[1]} {fallback_row[2]}".strip(),
            None,
        )
    finally:
        cur.close()


def prepare_live_activity_parameters(
    rules: BusinessRules,
    *,
    raw_sport: str,
    duration_minutes: float,
    distance_km: float | None = None,
    comment: str | None = None,
) -> tuple[str, int, int | None, str | None]:
    if duration_minutes <= 0:
        raise ValueError(f"La duree doit etre strictement positive : {duration_minutes}")

    normalized_sport = rules.normalize_sport(raw_sport)
    needs_distance = rules.requires_distance(normalized_sport)
    elapsed_seconds = max(60, int(round(duration_minutes * 60)))

    if not needs_distance:
        if distance_km is not None and distance_km > 0:
            raise ValueError(
                f"La discipline {normalized_sport!r} ne prend pas en charge de distance metrique"
            )
        resolved_distance_m = None
    else:
        if distance_km is not None:
            if distance_km <= 0:
                raise ValueError("La distance doit etre strictement positive")
            resolved_distance_m = int(round(distance_km * 1000))
        else:
            speed_kmh = DEFAULT_SPEED_KMH.get(normalized_sport, 10.0)
            estimated_km = speed_kmh * (elapsed_seconds / 3600.0)
            resolved_distance_m = max(100, int(round(estimated_km * 1000)))

    cleaned_comment = comment.strip() if comment and comment.strip() else None
    return (normalized_sport, elapsed_seconds, resolved_distance_m, cleaned_comment)


def insert_unit_live_activity(
    conn: pg8000.dbapi.Connection,
    rules: BusinessRules,
    *,
    employee_id: int | None = None,
    sport: str | None = None,
    duration_minutes: float = 45.0,
    distance_km: float | None = None,
    comment: str | None = "Séance collective enregistrée en temps réel",
) -> dict[str, Any]:
    started_run = datetime.now(UTC)
    emp_id, full_name, default_sport = resolve_employee_identity(conn, employee_id)
    chosen_sport = sport or default_sport or "Course à pied"

    normalized_sport, elapsed_seconds, distance_m, cleaned_comment = (
        prepare_live_activity_parameters(
            rules,
            raw_sport=chosen_sport,
            duration_minutes=duration_minutes,
            distance_km=distance_km,
            comment=comment,
        )
    )

    activity_end = datetime.now(UTC)
    activity_start = activity_end - timedelta(seconds=elapsed_seconds)

    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO oltp_sports.sport_activities (
                employee_id, employee_full_name, sport_type,
                activity_start, activity_end, elapsed_seconds,
                distance_m, comment, ingestion_mode, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'live_event', %s)
            RETURNING activity_id;
            """,
            (
                emp_id,
                full_name,
                normalized_sport,
                activity_start,
                activity_end,
                elapsed_seconds,
                distance_m,
                cleaned_comment,
                activity_end,
            ),
        )
        row = cur.fetchone()
        activity_id = int(row[0])

        finished_run = datetime.now(UTC)
        duration_ms = max(0, int((finished_run - started_run).total_seconds() * 1000))
        monitoring_details = {
            "activity_id": activity_id,
            "employee_id": emp_id,
            "sport_type": normalized_sport,
            "elapsed_seconds": elapsed_seconds,
            "distance_m": distance_m,
            "ingestion_mode": "live_event",
        }
        cur.execute(
            """
            INSERT INTO ops_monitoring.pipeline_runs (
                run_id, pipeline_name, started_at, finished_at,
                status, rows_processed, anomalies_detected, duration_ms, details
            )
            VALUES (%s, %s, %s, %s, 'SUCCESS', 1, 0, %s, CAST(%s AS jsonb));
            """,
            (
                str(uuid.uuid4()),
                PIPELINE_NAME,
                started_run,
                finished_run,
                duration_ms,
                json.dumps(monitoring_details, ensure_ascii=False),
            ),
        )
        conn.commit()
        return monitoring_details
    finally:
        cur.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Injection d'activites sportives temps reel (live_event) dans PostgreSQL."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Nombre d'activites aleatoires a generer via postgres_generator (--live-events).",
    )
    parser.add_argument(
        "--employee-id",
        type=int,
        default=None,
        help="Identifiant du collaborateur pour une injection unitaire ciblee.",
    )
    parser.add_argument(
        "--sport",
        type=str,
        default=None,
        help="Discipline sportive (normalisee automatiquement selon business_rules.yaml).",
    )
    parser.add_argument(
        "--duration-minutes",
        type=float,
        default=45.0,
        help="Duree de la seance en minutes (mode unitaire).",
    )
    parser.add_argument(
        "--distance-km",
        type=float,
        default=None,
        help="Distance parcourue en kilometres (uniquement pour les sports d'endurance).",
    )
    parser.add_argument(
        "--comment",
        type=str,
        default="Séance collective enregistrée en temps réel",
        help="Commentaire associe a l'activite.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    args = parse_args(argv)
    has_custom_override = any(
        val is not None for val in (args.employee_id, args.sport, args.distance_km)
    )

    if args.count > 1 and not has_custom_override:
        cmd = [
            sys.executable,
            "-m",
            "src.ingestion.postgres_generator",
            "--live-events",
            str(args.count),
        ]
        subprocess.run(cmd, check=True)
        return

    settings = EnvSettings()
    rules = load_business_rules()
    conn = pg8000.dbapi.connect(**settings.pg8000_connect_kwargs("pipeline"))
    try:
        summary = insert_unit_live_activity(
            conn,
            rules,
            employee_id=args.employee_id,
            sport=args.sport,
            duration_minutes=args.duration_minutes,
            distance_km=args.distance_km,
            comment=args.comment,
        )
        logger.info(
            "Activite temps reel injectee : %s",
            json.dumps(summary, ensure_ascii=False),
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()