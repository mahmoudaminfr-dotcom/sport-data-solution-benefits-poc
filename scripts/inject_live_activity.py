import argparse
import json
import logging
import random
import sys
import time
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
    "Cyclisme": 24.0,
    "Triathlon": 18.5,
    "Vélo de route": 26.0,
    "VTT": 17.5,
    "Natation": 2.6,
    "Voile": 11.0,
    "Équitation": 12.0,
    "Marche nordique": 5.8,
    "Trottinette": 14.0,
}
DEMO_SPORTS: tuple[str, ...] = (
    "Course à pied",
    "Randonnée",
    "Cyclisme",
    "Natation",
    "Triathlon",
    "Tennis",
    "Badminton",
    "Escalade",
    "Football",
    "Basketball",
    "Rugby",
)
DEMO_COMMENTS: tuple[str, ...] = (
    "Séance collective à la pause déjeuner",
    "Sortie sportive après le travail",
    "Entraînement régulier de la semaine",
    "Session matinale avant la prise de poste",
    "Sortie entre collègues en fin de journée",
)


def resolve_employee_identity(
    conn: pg8000.dbapi.Connection,
    employee_id: int | None = None,
) -> tuple[int, str, str | None]:
    """Resout l'identite et le sport habituel sans exposer salaire ni adresse."""
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
                raise ValueError(
                    f"Collaborateur introuvable pour employee_id={employee_id}"
                )
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
            raise RuntimeError(
                "Aucun collaborateur disponible en base pour l'injection live"
            )
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
        raise ValueError(
            f"La duree doit etre strictement positive : {duration_minutes}"
        )

    normalized_sport = rules.normalize_sport(raw_sport)
    needs_distance = rules.requires_distance(normalized_sport)
    elapsed_seconds = max(60, int(round(duration_minutes * 60)))

    if not needs_distance:
        if distance_km is not None and distance_km > 0:
            raise ValueError(
                f"La discipline {normalized_sport!r} ne prend pas en charge "
                "de distance metrique"
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


def list_employee_ids(conn: pg8000.dbapi.Connection) -> list[int]:
    """Recupere la liste des identifiants collaborateurs pour le tirage aleatoire."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT employee_id FROM hr_restricted.employees_audit "
            "ORDER BY employee_id ASC;"
        )
        rows = cur.fetchall()
        if rows:
            return [int(r[0]) for r in rows]

        cur.execute(
            "SELECT DISTINCT employee_id FROM oltp_sports.sport_activities "
            "ORDER BY employee_id ASC;"
        )
        fallback = cur.fetchall()
        if not fallback:
            raise RuntimeError("Aucun collaborateur disponible en base")
        return [int(r[0]) for r in fallback]
    finally:
        cur.close()


def reset_live_activities(conn: pg8000.dbapi.Connection) -> int:
    """Supprime uniquement les activites temps reel (live_event) de demonstration."""
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM oltp_sports.sport_activities "
            "WHERE ingestion_mode = 'live_event';"
        )
        deleted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        conn.commit()
        return int(deleted)
    finally:
        cur.close()


def stream_live_activities(
    conn: pg8000.dbapi.Connection,
    rules: BusinessRules,
    *,
    interval_seconds: float = 3.0,
    max_events: int | None = None,
) -> int:
    """Genere automatiquement des activites salaries a intervalle regulier."""
    employee_ids = list_employee_ids(conn)
    pause = max(0.5, interval_seconds)
    injected = 0

    logger.info(
        "Flux automatique demarre (1 activite toutes les %.1fs, Ctrl+C pour arreter)...",
        pause,
    )
    try:
        while max_events is None or injected < max_events:
            emp_id = random.choice(employee_ids)
            sport = random.choice(DEMO_SPORTS)
            duration = float(random.randint(25, 75))
            comment = random.choice(DEMO_COMMENTS)

            details = insert_unit_live_activity(
                conn,
                rules,
                employee_id=emp_id,
                sport=sport,
                duration_minutes=duration,
                comment=comment,
            )
            injected += 1
            logger.info(
                "[#%d] Activite injectee : id=%s | sport=%s | %d min",
                injected,
                details["activity_id"],
                details["sport_type"],
                int(duration),
            )
            if max_events is None or injected < max_events:
                time.sleep(pause)
    except KeyboardInterrupt:
        logger.info("Flux interrompu proprement apres %d activite(s).", injected)

    return injected


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Injection d'activites sportives temps reel (live_event) dans PostgreSQL."
        )
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Nombre d'activites a generer (lot ou plafond avec --stream).",
    )
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Genere automatiquement des activites en continu (Ctrl+C pour arreter).",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=3.0,
        help="Pause en secondes entre deux activites en mode --stream (defaut: 3.0).",
    )
    parser.add_argument(
        "--reset-live",
        action="store_true",
        help="Purge uniquement les activites live_event pour revenir a l'etat initial.",
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
        help="Discipline sportive (normalisee selon business_rules.yaml).",
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
        help="Distance parcourue en kilometres (sports d'endurance).",
    )
    parser.add_argument(
        "--comment",
        type=str,
        default="Séance collective enregistrée en temps réel",
        help="Commentaire associe a l'activite.",
    )
    return parser.parse_args(argv)


def inject_batch_live_activities(
    conn: pg8000.dbapi.Connection,
    rules: BusinessRules,
    *,
    count: int,
) -> list[dict[str, Any]]:
    """Insere directement un lot d'activites live_event dans PostgreSQL OLTP."""
    if count < 1:
        raise ValueError(f"Le nombre d'activites doit etre >= 1 : {count}")

    employee_ids = list_employee_ids(conn)
    chosen_ids = (
        random.sample(employee_ids, k=count)
        if len(employee_ids) >= count
        else random.choices(employee_ids, k=count)
    )
    inserted_batch: list[dict[str, Any]] = []
    for idx, emp_id in enumerate(chosen_ids, start=1):
        sport = random.choice(DEMO_SPORTS)
        duration = float(random.randint(25, 75))
        comment = random.choice(DEMO_COMMENTS)
        details = insert_unit_live_activity(
            conn,
            rules,
            employee_id=emp_id,
            sport=sport,
            duration_minutes=duration,
            comment=comment,
        )
        inserted_batch.append(details)
        logger.info(
            "[%d/%d] Activite temps reel injectee : id=%s | sport=%s | %d min",
            idx,
            count,
            details["activity_id"],
            details["sport_type"],
            int(duration),
        )
    return inserted_batch


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    raw_argv = sys.argv[1:] if argv is None else argv
    args = parse_args(raw_argv)

    if not args.reset_live and args.count < 1:
        raise ValueError(f"Le parametre --count doit etre >= 1 : {args.count}")

    settings = EnvSettings()
    rules = load_business_rules()
    conn = pg8000.dbapi.connect(**settings.pg8000_connect_kwargs("pipeline"))
    try:
        if args.reset_live:
            deleted = reset_live_activities(conn)
            logger.info(
                "Remise a zero terminee : %d activite(s) live_event supprimee(s).",
                deleted,
            )
            return

        if args.stream:
            has_explicit_count = any(
                tok == "--count" or tok.startswith("--count=") for tok in raw_argv
            )
            max_events = args.count if has_explicit_count else None
            stream_live_activities(
                conn,
                rules,
                interval_seconds=args.interval_seconds,
                max_events=max_events,
            )
            return

        has_custom_override = any(
            val is not None
            for val in (args.employee_id, args.sport, args.distance_km)
        )
        if args.count > 1 and not has_custom_override:
            inject_batch_live_activities(conn, rules, count=args.count)
            return

        for _ in range(args.count):
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
