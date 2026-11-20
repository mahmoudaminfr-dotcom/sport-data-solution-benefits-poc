import argparse
import json
import logging
from datetime import date

from src.processing.lakehouse_bronze_silver import run_bronze_silver_pipeline
from src.processing.lakehouse_gold import run_gold_pipeline
from src.settings import BusinessRules, EnvSettings, load_business_rules

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Recalcul dynamique des indicateurs Gold (Delta Lake + PostgreSQL gold_mart) "
            "pour Sport Data Solution."
        )
    )
    parser.add_argument(
        "--prime-rate",
        type=float,
        default=None,
        help="Taux annuel de prime mobilite sur salaire brut (ex: 0.04 pour 4 %%).",
    )
    parser.add_argument(
        "--max-walk-run-km",
        type=float,
        default=None,
        help="Plafond kilometrique domicile-travail pour Marche/running (defaut YAML: 15.0).",
    )
    parser.add_argument(
        "--max-bike-km",
        type=float,
        default=None,
        help="Plafond kilometrique domicile-travail pour Velo/Trottinette (defaut YAML: 25.0).",
    )
    parser.add_argument(
        "--min-activities",
        type=int,
        default=None,
        help="Seuil minimal d'activites sur 12 mois pour les jours bien-etre (defaut YAML: 15).",
    )
    parser.add_argument(
        "--wellbeing-days",
        type=int,
        default=None,
        help="Nombre de jours bien-etre accordes par salarie eligible (defaut YAML: 5).",
    )
    parser.add_argument(
        "--as-of-date",
        type=str,
        default=None,
        help="Date de reference ISO (YYYY-MM-DD) pour la fenetre glissante de 12 mois.",
    )
    parser.add_argument(
        "--refresh-bronze-silver",
        action="store_true",
        help="Rafraichit egalement les couches Bronze et Silver depuis PostgreSQL avant Gold.",
    )
    parser.add_argument(
        "--reset-defaults",
        action="store_true",
        help="Ignore toute surcharge et restaure strictement config/business_rules.yaml.",
    )
    return parser.parse_args(argv)


def build_effective_rules(
    args: argparse.Namespace,
    base_rules: BusinessRules | None = None,
) -> BusinessRules:
    rules = base_rules or load_business_rules()
    if args.reset_defaults:
        return rules
    return rules.with_overrides(
        prime_rate=args.prime_rate,
        max_walk_run_km=args.max_walk_run_km,
        max_bike_micro_km=args.max_bike_km,
        min_activities_per_year=args.min_activities,
        wellbeing_days_granted=args.wellbeing_days,
    )


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    args = parse_args(argv)
    cfg = EnvSettings()
    effective_rules = build_effective_rules(args)
    ref_date = date.fromisoformat(args.as_of_date) if args.as_of_date else None

    if args.refresh_bronze_silver:
        bs_summary = run_bronze_silver_pipeline(
            cfg, effective_rules, as_of_date=ref_date
        )
        logger.info(
            "Couches Bronze/Silver actualisees : %s",
            json.dumps(bs_summary["silver"], ensure_ascii=False),
        )

    gold_summary = run_gold_pipeline(
        cfg,
        effective_rules,
        as_of_date=ref_date,
    )
    logger.info(
        "Recalcul Gold termine (moteur=%s ms, total=%s ms) : %s",
        gold_summary["compute_engine_ms"],
        gold_summary["total_pipeline_ms"],
        json.dumps(gold_summary["kpis"], ensure_ascii=False),
    )


if __name__ == "__main__":
    main()