import pytest
from pydantic import ValidationError

from src.settings import BusinessRules, EnvSettings, load_business_rules


def test_default_business_rules_integrity() -> None:
    rules = load_business_rules()
    assert rules.company.name == "Sport Data Solution"
    assert rules.company.headquarters_address == "1362 Av. des Platanes, 34970 Lattes, France"
    assert rules.mobility_bonus.annual_gross_salary_rate == pytest.approx(0.05)
    assert rules.mobility_bonus.eligible_modes["Marche/running"].max_distance_km == 15.0
    assert rules.mobility_bonus.eligible_modes["Vélo/Trottinette/Autres"].max_distance_km == 25.0
    assert rules.wellbeing_days.min_activities_per_year == 15
    assert rules.wellbeing_days.granted_days_count == 5
    assert rules.wellbeing_days.rolling_window_months == 12


def test_all_referential_sports_normalized_and_classified() -> None:
    rules = load_business_rules()
    assert rules.normalize_sport("Runing") == "Course à pied"
    assert rules.normalize_sport(" Running ") == "Course à pied"

    expected_distance = ["Runing", "Randonnée", "Triathlon", "Natation", "Voile", "Équitation"]
    for sport in expected_distance:
        assert rules.requires_distance(sport) is True

    expected_null_distance = [
        "Escalade",
        "Tennis",
        "Badminton",
        "Tennis de table",
        "Football",
        "Rugby",
        "Basketball",
        "Judo",
        "Boxe",
    ]
    for sport in expected_null_distance:
        assert rules.requires_distance(sport) is False

    with pytest.raises(ValueError, match="Discipline sportive non referencee"):
        rules.requires_distance("Parachutisme")


def test_commute_declaration_evaluation_and_anomalies() -> None:
    rules = load_business_rules()

    assert rules.evaluate_commute_declaration("véhicule thermique/électrique", None) == (
        False,
        False,
        None,
    )
    assert rules.evaluate_commute_declaration("Transports en commun", 8.5) == (
        False,
        False,
        None,
    )

    assert rules.evaluate_commute_declaration("Marche/running", 15.0) == (True, False, None)
    eligible, anomaly, reason = rules.evaluate_commute_declaration("Marche/running", 50.0)
    assert eligible is False
    assert anomaly is True
    assert reason is not None and "15.0 km" in reason

    assert rules.evaluate_commute_declaration("Vélo/Trottinette/Autres", 25.0) == (
        True,
        False,
        None,
    )
    eligible_bike, anomaly_bike, _ = rules.evaluate_commute_declaration(
        "Vélo/Trottinette/Autres", 25.4
    )
    assert eligible_bike is False
    assert anomaly_bike is True


def test_parametric_overrides_and_validation_guardrails() -> None:
    base_rules = load_business_rules()
    simulated = base_rules.with_overrides(
        prime_rate=0.08,
        min_activities_per_year=20,
        wellbeing_days_granted=6,
    )
    assert simulated.mobility_bonus.annual_gross_salary_rate == pytest.approx(0.08)
    assert simulated.wellbeing_days.min_activities_per_year == 20
    assert simulated.wellbeing_days.granted_days_count == 6
    assert base_rules.mobility_bonus.annual_gross_salary_rate == pytest.approx(0.05)

    with pytest.raises(ValidationError):
        base_rules.with_overrides(prime_rate=0.75)

    invalid_payload = base_rules.model_dump()
    invalid_payload["sports_catalog"]["non_distance_sports"].append("Randonnée")
    with pytest.raises(ValidationError):
        BusinessRules.model_validate(invalid_payload)


def test_env_settings_pg8000_and_delta_storage_contracts() -> None:
    env = EnvSettings(_env_file=None)
    pg_pipeline = env.pg8000_connect_kwargs("pipeline")
    assert pg_pipeline["user"] == "sds_pipeline"
    assert pg_pipeline["database"] == "sds_platform"
    assert pg_pipeline["port"] == 5432

    pg_bi = env.pg8000_connect_kwargs("bi_reader")
    assert pg_bi["user"] == "sds_bi_reader"
    assert pg_bi["user"] != pg_pipeline["user"]

    storage = env.delta_storage_options()
    assert storage["AWS_ENDPOINT_URL"] == "http://localhost:9000"
    assert storage["AWS_REGION"] == "eu-west-3"
    assert storage["AWS_ALLOW_HTTP"] == "true"