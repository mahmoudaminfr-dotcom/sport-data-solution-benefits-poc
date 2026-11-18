import unicodedata
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RULES_PATH = PROJECT_ROOT / "config" / "business_rules.yaml"
PLACEHOLDER_MARKERS = ("change_me", "your_", "placeholder", "xxxx")

DbRole = Literal["pipeline", "bi_reader", "admin"]


class CommuteRule(BaseModel):
    google_maps_mode: Literal["walking", "bicycling", "driving", "transit"]
    max_distance_km: float = Field(gt=0.0)


class CompanyConfig(BaseModel):
    name: str
    headquarters_address: str
    currency: str = "EUR"


class MobilityBonusConfig(BaseModel):
    annual_gross_salary_rate: float = Field(ge=0.0, le=0.5)
    eligible_modes: dict[str, CommuteRule]
    non_eligible_modes: list[str]

    @model_validator(mode="after")
    def verify_disjoint_commute_modes(self) -> "MobilityBonusConfig":
        overlap = set(self.eligible_modes.keys()).intersection(set(self.non_eligible_modes))
        if overlap:
            modes = sorted(overlap)
            raise ValueError(f"Conflit de declaration sur les modes de transport : {modes}")
        return self


class WellbeingDaysConfig(BaseModel):
    min_activities_per_year: int = Field(ge=1)
    granted_days_count: int = Field(ge=0)
    rolling_window_months: int = Field(default=12, ge=1)


class SportsCatalogConfig(BaseModel):
    normalization_map: dict[str, str]
    distance_tracked_sports: list[str]
    non_distance_sports: list[str]

    @model_validator(mode="after")
    def verify_disjoint_sport_categories(self) -> "SportsCatalogConfig":
        overlap = set(self.distance_tracked_sports).intersection(set(self.non_distance_sports))
        if overlap:
            raise ValueError(f"Conflit de categorisation pour les disciplines : {sorted(overlap)}")
        return self


class BusinessRules(BaseModel):
    company: CompanyConfig
    mobility_bonus: MobilityBonusConfig
    wellbeing_days: WellbeingDaysConfig
    sports_catalog: SportsCatalogConfig

    def normalize_sport(self, raw_sport: str) -> str:
        cleaned = raw_sport.strip()
        return self.sports_catalog.normalization_map.get(cleaned, cleaned)

    def requires_distance(self, sport_name: str) -> bool:
        normalized = self.normalize_sport(sport_name)
        if normalized in self.sports_catalog.distance_tracked_sports:
            return True
        if normalized in self.sports_catalog.non_distance_sports:
            return False
        raise ValueError(f"Discipline sportive non referencee au catalogue : {sport_name!r}")

    def evaluate_commute_declaration(
        self,
        declared_mode: str,
        computed_distance_km: float | None,
    ) -> tuple[bool, bool, str | None]:
        mode = declared_mode.strip()
        if mode in self.mobility_bonus.non_eligible_modes:
            return (False, False, None)

        rule = self.mobility_bonus.eligible_modes.get(mode)
        if rule is None:
            raise ValueError(f"Mode de deplacement inconnu du referentiel RH : {declared_mode!r}")

        if computed_distance_km is None or computed_distance_km < 0.0:
            return (False, True, "Distance domicile-travail indisponible ou invalide")

        if computed_distance_km <= rule.max_distance_km:
            return (True, False, None)

        reason = (
            f"Distance calculee ({computed_distance_km:.2f} km) superieure au plafond "
            f"autorise ({rule.max_distance_km:.1f} km) pour le mode {mode}"
        )
        return (False, True, reason)

    def with_overrides(
        self,
        *,
        prime_rate: float | None = None,
        max_walk_run_km: float | None = None,
        max_bike_micro_km: float | None = None,
        min_activities_per_year: int | None = None,
        wellbeing_days_granted: int | None = None,
    ) -> "BusinessRules":
        payload = self.model_dump()
        eligible = payload["mobility_bonus"]["eligible_modes"]
        if prime_rate is not None:
            payload["mobility_bonus"]["annual_gross_salary_rate"] = prime_rate
        if max_walk_run_km is not None:
            eligible["Marche/running"]["max_distance_km"] = max_walk_run_km
        if max_bike_micro_km is not None:
            eligible["Vélo/Trottinette/Autres"]["max_distance_km"] = max_bike_micro_km
        if min_activities_per_year is not None:
            payload["wellbeing_days"]["min_activities_per_year"] = min_activities_per_year
        if wellbeing_days_granted is not None:
            payload["wellbeing_days"]["granted_days_count"] = wellbeing_days_granted
        return BusinessRules.model_validate(payload)


class EnvSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "sds_platform"
    postgres_admin_user: str = "sds_admin"
    postgres_admin_password: SecretStr = SecretStr("change_me_admin_secret")

    sds_pipeline_user: str = "sds_pipeline"
    sds_pipeline_password: SecretStr = SecretStr("change_me_pipeline_secret")
    sds_debezium_user: str = "sds_debezium"
    sds_debezium_password: SecretStr = SecretStr("change_me_debezium_secret")
    sds_bi_reader_user: str = "sds_bi_reader"
    sds_bi_reader_password: SecretStr = SecretStr("change_me_bi_reader_secret")

    minio_endpoint: str = "http://localhost:9000"
    minio_console_port: int = 9001
    minio_root_user: str = "sds_minio_admin"
    minio_root_password: SecretStr = SecretStr("change_me_minio_secret")
    minio_region: str = "eu-west-3"
    minio_bucket_raw: str = "sds-raw-referentials"
    minio_bucket_bronze: str = "sds-bronze"
    minio_bucket_silver: str = "sds-silver"
    minio_bucket_gold: str = "sds-gold"

    raw_data_dir: Path = PROJECT_ROOT / "data" / "raw"
    cache_data_dir: Path = PROJECT_ROOT / "data" / "cache"
    hr_source_filename: str = "Données+RH.xlsx"
    sports_source_filename: str = "Données+Sportive.xlsx"
    hr_referential_s3_key: str = "referentials/donnees_rh.xlsx"
    sports_referential_s3_key: str = "referentials/donnees_sportives.xlsx"

    redpanda_brokers: str = "localhost:19092"
    redpanda_console_port: int = 8080
    debezium_connect_url: str = "http://localhost:8083"
    redpanda_activity_topic: str = "sds.oltp_sports.sport_activities"

    google_maps_api_key: SecretStr = SecretStr("")
    google_maps_distance_matrix_url: str = (
        "https://maps.googleapis.com/maps/api/distancematrix/json"
    )
    commute_enable_open_fallback: bool = True
    commute_cache_filename: str = "commute_distances_cache.json"
    ban_geocoding_url: str = "https://api-adresse.data.gouv.fr/search/"
    ban_min_confidence_score: float = 0.35
    ban_no_postcode_min_score: float = 0.80
    osrm_foot_route_url: str = "https://routing.openstreetmap.de/routed-foot/route/v1/driving"
    osrm_bike_route_url: str = "https://routing.openstreetmap.de/routed-bike/route/v1/driving"
    http_timeout_seconds: float = 10.0
    http_max_retries: int = 3
    http_retry_backoff_seconds: float = 0.5
    historical_random_seed: int = 42

    slack_webhook_url: SecretStr = SecretStr("")
    slack_channel_name: str = "#defis-sportifs-sds"
    slack_notifications_enabled: bool = True
    sds_pii_hash_salt: SecretStr = SecretStr("change_me_random_32_chars_salt_value")

    def lakehouse_buckets(self) -> tuple[str, str, str, str]:
        return (
            self.minio_bucket_raw,
            self.minio_bucket_bronze,
            self.minio_bucket_silver,
            self.minio_bucket_gold,
        )

    def _resolve_in_raw_dir(self, filename: str) -> Path:
        direct_path = self.raw_data_dir / filename
        if direct_path.exists() or not self.raw_data_dir.is_dir():
            return direct_path
        expected_nfc = unicodedata.normalize("NFC", filename)
        for candidate in sorted(self.raw_data_dir.iterdir()):
            if unicodedata.normalize("NFC", candidate.name) == expected_nfc:
                return candidate
        return direct_path

    def referential_sources(self) -> dict[str, Path]:
        return {
            self.hr_referential_s3_key: self._resolve_in_raw_dir(self.hr_source_filename),
            self.sports_referential_s3_key: self._resolve_in_raw_dir(self.sports_source_filename),
        }

    def commute_cache_path(self) -> Path:
        return self.cache_data_dir / self.commute_cache_filename

    def has_valid_google_maps_key(self) -> bool:
        raw = self.google_maps_api_key.get_secret_value().strip()
        if not raw:
            return False
        lowered = raw.lower()
        return not any(marker in lowered for marker in PLACEHOLDER_MARKERS)

    def pg8000_connect_kwargs(self, role: DbRole = "pipeline") -> dict[str, object]:
        credentials: dict[DbRole, tuple[str, SecretStr]] = {
            "pipeline": (self.sds_pipeline_user, self.sds_pipeline_password),
            "bi_reader": (self.sds_bi_reader_user, self.sds_bi_reader_password),
            "admin": (self.postgres_admin_user, self.postgres_admin_password),
        }
        if role not in credentials:
            raise ValueError(f"Role PostgreSQL non autorise : {role!r}")
        user, secret = credentials[role]
        return {
            "user": user,
            "password": secret.get_secret_value(),
            "host": self.postgres_host,
            "port": self.postgres_port,
            "database": self.postgres_db,
        }

    def delta_storage_options(self) -> dict[str, str]:
        return {
            "AWS_ENDPOINT_URL": self.minio_endpoint,
            "AWS_ACCESS_KEY_ID": self.minio_root_user,
            "AWS_SECRET_ACCESS_KEY": self.minio_root_password.get_secret_value(),
            "AWS_REGION": self.minio_region,
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }


def load_business_rules(path: Path = DEFAULT_RULES_PATH) -> BusinessRules:
    with path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh)
    return BusinessRules.model_validate(payload)
