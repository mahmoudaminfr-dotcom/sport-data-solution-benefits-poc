import hashlib
import json
import logging
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from src.settings import BusinessRules, EnvSettings, load_business_rules

logger = logging.getLogger(__name__)

POSTAL_CODE_RE = re.compile(r"\b\d{5}\b")
RETRYABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})
UNROUTABLE_GMAPS_STATUSES = frozenset(
    {"NOT_FOUND", "ZERO_RESULTS", "MAX_ROUTE_LENGTH_EXCEEDED"}
)
ABBREVIATION_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bAv\.\s*", flags=re.IGNORECASE), "Avenue "),
    (re.compile(r"\bBd\.?\s+", flags=re.IGNORECASE), "Boulevard "),
    (re.compile(r"\bPl\.\s*", flags=re.IGNORECASE), "Place "),
    (re.compile(r"\bImp\.\s*", flags=re.IGNORECASE), "Impasse "),
    (re.compile(r"\bChem\.\s*", flags=re.IGNORECASE), "Chemin "),
    (re.compile(r"\bRte\b\.?\s*", flags=re.IGNORECASE), "Route "),
    (re.compile(r",?\s*\bFrance\b\s*$", flags=re.IGNORECASE), ""),
)


def _normalize_text(value: str) -> str:
    collapsed = " ".join(value.strip().split())
    return unicodedata.normalize("NFC", collapsed)


def _expand_french_address(address: str) -> str:
    expanded = _normalize_text(address)
    for pattern, replacement in ABBREVIATION_PATTERNS:
        expanded = pattern.sub(replacement, expanded)
    return " ".join(expanded.split())


def build_route_cache_key(
    origin_address: str,
    destination_address: str,
    google_maps_mode: str,
    salt: str,
) -> str:
    orig = _normalize_text(origin_address).lower()
    dest = _normalize_text(destination_address).lower()
    mode = google_maps_mode.strip().lower()
    payload = f"{salt}|{orig}|{dest}|{mode}".encode()
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class RouteDistanceResult:
    distance_km: float | None
    status: str
    provider: str
    from_cache: bool


@dataclass(frozen=True)
class CommuteAuditEvaluation:
    declared_mode: str
    computed_distance_km: float | None
    max_allowed_distance_km: float | None
    is_mobility_bonus_eligible: bool
    is_commute_anomaly: bool
    anomaly_reason: str | None
    provider: str
    from_cache: bool


class CommuteDistanceResolver:
    def __init__(
        self,
        settings: EnvSettings | None = None,
        rules: BusinessRules | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings or EnvSettings()
        self.rules = rules or load_business_rules()
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=self.settings.http_timeout_seconds)
        self._cache_path: Path = self.settings.commute_cache_path()
        self._cache: dict[str, dict[str, Any]] = self._load_cache()
        self._cache_dirty = False
        self._geocode_memo: dict[str, tuple[float, float] | None] = {}
        self._warned_fallback = False

    def close(self) -> None:
        self.save_cache()
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "CommuteDistanceResolver":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def _load_cache(self) -> dict[str, dict[str, Any]]:
        if not self._cache_path.is_file():
            return {}
        raw = json.loads(self._cache_path.read_text(encoding="utf-8"))
        entries = raw.get("entries", {})
        return dict(entries) if isinstance(entries, dict) else {}

    def save_cache(self) -> None:
        if not self._cache_dirty:
            return
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "entries": dict(sorted(self._cache.items())),
        }
        serialized = json.dumps(payload, indent=2, ensure_ascii=True) + "\n"
        self._cache_path.write_text(serialized, encoding="utf-8")
        self._cache_dirty = False

    def _get_with_retry(self, url: str, params: dict[str, Any]) -> httpx.Response:
        attempts = max(1, self.settings.http_max_retries)
        for attempt in range(attempts):
            try:
                response = self._client.get(url, params=params)
            except httpx.TransportError:
                if attempt == attempts - 1:
                    raise
                time.sleep(self.settings.http_retry_backoff_seconds * (2**attempt))
                continue

            if response.status_code in RETRYABLE_HTTP_STATUSES and attempt < attempts - 1:
                time.sleep(self.settings.http_retry_backoff_seconds * (2**attempt))
                continue

            response.raise_for_status()
            return response

        raise RuntimeError("Echec inattendu de la boucle de requetes HTTP.")

    def resolve_distance(
        self,
        origin_address: str,
        google_maps_mode: str,
        destination_address: str | None = None,
    ) -> RouteDistanceResult:
        destination = destination_address or self.rules.company.headquarters_address
        cache_key = build_route_cache_key(
            origin_address=origin_address,
            destination_address=destination,
            google_maps_mode=google_maps_mode,
            salt=self.settings.sds_pii_hash_salt.get_secret_value(),
        )

        use_gmaps = self.settings.has_valid_google_maps_key()
        cached = self._cache.get(cache_key)
        if cached is not None:
            cached_provider = str(cached.get("provider", ""))
            if not (use_gmaps and cached_provider != "google_maps"):
                dist_val = cached.get("distance_km")
                return RouteDistanceResult(
                    distance_km=float(dist_val) if dist_val is not None else None,
                    status=str(cached.get("status", "OK")),
                    provider=cached_provider,
                    from_cache=True,
                )

        result: RouteDistanceResult | None = None
        if use_gmaps:
            result = self._query_google_maps(origin_address, destination, google_maps_mode)

        if result is None:
            if not self.settings.commute_enable_open_fallback:
                raise RuntimeError(
                    "Calcul Google Maps indisponible et repli routier desactive."
                )
            if not self._warned_fallback:
                logger.warning(
                    "Cle Google Maps absente ou refusee : bascule sur le routage BAN + OSRM."
                )
                self._warned_fallback = True
            result = self._query_ban_osrm(origin_address, destination, google_maps_mode)

        self._cache[cache_key] = {
            "mode": google_maps_mode,
            "distance_km": result.distance_km,
            "status": result.status,
            "provider": result.provider,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        self._cache_dirty = True
        return result

    def _query_google_maps(
        self,
        origin_address: str,
        destination_address: str,
        google_maps_mode: str,
    ) -> RouteDistanceResult | None:
        response = self._get_with_retry(
            self.settings.google_maps_distance_matrix_url,
            params={
                "origins": _normalize_text(origin_address),
                "destinations": _normalize_text(destination_address),
                "mode": google_maps_mode,
                "units": "metric",
                "region": "fr",
                "key": self.settings.google_maps_api_key.get_secret_value().strip(),
            },
        )
        body = response.json()
        top_status = str(body.get("status", ""))

        if top_status == "REQUEST_DENIED":
            return None
        if top_status != "OK":
            raise RuntimeError(f"Erreur Google Maps Distance Matrix : status={top_status!r}")

        rows = body.get("rows") or []
        elements = (rows[0].get("elements") if rows else None) or []
        if not elements:
            return RouteDistanceResult(
                distance_km=None,
                status="ZERO_RESULTS",
                provider="google_maps",
                from_cache=False,
            )

        elem = elements[0]
        elem_status = str(elem.get("status", ""))
        if elem_status == "OK":
            meters = float(elem["distance"]["value"])
            return RouteDistanceResult(
                distance_km=round(meters / 1000.0, 3),
                status="OK",
                provider="google_maps",
                from_cache=False,
            )
        if elem_status in UNROUTABLE_GMAPS_STATUSES:
            return RouteDistanceResult(
                distance_km=None,
                status=elem_status,
                provider="google_maps",
                from_cache=False,
            )
        raise RuntimeError(f"Statut element Google Maps inattendu : {elem_status!r}")

    def _geocode_ban(self, address: str) -> tuple[float, float] | None:
        normalized = _normalize_text(address)
        if normalized in self._geocode_memo:
            return self._geocode_memo[normalized]

        expanded = _expand_french_address(normalized)
        pc_match = POSTAL_CODE_RE.search(expanded)
        postcode = pc_match.group(0) if pc_match else None

        params: dict[str, Any] = {"q": expanded, "limit": 1}
        if postcode is not None:
            params["postcode"] = postcode

        response = self._get_with_retry(self.settings.ban_geocoding_url, params=params)
        features = response.json().get("features") or []

        if not features and postcode is not None:
            fallback_params: dict[str, Any] = {
                "q": expanded,
                "postcode": postcode,
                "type": "municipality",
                "limit": 1,
            }
            response = self._get_with_retry(
                self.settings.ban_geocoding_url,
                params=fallback_params,
            )
            features = response.json().get("features") or []

        if not features:
            self._geocode_memo[normalized] = None
            return None

        feature = features[0]
        props = feature.get("properties") or {}
        score = float(props.get("score") or 0.0)
        min_score = (
            self.settings.ban_min_confidence_score
            if postcode is not None
            else self.settings.ban_no_postcode_min_score
        )
        if score < min_score:
            self._geocode_memo[normalized] = None
            return None

        coords = (feature.get("geometry") or {}).get("coordinates") or []
        if len(coords) < 2:
            self._geocode_memo[normalized] = None
            return None

        lon_lat = (float(coords[0]), float(coords[1]))
        self._geocode_memo[normalized] = lon_lat
        return lon_lat

    def _query_ban_osrm(
        self,
        origin_address: str,
        destination_address: str,
        google_maps_mode: str,
    ) -> RouteDistanceResult:
        orig_coords = self._geocode_ban(origin_address)
        dest_coords = self._geocode_ban(destination_address)
        if orig_coords is None or dest_coords is None:
            return RouteDistanceResult(
                distance_km=None,
                status="NOT_FOUND",
                provider="ban_osrm",
                from_cache=False,
            )

        base_url = (
            self.settings.osrm_foot_route_url
            if google_maps_mode == "walking"
            else self.settings.osrm_bike_route_url
        ).rstrip("/")
        orig_pair = f"{orig_coords[0]:.6f},{orig_coords[1]:.6f}"
        dest_pair = f"{dest_coords[0]:.6f},{dest_coords[1]:.6f}"
        response = self._get_with_retry(
            f"{base_url}/{orig_pair};{dest_pair}",
            params={"overview": "false"},
        )
        body = response.json()
        routes = body.get("routes") or []
        if body.get("code") != "Ok" or not routes:
            return RouteDistanceResult(
                distance_km=None,
                status="ZERO_RESULTS",
                provider="ban_osrm",
                from_cache=False,
            )

        meters = float(routes[0]["distance"])
        return RouteDistanceResult(
            distance_km=round(meters / 1000.0, 3),
            status="OK",
            provider="ban_osrm",
            from_cache=False,
        )

    def audit_employee_commute(
        self,
        home_address: str,
        declared_mode: str,
    ) -> CommuteAuditEvaluation:
        cleaned_mode = _normalize_text(declared_mode)
        if cleaned_mode in self.rules.mobility_bonus.non_eligible_modes:
            return CommuteAuditEvaluation(
                declared_mode=cleaned_mode,
                computed_distance_km=None,
                max_allowed_distance_km=None,
                is_mobility_bonus_eligible=False,
                is_commute_anomaly=False,
                anomaly_reason=None,
                provider="not_applicable",
                from_cache=False,
            )

        rule = self.rules.mobility_bonus.eligible_modes.get(cleaned_mode)
        if rule is None:
            raise ValueError(f"Mode de deplacement inconnu du referentiel RH : {declared_mode!r}")

        route = self.resolve_distance(
            origin_address=home_address,
            google_maps_mode=rule.google_maps_mode,
        )
        is_eligible, is_anomaly, reason = self.rules.evaluate_commute_declaration(
            cleaned_mode,
            route.distance_km,
        )
        return CommuteAuditEvaluation(
            declared_mode=cleaned_mode,
            computed_distance_km=route.distance_km,
            max_allowed_distance_km=rule.max_distance_km,
            is_mobility_bonus_eligible=is_eligible,
            is_commute_anomaly=is_anomaly,
            anomaly_reason=reason,
            provider=route.provider,
            from_cache=route.from_cache,
        )
