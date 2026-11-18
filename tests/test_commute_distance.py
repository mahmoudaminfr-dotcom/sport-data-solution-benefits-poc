from pathlib import Path

import httpx
import pytest

from src.ingestion.commute_distance import CommuteDistanceResolver
from src.settings import EnvSettings, load_business_rules


def test_non_eligible_commute_makes_zero_http_calls(tmp_path: Path) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    settings = EnvSettings(_env_file=None, cache_data_dir=tmp_path)
    client = httpx.Client(transport=httpx.MockTransport(handler))
    with CommuteDistanceResolver(settings=settings, http_client=client) as resolver:
        audit = resolver.audit_employee_commute(
            home_address="10 Rue de la Loge, 34000 Montpellier",
            declared_mode="Transports en commun",
        )

    assert calls == 0
    assert audit.computed_distance_km is None
    assert audit.max_allowed_distance_km is None
    assert audit.is_mobility_bonus_eligible is False
    assert audit.is_commute_anomaly is False


def test_google_maps_primary_and_pii_safe_cache(tmp_path: Path) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "rows": [
                    {
                        "elements": [
                            {
                                "status": "OK",
                                "distance": {"value": 8450, "text": "8.5 km"},
                            }
                        ]
                    }
                ],
            },
        )

    settings = EnvSettings(
        _env_file=None,
        cache_data_dir=tmp_path,
        google_maps_api_key="AIzaSyTestValidLookingKey000001",
    )
    client = httpx.Client(transport=httpx.MockTransport(handler))
    secret_address = "15 Place de la Comedie, 34000 Montpellier"

    with CommuteDistanceResolver(settings=settings, http_client=client) as resolver:
        first = resolver.audit_employee_commute(secret_address, "Marche/running")
        second = resolver.audit_employee_commute(secret_address, "Marche/running")

    assert calls == 1
    assert first.from_cache is False
    assert first.provider == "google_maps"
    assert first.computed_distance_km == pytest.approx(8.45)
    assert first.is_mobility_bonus_eligible is True
    assert first.is_commute_anomaly is False
    assert second.from_cache is True

    raw_cache_text = settings.commute_cache_path().read_text(encoding="utf-8")
    assert "Comedie" not in raw_cache_text
    assert "Montpellier" not in raw_cache_text


def test_fallback_ban_osrm_retry_504_and_postcode_locking(tmp_path: Path) -> None:
    ban_attempts = 0
    seen_postcodes: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal ban_attempts
        url = str(request.url)
        if "api-adresse.data.gouv.fr" in url:
            ban_attempts += 1
            if ban_attempts == 1:
                return httpx.Response(504, text="Gateway Timeout")
            query = request.url.params.get("q", "")
            seen_postcodes.append(request.url.params.get("postcode"))
            if "1362 Avenue des Platanes" in query:
                return httpx.Response(
                    200,
                    json={
                        "features": [
                            {
                                "properties": {"score": 0.92},
                                "geometry": {"coordinates": [3.9231, 43.5857]},
                            }
                        ]
                    },
                )
            if "SansCodePostal" in query:
                return httpx.Response(
                    200,
                    json={
                        "features": [
                            {
                                "properties": {"score": 0.52},
                                "geometry": {"coordinates": [3.87, 43.61]},
                            }
                        ]
                    },
                )
            return httpx.Response(
                200,
                json={
                    "features": [
                        {
                            "properties": {"score": 0.49},
                            "geometry": {"coordinates": [4.3601, 43.8367]},
                        }
                    ]
                },
            )
        return httpx.Response(
            200,
            json={"code": "Ok", "routes": [{"distance": 51200.0}]},
        )

    settings = EnvSettings(
        _env_file=None,
        cache_data_dir=tmp_path,
        google_maps_api_key="your_google_maps_api_key_here",
        http_retry_backoff_seconds=0.01,
    )
    rules = load_business_rules()
    client = httpx.Client(transport=httpx.MockTransport(handler))

    with CommuteDistanceResolver(settings=settings, rules=rules, http_client=client) as resolver:
        far_bike = resolver.audit_employee_commute(
            "4 Bd Victor Hugo, 30000 Nimes",
            "Vélo/Trottinette/Autres",
        )
        invalid_addr = resolver.audit_employee_commute(
            "Av. Incomplete SansCodePostal",
            "Vélo/Trottinette/Autres",
        )

    assert "30000" in seen_postcodes
    assert "34970" in seen_postcodes
    assert far_bike.provider == "ban_osrm"
    assert far_bike.computed_distance_km == pytest.approx(51.2)
    assert far_bike.is_mobility_bonus_eligible is False
    assert far_bike.is_commute_anomaly is True

    assert invalid_addr.computed_distance_km is None
    assert invalid_addr.is_mobility_bonus_eligible is False
    assert invalid_addr.is_commute_anomaly is True
    assert invalid_addr.anomaly_reason is not None
