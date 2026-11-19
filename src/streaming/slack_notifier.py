import logging
import time
from datetime import datetime
from typing import Literal

import httpx
from pydantic import BaseModel, Field

from src.settings import EnvSettings

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

DeliveryMode = Literal["webhook_http", "dry_run", "disabled"]
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


class ActivityEvent(BaseModel):
    activity_id: int
    employee_id: int
    employee_full_name: str = Field(min_length=1)
    sport_type: str = Field(min_length=1)
    activity_start: datetime | None = None
    elapsed_seconds: int = Field(gt=0)
    distance_m: int | None = Field(default=None, ge=0)
    comment: str | None = None
    ingestion_mode: str


class SlackDeliveryResult(BaseModel):
    activity_id: int
    delivered: bool
    mode: DeliveryMode
    channel: str
    status_code: int | None = None
    message: str


def format_duration_fr(elapsed_seconds: int) -> str:
    if elapsed_seconds <= 0:
        raise ValueError(f"Duree d'activite invalide : {elapsed_seconds}")
    hours, rem = divmod(elapsed_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours > 0:
        if seconds > 0:
            return f"{hours} h {minutes:02d} min {seconds:02d} s"
        return f"{hours} h {minutes:02d} min"
    if minutes > 0:
        if seconds > 0:
            return f"{minutes} min {seconds:02d} s"
        return f"{minutes} min"
    return f"{seconds} s"


def format_distance_km_fr(distance_m: int | None) -> str | None:
    if distance_m is None or distance_m <= 0:
        return None
    km_val = distance_m / 1000.0
    if distance_m % 100 == 0:
        formatted = f"{km_val:.1f}".replace(".", ",")
    else:
        formatted = f"{km_val:.2f}".replace(".", ",")
    return f"{formatted} km"


def format_slack_activity_message(event: ActivityEvent) -> str:
    full_name = event.employee_full_name.strip()
    sport = event.sport_type.strip()
    duration_txt = format_duration_fr(event.elapsed_seconds)
    distance_txt = format_distance_km_fr(event.distance_m)

    if distance_txt is not None:
        headline = (
            f"Bravo à *{full_name}* qui vient d'enregistrer une séance de "
            f"*{sport}* de *{distance_txt}* en *{duration_txt}*."
        )
    else:
        headline = (
            f"Bravo à *{full_name}* qui vient d'enregistrer une séance de "
            f"*{sport}* d'une durée de *{duration_txt}*."
        )

    cleaned_comment = (event.comment or "").strip()
    if cleaned_comment:
        return f"{headline}\nCommentaire : « {cleaned_comment} »"
    return headline


class SlackNotifier:
    """Publie les notifications d'activites sportives sur Slack avec bascule dry-run."""

    def __init__(
        self,
        settings: EnvSettings | None = None,
        *,
        http_client: httpx.Client | None = None,
        sleep_fn=time.sleep,
    ) -> None:
        self.settings = settings or EnvSettings()
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=self.settings.http_timeout_seconds)
        self._sleep_fn = sleep_fn

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "SlackNotifier":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def notify_activity(self, event: ActivityEvent) -> SlackDeliveryResult:
        message = format_slack_activity_message(event)
        channel = self.settings.slack_channel_name

        if not self.settings.slack_notifications_enabled:
            logger.info(
                "Notification Slack desactivee (activity_id=%s, canal=%s)",
                event.activity_id,
                channel,
            )
            return SlackDeliveryResult(
                activity_id=event.activity_id,
                delivered=False,
                mode="disabled",
                channel=channel,
                message=message,
            )

        if not self.settings.has_valid_slack_webhook():
            logger.info(
                "Mode degrade Slack [dry_run] sur %s (activity_id=%s) : %s",
                channel,
                event.activity_id,
                message.replace("\n", " | "),
            )
            return SlackDeliveryResult(
                activity_id=event.activity_id,
                delivered=False,
                mode="dry_run",
                channel=channel,
                message=message,
            )

        webhook_url = self.settings.slack_webhook_url.get_secret_value().strip()
        payload = {"channel": channel, "text": message}
        max_attempts = max(1, self.settings.http_max_retries)
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                response = self._client.post(webhook_url, json=payload)
                if response.status_code in RETRYABLE_STATUS_CODES and attempt < max_attempts:
                    delay = self.settings.http_retry_backoff_seconds * (2 ** (attempt - 1))
                    self._sleep_fn(delay)
                    continue
                response.raise_for_status()
                logger.info(
                    "Notification Slack publiee sur %s (activity_id=%s, HTTP %s)",
                    channel,
                    event.activity_id,
                    response.status_code,
                )
                return SlackDeliveryResult(
                    activity_id=event.activity_id,
                    delivered=True,
                    mode="webhook_http",
                    channel=channel,
                    status_code=response.status_code,
                    message=message,
                )
            except httpx.TransportError as exc:
                last_error = exc
                if attempt < max_attempts:
                    delay = self.settings.http_retry_backoff_seconds * (2 ** (attempt - 1))
                    self._sleep_fn(delay)
                    continue
                raise RuntimeError(
                    f"Echec reseau lors de l'envoi Slack pour activity_id={event.activity_id}"
                ) from exc
            except httpx.HTTPStatusError as exc:
                raise RuntimeError(
                    f"Rejet HTTP {exc.response.status_code} du webhook Slack "
                    f"pour activity_id={event.activity_id}"
                ) from exc

        raise RuntimeError(
            f"Echec d'envoi Slack apres {max_attempts} tentatives"
        ) from last_error