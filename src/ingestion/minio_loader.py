import hashlib
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from src.settings import EnvSettings

logger = logging.getLogger(__name__)

XLSX_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
NOT_FOUND_CODES = frozenset({"404", "NoSuchKey", "NotFound"})
BUCKET_EXISTS_CODES = frozenset({"BucketAlreadyOwnedByYou", "BucketAlreadyExists"})


class S3ClientProtocol(Protocol):
    def list_buckets(self) -> dict[str, Any]: ...

    def create_bucket(self, *, Bucket: str) -> Any: ...

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]: ...

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
        Metadata: dict[str, str],
    ) -> Any: ...


@dataclass(frozen=True)
class ObjectSyncResult:
    bucket: str
    s3_key: str
    source_path: Path
    size_bytes: int
    sha256: str
    uploaded: bool


@dataclass(frozen=True)
class MinioSyncSummary:
    created_buckets: tuple[str, ...]
    existing_buckets: tuple[str, ...]
    objects: tuple[ObjectSyncResult, ...]


def build_s3_client(settings: EnvSettings) -> S3ClientProtocol:
    s3_config = Config(
        region_name=settings.minio_region,
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    )
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_root_user,
        aws_secret_access_key=settings.minio_root_password.get_secret_value(),
        region_name=settings.minio_region,
        config=s3_config,
    )


def compute_file_sha256(file_path: Path, chunk_size: int = 65536) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as fh:
        while chunk := fh.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_lakehouse_buckets(
    s3_client: S3ClientProtocol,
    buckets: Iterable[str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    response = s3_client.list_buckets()
    known = {item["Name"] for item in response.get("Buckets", [])}
    created: list[str] = []
    existing: list[str] = []

    for bucket in buckets:
        if bucket in known:
            existing.append(bucket)
            continue
        try:
            s3_client.create_bucket(Bucket=bucket)
            created.append(bucket)
            known.add(bucket)
            logger.info("Bucket S3 cree : %s", bucket)
        except ClientError as exc:
            error_code = str(exc.response.get("Error", {}).get("Code", ""))
            if error_code in BUCKET_EXISTS_CODES:
                existing.append(bucket)
                known.add(bucket)
            else:
                raise

    return tuple(created), tuple(existing)


def _remote_object_matches(
    s3_client: S3ClientProtocol,
    *,
    bucket: str,
    s3_key: str,
    expected_sha256: str,
    expected_size: int,
) -> bool:
    try:
        head = s3_client.head_object(Bucket=bucket, Key=s3_key)
    except ClientError as exc:
        error_code = str(exc.response.get("Error", {}).get("Code", ""))
        http_status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if error_code in NOT_FOUND_CODES or http_status == 404:
            return False
        raise

    metadata = {k.lower(): v for k, v in head.get("Metadata", {}).items()}
    remote_sha256 = metadata.get("sha256")
    remote_size = head.get("ContentLength")
    return remote_sha256 == expected_sha256 and remote_size == expected_size


def sync_referential_file(
    s3_client: S3ClientProtocol,
    *,
    bucket: str,
    s3_key: str,
    source_path: Path,
) -> ObjectSyncResult:
    if not source_path.is_file():
        raise FileNotFoundError(f"Fichier referentiel introuvable : {source_path}")

    payload = source_path.read_bytes()
    size_bytes = len(payload)
    file_sha256 = hashlib.sha256(payload).hexdigest()

    if _remote_object_matches(
        s3_client,
        bucket=bucket,
        s3_key=s3_key,
        expected_sha256=file_sha256,
        expected_size=size_bytes,
    ):
        logger.info("Objet S3 deja a jour (SHA-256 identique) : s3://%s/%s", bucket, s3_key)
        return ObjectSyncResult(
            bucket=bucket,
            s3_key=s3_key,
            source_path=source_path,
            size_bytes=size_bytes,
            sha256=file_sha256,
            uploaded=False,
        )

    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=payload,
        ContentType=XLSX_CONTENT_TYPE,
        Metadata={"sha256": file_sha256},
    )
    logger.info(
        "Objet S3 televerse : s3://%s/%s (%d octets, sha256=%s)",
        bucket,
        s3_key,
        size_bytes,
        file_sha256[:12],
    )
    return ObjectSyncResult(
        bucket=bucket,
        s3_key=s3_key,
        source_path=source_path,
        size_bytes=size_bytes,
        sha256=file_sha256,
        uploaded=True,
    )


def sync_raw_referentials(
    settings: EnvSettings | None = None,
    s3_client: S3ClientProtocol | None = None,
) -> MinioSyncSummary:
    active_settings = settings or EnvSettings()
    client = s3_client or build_s3_client(active_settings)

    created_buckets, existing_buckets = ensure_lakehouse_buckets(
        client,
        active_settings.lakehouse_buckets(),
    )

    results: list[ObjectSyncResult] = []
    for s3_key, local_path in active_settings.referential_sources().items():
        result = sync_referential_file(
            client,
            bucket=active_settings.minio_bucket_raw,
            s3_key=s3_key,
            source_path=local_path,
        )
        results.append(result)

    return MinioSyncSummary(
        created_buckets=created_buckets,
        existing_buckets=existing_buckets,
        objects=tuple(results),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    summary = sync_raw_referentials()
    logger.info(
        "Synchronisation S3 terminee : buckets_crees=%s, buckets_existants=%s",
        list(summary.created_buckets),
        list(summary.existing_buckets),
    )
    for obj in summary.objects:
        action = "UPLOADED" if obj.uploaded else "SKIPPED_UNCHANGED"
        logger.info(
            "[%s] s3://%s/%s | %d octets | sha256=%s",
            action,
            obj.bucket,
            obj.s3_key,
            obj.size_bytes,
            obj.sha256,
        )


if __name__ == "__main__":
    main()
