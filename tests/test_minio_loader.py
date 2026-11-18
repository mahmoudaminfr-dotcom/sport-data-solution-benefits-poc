from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from src.ingestion.minio_loader import (
    compute_file_sha256,
    ensure_lakehouse_buckets,
    sync_raw_referentials,
)
from src.settings import EnvSettings


class FakeS3Client:
    def __init__(self) -> None:
        self.buckets: set[str] = set()
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
        self.put_calls: int = 0

    def list_buckets(self) -> dict[str, Any]:
        return {"Buckets": [{"Name": name} for name in sorted(self.buckets)]}

    def create_bucket(self, *, Bucket: str) -> dict[str, Any]:
        self.buckets.add(Bucket)
        return {"Location": f"/{Bucket}"}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        record = self.objects.get((Bucket, Key))
        if record is None:
            raise ClientError(
                {
                    "Error": {"Code": "404", "Message": "Not Found"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            )
        return {
            "ContentLength": len(record["Body"]),
            "ContentType": record["ContentType"],
            "Metadata": dict(record["Metadata"]),
        }

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
        Metadata: dict[str, str],
    ) -> dict[str, Any]:
        if Bucket not in self.buckets:
            raise ValueError(f"Bucket inexistant : {Bucket}")
        self.put_calls += 1
        self.objects[(Bucket, Key)] = {
            "Body": Body,
            "ContentType": ContentType,
            "Metadata": {k.lower(): v for k, v in Metadata.items()},
        }
        return {"ETag": '"fake-etag"'}


def test_ensure_lakehouse_buckets_is_idempotent() -> None:
    fake_s3 = FakeS3Client()
    target_buckets = ("sds-raw-referentials", "sds-bronze", "sds-silver", "sds-gold")

    created_first, existing_first = ensure_lakehouse_buckets(fake_s3, target_buckets)
    assert created_first == target_buckets
    assert existing_first == ()

    created_second, existing_second = ensure_lakehouse_buckets(fake_s3, target_buckets)
    assert created_second == ()
    assert existing_second == target_buckets


def test_sync_raw_referentials_idempotency_and_delta_upload(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    hr_nfd_path = raw_dir / "Donne\u0301es+RH.xlsx"
    sports_nfd_path = raw_dir / "Donne\u0301es+Sportive.xlsx"
    hr_nfd_path.write_bytes(b"xlsx-hr-payload-v1")
    sports_nfd_path.write_bytes(b"xlsx-sports-payload-v1")

    settings = EnvSettings(raw_data_dir=raw_dir)
    fake_s3 = FakeS3Client()

    first_run = sync_raw_referentials(settings=settings, s3_client=fake_s3)
    assert first_run.created_buckets == settings.lakehouse_buckets()
    assert all(obj.uploaded for obj in first_run.objects)
    assert fake_s3.put_calls == 2

    hr_record = fake_s3.objects[(settings.minio_bucket_raw, settings.hr_referential_s3_key)]
    assert hr_record["Metadata"]["sha256"] == compute_file_sha256(hr_nfd_path)

    second_run = sync_raw_referentials(settings=settings, s3_client=fake_s3)
    assert second_run.created_buckets == ()
    assert not any(obj.uploaded for obj in second_run.objects)
    assert fake_s3.put_calls == 2

    hr_nfd_path.write_bytes(b"xlsx-hr-payload-v2-updated")
    third_run = sync_raw_referentials(settings=settings, s3_client=fake_s3)
    upload_flags = {obj.s3_key: obj.uploaded for obj in third_run.objects}
    assert upload_flags == {
        settings.hr_referential_s3_key: True,
        settings.sports_referential_s3_key: False,
    }
    assert fake_s3.put_calls == 3


def test_sync_raw_referentials_raises_when_source_missing(tmp_path: Path) -> None:
    empty_raw_dir = tmp_path / "empty_raw"
    empty_raw_dir.mkdir()
    settings = EnvSettings(raw_data_dir=empty_raw_dir)
    fake_s3 = FakeS3Client()

    with pytest.raises(FileNotFoundError):
        sync_raw_referentials(settings=settings, s3_client=fake_s3)
