import gzip

from telemetry.clients import make_s3_client
from telemetry.config import Settings
from telemetry.consumers.bronze_sink.batcher import RawRecord


class BronzeWriter:
    def __init__(self, settings: Settings) -> None:
        self._s3 = make_s3_client(settings)  # retries transient errors (Step 3 config)
        self._bucket = settings.s3_bucket

    def write(self, key: str, records: list[RawRecord]) -> int:
        body = gzip.compress(b"\n".join(r.line for r in records) + b"\n", mtime=0)
        self._s3.put_object(
            Bucket=self._bucket, Key=key, Body=body, ContentType="application/x-ndjson"
        )
        return len(body)
