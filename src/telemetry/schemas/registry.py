"""Export Pydantic contracts to schemas/*.json and register them with BACKWARD compatibility."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import httpx
from pydantic import BaseModel

from telemetry.config import Settings, get_settings
from telemetry.log import setup_logging
from telemetry.schemas.events import DailyRecoveryLogged, WorkoutSetLogged

log = logging.getLogger("registry")
SCHEMA_DIR = Path("schemas")


def contracts(settings: Settings) -> dict[str, type[BaseModel]]:
    # TopicNameStrategy: subject = "<topic>-value"
    return {
        f"{settings.topic_sets}-value": WorkoutSetLogged,
        f"{settings.topic_recovery}-value": DailyRecoveryLogged,
    }


def export_schema(subject: str, model: type[BaseModel]) -> str:
    text = json.dumps(model.model_json_schema(), indent=2, sort_keys=True)
    SCHEMA_DIR.mkdir(exist_ok=True)
    (SCHEMA_DIR / f"{subject}.json").write_text(text + "\n")
    return text


def register(client: httpx.Client, subject: str, schema_text: str) -> int:
    resp = client.post(
        f"/subjects/{subject}/versions",
        json={"schemaType": "JSON", "schema": schema_text},
    )
    if resp.status_code == 409:
        raise SystemExit(f"{subject}: incompatible with the latest version: {resp.text}")
    resp.raise_for_status()
    return resp.json()["id"]


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    with httpx.Client(base_url=settings.schema_registry_url, timeout=10) as client:
        for subject, model in contracts(settings).items():
            schema_id = register(client, subject, export_schema(subject, model))
            client.put(f"/config/{subject}", json={"compatibility": "BACKWARD"}).raise_for_status()
            versions = client.get(f"/subjects/{subject}/versions").json()
            log.info("%s -> id=%s versions=%s compatibility=BACKWARD", subject, schema_id, versions)


if __name__ == "__main__":
    main()
