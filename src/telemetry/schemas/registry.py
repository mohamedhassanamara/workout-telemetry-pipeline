"""Export Pydantic contracts to schemas/*.json and register them with BACKWARD compatibility."""

from __future__ import annotations

import argparse
import json
import logging
import sys
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


def inline_refs(schema: dict) -> dict:
    """Return a copy of `schema` with every local `$ref` replaced by its definition.

    Pydantic emits `$ref`/`$defs` for enums; the registry's JSON Schema compatibility checker
    cannot evaluate `$ref`, so the schema is flattened before it is exported or registered.
    """
    defs = schema.get("$defs", {})

    def resolve(node, stack: tuple[str, ...]):
        if isinstance(node, list):
            return [resolve(item, stack) for item in node]
        if not isinstance(node, dict):
            return node
        if "$ref" in node:
            ref = node["$ref"]
            prefix = "#/$defs/"
            if not isinstance(ref, str) or not ref.startswith(prefix):
                raise ValueError(f"unsupported $ref: {ref!r}")
            name = ref[len(prefix) :]
            if name not in defs:
                raise ValueError(f"unresolvable $ref: {ref!r}")
            if name in stack:
                raise ValueError(f"cyclic $ref: {' -> '.join((*stack, name))}")
            merged = resolve(defs[name], (*stack, name))
            siblings = {k: resolve(v, stack) for k, v in node.items() if k != "$ref"}
            return {**merged, **siblings}
        return {k: resolve(v, stack) for k, v in node.items()}

    return resolve({k: v for k, v in schema.items() if k != "$defs"}, ())


def export_schema(subject: str, model: type[BaseModel]) -> str:
    text = json.dumps(inline_refs(model.model_json_schema()), indent=2, sort_keys=True)
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


def check_compatibility(client: httpx.Client, subject: str, schema_text: str) -> bool:
    """Ask the registry whether `schema_text` is compatible with the latest version."""
    resp = client.post(
        f"/compatibility/subjects/{subject}/versions/latest",
        json={"schemaType": "JSON", "schema": schema_text},
    )
    if resp.status_code == 404:  # no versions yet: nothing to be incompatible with
        return True
    resp.raise_for_status()
    return bool(resp.json()["is_compatible"])


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="check compatibility against the registry without registering; exit 1 if incompatible",
    )
    args = parser.parse_args(argv)
    settings = get_settings()
    setup_logging(settings.log_level)
    with httpx.Client(base_url=settings.schema_registry_url, timeout=10) as client:
        if args.check:
            incompatible = False
            for subject, model in contracts(settings).items():
                ok = check_compatibility(client, subject, export_schema(subject, model))
                print(f"{subject}: {'compatible' if ok else 'INCOMPATIBLE'}")
                incompatible |= not ok
            sys.exit(1 if incompatible else 0)
        for subject, model in contracts(settings).items():
            schema_id = register(client, subject, export_schema(subject, model))
            client.put(f"/config/{subject}", json={"compatibility": "BACKWARD"}).raise_for_status()
            versions = client.get(f"/subjects/{subject}/versions").json()
            log.info("%s -> id=%s versions=%s compatibility=BACKWARD", subject, schema_id, versions)


if __name__ == "__main__":
    main()
