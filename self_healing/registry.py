"""Pipeline registry persistence utilities for self-healing pipelines."""

import os
import json
from datetime import datetime
import re


def _get_storage_dir() -> str:
    """Return the absolute path to the pipeline storage directory."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "storage")


def _ensure_storage_dir() -> str:
    """Ensure the storage directory exists and return its absolute path."""
    storage_dir = _get_storage_dir()
    os.makedirs(storage_dir, exist_ok=True)
    return storage_dir


def _sanitize_pipeline_id(pipeline_id: str) -> str:
    """Normalize pipeline_id for safe file naming."""
    normalized = (pipeline_id or "").strip().lower().replace(" ", "_")
    normalized = re.sub(r"[^a-z0-9_]", "", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        raise ValueError("pipeline_id must contain at least one alphanumeric character after sanitization")
    return normalized


def _pipeline_file_path(pipeline_id: str) -> str:
    """Build the absolute file path for a pipeline JSON file."""
    safe_id = _sanitize_pipeline_id(pipeline_id)
    return os.path.join(_ensure_storage_dir(), f"{safe_id}.json")


def save_pipeline(
    pipeline_id: str,
    requirement: str,
    source_tables: list[str],
    schema_snapshot: dict,
    staging_sql: str,
    transform_sql: str,
    business_sql: str,
) -> str:
    """Persist a pipeline JSON file to disk and return its absolute path."""
    safe_id = _sanitize_pipeline_id(pipeline_id)
    file_path = _pipeline_file_path(safe_id)

    payload = {
        "pipeline_id": safe_id,
        "requirement": requirement,
        "source_tables": source_tables,
        "schema_snapshot": schema_snapshot,
        "sql": {
            "staging": staging_sql,
            "transform": transform_sql,
            "business": business_sql,
        },
        "last_updated": datetime.utcnow().isoformat(),
    }

    with open(file_path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)

    return os.path.abspath(file_path)


def load_pipeline(pipeline_id: str) -> dict:
    """Load and return a pipeline JSON dictionary by pipeline_id."""
    file_path = _pipeline_file_path(pipeline_id)

    if not os.path.exists(file_path):
        safe_id = _sanitize_pipeline_id(pipeline_id)
        raise FileNotFoundError(
            f"Pipeline '{safe_id}' was not found in storage directory: {_get_storage_dir()}"
        )

    with open(file_path, "r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def list_pipelines() -> list[dict]:
    """List pipeline metadata for all stored pipeline JSON files."""
    storage_dir = _ensure_storage_dir()
    pipelines = []

    for file_name in sorted(os.listdir(storage_dir)):
        if not file_name.lower().endswith(".json"):
            continue

        file_path = os.path.join(storage_dir, file_name)
        if not os.path.isfile(file_path):
            continue

        with open(file_path, "r", encoding="utf-8") as file_obj:
            payload = json.load(file_obj)

        pipelines.append(
            {
                "pipeline_id": payload.get("pipeline_id", ""),
                "requirement": payload.get("requirement", ""),
                "source_tables": payload.get("source_tables", []),
                "last_updated": payload.get("last_updated", ""),
            }
        )

    return pipelines


def pipeline_exists(pipeline_id: str) -> bool:
    """Return True if the pipeline JSON file exists in storage."""
    file_path = _pipeline_file_path(pipeline_id)
    return os.path.exists(file_path)
