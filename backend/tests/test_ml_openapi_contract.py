from __future__ import annotations

from pathlib import Path

import yaml


EXPECTED_PATHS = {
    "/api/v1/pipelines/generate",
    "/api/v1/pipelines/dialogs",
    "/api/v1/pipelines/dialogs/{dialog_id}/history",
    "/api/v1/pipelines/dialog/reset",
    "/api/v1/pipelines/{pipeline_id}/run",
    "/api/v1/executions",
    "/api/v1/executions/{run_id}",
}


def _load_ml_spec() -> dict:
    repo_root = Path(__file__).resolve().parents[2]
    spec_path = repo_root / "docs" / "ml_core_backend_openapi.yaml"
    assert spec_path.exists(), f"Missing OpenAPI file: {spec_path}"
    with spec_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    assert isinstance(data, dict), "OpenAPI document must be a mapping"
    return data


def test_ml_openapi_paths_contract():
    spec = _load_ml_spec()
    paths = spec.get("paths", {})
    assert isinstance(paths, dict), "paths section must be an object"
    assert set(paths.keys()) == EXPECTED_PATHS
    assert len(paths) == 7


def test_ml_openapi_has_bearer_auth():
    spec = _load_ml_spec()
    components = spec.get("components", {})
    security_schemes = components.get("securitySchemes", {})
    bearer_auth = security_schemes.get("bearerAuth")

    assert isinstance(bearer_auth, dict), "bearerAuth security scheme must exist"
    assert bearer_auth.get("type") == "http"
    assert bearer_auth.get("scheme") == "bearer"
    assert bearer_auth.get("bearerFormat") == "JWT"


def test_ml_openapi_status_enums_contract():
    spec = _load_ml_spec()
    schemas = spec["components"]["schemas"]

    assert set(
        schemas["PipelineGenerateResponse"]["properties"]["status"]["enum"]
    ) == {"ready", "needs_input", "cannot_build"}
    assert set(
        schemas["RunPipelineResponse"]["properties"]["status"]["enum"]
    ) == {"QUEUED", "RUNNING"}
    assert set(
        schemas["ExecutionRunListItemResponse"]["properties"]["status"]["enum"]
    ) == {"QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "PARTIAL_FAILED"}
    assert set(
        schemas["ExecutionRunDetailResponse"]["properties"]["status"]["enum"]
    ) == {"QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "PARTIAL_FAILED"}
    assert set(
        schemas["ExecutionStepRunResponse"]["properties"]["status"]["enum"]
    ) == {"PENDING", "RUNNING", "SUCCEEDED", "FAILED", "SKIPPED"}

