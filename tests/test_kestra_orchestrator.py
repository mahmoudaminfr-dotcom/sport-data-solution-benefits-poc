from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from src.monitoring import (
    EXPECTED_TASK_ORDER,
    KestraDeploymentResult,
    KestraExecutionResult,
    KestraOrchestrationError,
    deploy_flow_idempotent,
    load_and_validate_flow_yaml,
    trigger_and_wait_flow,
    validate_flow_remotely,
)
from src.monitoring.kestra_orchestrator import KestraTaskRunSummary
from src.monitoring.kestra_orchestrator import main as kestra_main
from src.settings import PROJECT_ROOT, EnvSettings


def _test_settings() -> EnvSettings:
    return EnvSettings(
        _env_file=None,
        kestra_url="http://kestra.local:8085",
        kestra_api_tenant="main",
        kestra_basic_auth_enabled=True,
        kestra_basic_auth_username="ops@sportdatasolution.fr",
        kestra_basic_auth_password=SecretStr("TestKestraSecret99!"),
        kestra_namespace="sds.benefits",
        kestra_daily_flow_id="daily_benefits_pipeline",
    )


def test_daily_benefits_flow_yaml_structural_contract() -> None:
    flow_path = PROJECT_ROOT / "flows" / "daily_benefits_pipeline.yml"
    raw_yaml, parsed = load_and_validate_flow_yaml(flow_path)

    assert parsed["id"] == "daily_benefits_pipeline"
    assert parsed["namespace"] == "sds.benefits"
    assert parsed["concurrency"]["limit"] == 1
    assert tuple(t["id"] for t in parsed["tasks"]) == EXPECTED_TASK_ORDER
    assert all("required" not in inp for inp in parsed["inputs"])

    soda_task = next(t for t in parsed["tasks"] if t["id"] == "run_soda_quality_gate")
    soda_cmds = " ".join(soda_task["commands"])
    assert "src.quality.soda_runner" in soda_cmds
    assert "--no-fail-exit" not in soda_cmds

    err_cmds = " ".join(parsed["errors"][0]["commands"])
    assert "--send-failure-alert" in err_cmds
    assert len(raw_yaml) > 500


def test_load_and_validate_flow_yaml_rejects_invalid_manifests(tmp_path: Path) -> None:
    missing = tmp_path / "absent.yml"
    with pytest.raises(KestraOrchestrationError, match="introuvable"):
        load_and_validate_flow_yaml(missing)

    wrong_id = tmp_path / "wrong_id.yml"
    wrong_id.write_text("id: wrong\nnamespace: other\n", encoding="utf-8")
    with pytest.raises(KestraOrchestrationError, match="Identite du flow inattendue"):
        load_and_validate_flow_yaml(wrong_id)

    broken_order = tmp_path / "broken_order.yml"
    broken_order.write_text(
        "\n".join(
            [
                "id: daily_benefits_pipeline",
                "namespace: sds.benefits",
                "tasks:",
                "  - id: only_one_step",
                "    type: io.kestra.plugin.scripts.shell.Commands",
                "    taskRunner:",
                "      type: io.kestra.plugin.core.runner.Process",
                "    commands:",
                "      - echo ok",
                "errors: []",
                "triggers: []",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(KestraOrchestrationError, match="Ordre sequentiel des taches invalide"):
        load_and_validate_flow_yaml(broken_order)

    broken_input = tmp_path / "broken_input.yml"
    broken_input.write_text(
        "\n".join(
            [
                "id: daily_benefits_pipeline",
                "namespace: sds.benefits",
                "inputs:",
                "  - id: reset_defaults",
                "    type: BOOL",
                "    defaults: true",
                "    required: false",
                "tasks: []",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(KestraOrchestrationError, match="Input Kestra 2.0 invalide"):
        load_and_validate_flow_yaml(broken_input)


def test_deploy_flow_idempotent_create_then_update() -> None:
    settings = _test_settings()
    flow_state: dict[str, Any] = {"exists": False, "revision": 0}
    recorded_calls: list[tuple[str, str]] = []
    flow_route = "/api/v1/main/flows/sds.benefits/daily_benefits_pipeline"

    def _handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Authorization", "").startswith("Basic ")
        path = request.url.path
        recorded_calls.append((request.method, path))

        if request.method == "POST" and path == "/api/v1/main/flows/validate":
            return httpx.Response(200, json=[{"index": 0, "warnings": ["minor note"]}])

        if request.method == "GET" and path == flow_route:
            if not flow_state["exists"]:
                return httpx.Response(404, json={"title": "Not Found"})
            return httpx.Response(
                200,
                json={
                    "id": "daily_benefits_pipeline",
                    "namespace": "sds.benefits",
                    "revision": flow_state["revision"],
                },
            )

        if request.method == "POST" and path == "/api/v1/main/flows":
            flow_state["exists"] = True
            flow_state["revision"] = 1
            return httpx.Response(
                200,
                json={
                    "id": "daily_benefits_pipeline",
                    "namespace": "sds.benefits",
                    "revision": 1,
                    "disabled": False,
                },
            )

        if request.method == "PUT" and path == flow_route:
            return httpx.Response(
                200,
                json={
                    "id": "daily_benefits_pipeline",
                    "namespace": "sds.benefits",
                    "revision": flow_state["revision"],
                    "disabled": False,
                },
            )

        return httpx.Response(500, text="Unexpected route")

    with httpx.Client(transport=httpx.MockTransport(_handler)) as mock_client:
        first = deploy_flow_idempotent(settings=settings, http_client=mock_client)
        second = deploy_flow_idempotent(settings=settings, http_client=mock_client)

    assert first.action == "CREATED"
    assert first.revision == 1
    assert first.tasks_count == 6
    assert second.action == "UPDATED"
    assert second.revision == 1
    assert ("POST", "/api/v1/main/flows") in recorded_calls
    assert ("PUT", flow_route) in recorded_calls


def test_validate_flow_remotely_raises_on_constraint_or_http_error() -> None:
    settings = _test_settings()

    def _constraint_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=json.dumps([{"index": 0, "constraints": "Invalid cron expression"}]),
        )

    with httpx.Client(transport=httpx.MockTransport(_constraint_handler)) as mock_client:
        with pytest.raises(KestraOrchestrationError, match="Invalid cron expression"):
            validate_flow_remotely("id: bad", settings=settings, http_client=mock_client)

    def _http_err_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, text="Unauthorized")

    with httpx.Client(transport=httpx.MockTransport(_http_err_handler)) as mock_client:
        with pytest.raises(KestraOrchestrationError, match="HTTP 401"):
            validate_flow_remotely("id: bad", settings=settings, http_client=mock_client)


def test_trigger_and_wait_flow_polls_and_timeout() -> None:
    settings = _test_settings()
    polls = {"count": 0}

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith(
            "/executions/sds.benefits/daily_benefits_pipeline"
        ):
            return httpx.Response(
                200,
                json={
                    "id": "exec-sds-001",
                    "namespace": "sds.benefits",
                    "flowId": "daily_benefits_pipeline",
                    "flowRevision": 1,
                    "state": {"current": "RUNNING"},
                    "taskRunList": [],
                },
            )
        if request.method == "GET" and request.url.path.endswith("/executions/exec-sds-001"):
            polls["count"] += 1
            current = "RUNNING" if polls["count"] == 1 else "SUCCESS"
            return httpx.Response(
                200,
                json={
                    "id": "exec-sds-001",
                    "namespace": "sds.benefits",
                    "flowId": "daily_benefits_pipeline",
                    "flowRevision": 1,
                    "state": {"current": current},
                    "taskRunList": [
                        {
                            "taskId": "run_soda_quality_gate",
                            "state": {"current": current, "duration": "PT4.1S"},
                        }
                    ],
                },
            )
        return httpx.Response(404)

    with httpx.Client(transport=httpx.MockTransport(_handler)) as mock_client:
        res = trigger_and_wait_flow(
            inputs={"reset_defaults": True},
            poll_interval_seconds=0.01,
            settings=settings,
            http_client=mock_client,
        )

    assert res.execution_id == "exec-sds-001"
    assert res.state == "SUCCESS"
    assert polls["count"] == 2
    assert res.to_dict()["task_runs"][0]["task_id"] == "run_soda_quality_gate"

    def _stuck_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "exec-stuck",
                "namespace": "sds.benefits",
                "flowId": "daily_benefits_pipeline",
                "flowRevision": 1,
                "state": {"current": "RUNNING"},
            },
        )

    with httpx.Client(transport=httpx.MockTransport(_stuck_handler)) as stuck_client:
        with pytest.raises(KestraOrchestrationError, match="Expiration"):
            trigger_and_wait_flow(
                timeout_seconds=0.02,
                poll_interval_seconds=0.01,
                settings=settings,
                http_client=stuck_client,
            )


def test_kestra_cli_entrypoints(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc_val = kestra_main(["--validate-only", "--json"])
    out_val = json.loads(capsys.readouterr().out)
    assert rc_val == 0
    assert out_val["status"] == "VALID_LOCAL_YAML"
    assert len(out_val["tasks"]) == 6

    monkeypatch.setattr(
        "src.monitoring.kestra_orchestrator.deploy_flow_idempotent",
        lambda settings=None: KestraDeploymentResult(
            namespace="sds.benefits",
            flow_id="daily_benefits_pipeline",
            revision=2,
            action="UPDATED",
            disabled=False,
            tasks_count=6,
            triggers_count=1,
        ),
    )
    monkeypatch.setattr(
        "src.monitoring.kestra_orchestrator.trigger_and_wait_flow",
        lambda **kwargs: KestraExecutionResult(
            execution_id="exec-cli-ok",
            namespace="sds.benefits",
            flow_id="daily_benefits_pipeline",
            flow_revision=2,
            state="SUCCESS",
            elapsed_seconds=5.4,
            task_runs=[
                KestraTaskRunSummary(
                    task_id="consolidate_ops_monitoring",
                    state="SUCCESS",
                    duration="PT0.3S",
                )
            ],
        ),
    )
    rc_ok = kestra_main(["--deploy", "--trigger"])
    out_ok = capsys.readouterr().out
    assert rc_ok == 0
    assert "action=UPDATED" in out_ok
    assert "state=SUCCESS" in out_ok

    monkeypatch.setattr(
        "src.monitoring.kestra_orchestrator.trigger_and_wait_flow",
        lambda **kwargs: KestraExecutionResult(
            execution_id="exec-cli-ko",
            namespace="sds.benefits",
            flow_id="daily_benefits_pipeline",
            flow_revision=2,
            state="FAILED",
            elapsed_seconds=1.2,
            task_runs=[],
        ),
    )
    rc_ko = kestra_main(["--trigger", "--json"])
    out_ko = json.loads(capsys.readouterr().out)
    assert rc_ko == 1
    assert out_ko["execution"]["state"] == "FAILED"