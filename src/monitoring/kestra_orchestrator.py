from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

import httpx
import yaml

from src.settings import EnvSettings

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

DeploymentAction = Literal["CREATED", "UPDATED"]

EXPECTED_TASK_ORDER: tuple[str, ...] = (
    "bootstrap_python_runtime",
    "sync_s3_and_postgres_oltp",
    "refresh_lakehouse_bronze_silver",
    "recompute_lakehouse_gold_and_mart",
    "run_soda_quality_gate",
    "consolidate_ops_monitoring",
)

TERMINAL_EXECUTION_STATES: frozenset[str] = frozenset(
    {"SUCCESS", "WARNING", "FAILED", "KILLED", "CANCELLED"}
)


class KestraOrchestrationError(RuntimeError):
    """Erreur levee lors de la validation, du deploiement ou de l'execution d'un flow Kestra."""


@dataclass(frozen=True)
class KestraDeploymentResult:
    namespace: str
    flow_id: str
    revision: int
    action: DeploymentAction
    disabled: bool
    tasks_count: int
    triggers_count: int


@dataclass(frozen=True)
class KestraTaskRunSummary:
    task_id: str
    state: str
    duration: str | None


@dataclass(frozen=True)
class KestraExecutionResult:
    execution_id: str
    namespace: str
    flow_id: str
    flow_revision: int
    state: str
    elapsed_seconds: float
    task_runs: list[KestraTaskRunSummary]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_and_validate_flow_yaml(
    flow_path: Path,
    *,
    expected_namespace: str = "sds.benefits",
    expected_flow_id: str = "daily_benefits_pipeline",
) -> tuple[str, dict[str, Any]]:
    if not flow_path.is_file():
        raise KestraOrchestrationError(f"Fichier de flow Kestra introuvable : {flow_path}")

    raw_yaml = flow_path.read_text(encoding="utf-8")
    try:
        parsed = yaml.safe_load(raw_yaml)
    except yaml.YAMLError as exc:
        raise KestraOrchestrationError(
            f"Syntaxe YAML invalide dans {flow_path} : {exc}"
        ) from exc

    if not isinstance(parsed, dict):
        raise KestraOrchestrationError("Le manifeste Kestra doit etre un objet YAML racine.")

    flow_id = parsed.get("id")
    namespace = parsed.get("namespace")
    if flow_id != expected_flow_id or namespace != expected_namespace:
        raise KestraOrchestrationError(
            f"Identite du flow inattendue ({namespace}/{flow_id}) vs "
            f"attendu ({expected_namespace}/{expected_flow_id})."
        )

    for inp in parsed.get("inputs") or []:
        if isinstance(inp, dict) and "defaults" in inp and inp.get("required") is False:
            raise KestraOrchestrationError(
                f"Input Kestra 2.0 invalide ({inp.get('id')!r}) : "
                "un input avec 'defaults' ne doit pas declarer 'required: false'."
            )

    tasks = parsed.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise KestraOrchestrationError("Le flow Kestra doit declarer une liste non vide de tasks.")

    task_ids = tuple(str(t.get("id", "")) for t in tasks if isinstance(t, dict))
    if task_ids != EXPECTED_TASK_ORDER:
        raise KestraOrchestrationError(
            f"Ordre sequentiel des taches invalide : {task_ids} vs {EXPECTED_TASK_ORDER}."
        )

    for task in tasks:
        runner = (task.get("taskRunner") or {}).get("type")
        if runner != "io.kestra.plugin.core.runner.Process":
            raise KestraOrchestrationError(
                f"La tache {task.get('id')!r} doit utiliser le runner Process local."
            )

    errors_handlers = parsed.get("errors")
    if not isinstance(errors_handlers, list) or not errors_handlers:
        raise KestraOrchestrationError(
            "Le flow Kestra doit definir un bloc errors d'alerte Slack en cas d'echec."
        )

    triggers = parsed.get("triggers")
    if not isinstance(triggers, list) or not triggers:
        raise KestraOrchestrationError(
            "Le flow Kestra doit definir au moins un declencheur planifie dans triggers."
        )
    has_schedule = any(
        isinstance(tr, dict) and tr.get("type") == "io.kestra.plugin.core.trigger.Schedule"
        for tr in triggers
    )
    if not has_schedule:
        raise KestraOrchestrationError(
            "Aucun declencheur de type io.kestra.plugin.core.trigger.Schedule trouve."
        )

    return raw_yaml, parsed


def validate_flow_remotely(
    raw_yaml: str,
    *,
    settings: EnvSettings | None = None,
    http_client: httpx.Client | None = None,
) -> list[str]:
    cfg = settings or EnvSettings()
    url = f"{cfg.kestra_api_base_url()}/flows/validate"
    auth = cfg.kestra_basic_auth_tuple()
    owns_client = http_client is None
    client = (
        httpx.Client(timeout=cfg.http_timeout_seconds)
        if owns_client
        else http_client
    )

    try:
        response = client.post(
            url,
            content=raw_yaml.encode("utf-8"),
            headers={"Content-Type": "application/x-yaml"},
            auth=auth,
        )
    except httpx.HTTPError as exc:
        raise KestraOrchestrationError(
            f"Impossible de joindre l'API Kestra ({url}) : {exc}"
        ) from exc
    finally:
        if owns_client:
            client.close()

    if response.status_code != 200:
        raise KestraOrchestrationError(
            f"Validation distante Kestra en echec (HTTP {response.status_code}) : "
            f"{response.text[:300]}"
        )

    payload = response.json()
    constraints: list[str] = []
    warnings: list[str] = []
    items = payload if isinstance(payload, list) else [payload]
    for item in items:
        if not isinstance(item, dict):
            continue
        err = item.get("constraints")
        if err:
            constraints.append(str(err))
        for warn in item.get("warnings") or []:
            warnings.append(str(warn))

    if constraints:
        joined = "; ".join(constraints)
        raise KestraOrchestrationError(f"Contraintes Kestra violees : {joined}")

    return warnings


def deploy_flow_idempotent(
    *,
    flow_path: Path | None = None,
    settings: EnvSettings | None = None,
    http_client: httpx.Client | None = None,
) -> KestraDeploymentResult:
    cfg = settings or EnvSettings()
    target_path = flow_path or cfg.kestra_daily_flow_path
    raw_yaml, parsed = load_and_validate_flow_yaml(
        target_path,
        expected_namespace=cfg.kestra_namespace,
        expected_flow_id=cfg.kestra_daily_flow_id,
    )

    base_api = cfg.kestra_api_base_url()
    namespace = cfg.kestra_namespace
    flow_id = cfg.kestra_daily_flow_id
    auth = cfg.kestra_basic_auth_tuple()
    yaml_headers = {"Content-Type": "application/x-yaml"}

    owns_client = http_client is None
    client = (
        httpx.Client(timeout=cfg.http_timeout_seconds)
        if owns_client
        else http_client
    )

    try:
        validate_flow_remotely(raw_yaml, settings=cfg, http_client=client)

        get_url = f"{base_api}/flows/{namespace}/{flow_id}"
        probe = client.get(get_url, auth=auth)
        if probe.status_code == 404:
            deploy_resp = client.post(
                f"{base_api}/flows",
                content=raw_yaml.encode("utf-8"),
                headers=yaml_headers,
                auth=auth,
            )
            action: DeploymentAction = "CREATED"
        elif probe.status_code == 200:
            deploy_resp = client.put(
                get_url,
                content=raw_yaml.encode("utf-8"),
                headers=yaml_headers,
                auth=auth,
            )
            action = "UPDATED"
        else:
            raise KestraOrchestrationError(
                f"Lecture prealable du flow Kestra echouee (HTTP {probe.status_code}) : "
                f"{probe.text[:240]}"
            )

        if deploy_resp.status_code not in (200, 201):
            raise KestraOrchestrationError(
                f"Deploiement du flow Kestra echoue (HTTP {deploy_resp.status_code}) : "
                f"{deploy_resp.text[:300]}"
            )

        body = deploy_resp.json()
        return KestraDeploymentResult(
            namespace=str(body.get("namespace", namespace)),
            flow_id=str(body.get("id", flow_id)),
            revision=int(body.get("revision", 1)),
            action=action,
            disabled=bool(body.get("disabled", False)),
            tasks_count=len(parsed.get("tasks") or []),
            triggers_count=len(parsed.get("triggers") or []),
        )
    except httpx.HTTPError as exc:
        raise KestraOrchestrationError(
            f"Erreur reseau lors du deploiement Kestra : {exc}"
        ) from exc
    finally:
        if owns_client:
            client.close()


def trigger_and_wait_flow(
    *,
    inputs: dict[str, Any] | None = None,
    wait: bool = True,
    timeout_seconds: float = 180.0,
    poll_interval_seconds: float = 1.5,
    settings: EnvSettings | None = None,
    http_client: httpx.Client | None = None,
) -> KestraExecutionResult:
    cfg = settings or EnvSettings()
    base_api = cfg.kestra_api_base_url()
    namespace = cfg.kestra_namespace
    flow_id = cfg.kestra_daily_flow_id
    auth = cfg.kestra_basic_auth_tuple()

    owns_client = http_client is None
    client = (
        httpx.Client(timeout=max(cfg.http_timeout_seconds, 20.0))
        if owns_client
        else http_client
    )

    t0 = time.perf_counter()
    try:
        trigger_url = f"{base_api}/executions/{namespace}/{flow_id}"
        if inputs:
            multipart_fields = {
                k: (None, str(v).lower() if isinstance(v, bool) else str(v))
                for k, v in inputs.items()
            }
            trig_resp = client.post(trigger_url, files=multipart_fields, auth=auth)
        else:
            trig_resp = client.post(trigger_url, auth=auth)

        if trig_resp.status_code not in (200, 201):
            raise KestraOrchestrationError(
                f"Declenchement Kestra echoue (HTTP {trig_resp.status_code}) : "
                f"{trig_resp.text[:300]}"
            )

        exec_payload = trig_resp.json()
        execution_id = str(exec_payload["id"])

        while wait:
            current_state = str((exec_payload.get("state") or {}).get("current", "CREATED"))
            if current_state in TERMINAL_EXECUTION_STATES:
                break
            if (time.perf_counter() - t0) > timeout_seconds:
                raise KestraOrchestrationError(
                    f"Expiration ({timeout_seconds:.0f}s) en attente de l'execution "
                    f"{execution_id} (dernier etat={current_state})."
                )
            time.sleep(poll_interval_seconds)
            poll_resp = client.get(f"{base_api}/executions/{execution_id}", auth=auth)
            if poll_resp.status_code != 200:
                raise KestraOrchestrationError(
                    f"Lecture de l'execution {execution_id} echouee "
                    f"(HTTP {poll_resp.status_code})."
                )
            exec_payload = poll_resp.json()

        elapsed = round(time.perf_counter() - t0, 2)
        final_state = str((exec_payload.get("state") or {}).get("current", "UNKNOWN"))
        task_runs: list[KestraTaskRunSummary] = []
        for tr in exec_payload.get("taskRunList") or []:
            if not isinstance(tr, dict):
                continue
            tr_state = tr.get("state") or {}
            task_runs.append(
                KestraTaskRunSummary(
                    task_id=str(tr.get("taskId", "")),
                    state=str(tr_state.get("current", "UNKNOWN")),
                    duration=tr_state.get("duration"),
                )
            )

        return KestraExecutionResult(
            execution_id=execution_id,
            namespace=str(exec_payload.get("namespace", namespace)),
            flow_id=str(exec_payload.get("flowId", flow_id)),
            flow_revision=int(exec_payload.get("flowRevision", 1)),
            state=final_state,
            elapsed_seconds=elapsed,
            task_runs=task_runs,
        )
    except httpx.HTTPError as exc:
        raise KestraOrchestrationError(
            f"Erreur HTTP lors du suivi d'execution Kestra : {exc}"
        ) from exc
    finally:
        if owns_client:
            client.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validation, deploiement idempotent et execution du workflow Kestra SDS."
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Valide uniquement la structure YAML locale du workflow.",
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Valide et provisionne de facon idempotente le flow dans Kestra.",
    )
    parser.add_argument(
        "--trigger",
        action="store_true",
        help="Declenche une execution du workflow quotidien dans Kestra.",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Ne pas attendre la fin de l'execution lorsque --trigger est actif.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=240.0,
        help="Delai maximal d'attente d'une execution Kestra en secondes.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Restitue la sortie au format JSON.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    args = parse_args(argv)
    settings = EnvSettings()

    if args.validate_only or (not args.deploy and not args.trigger):
        _, parsed = load_and_validate_flow_yaml(
            settings.kestra_daily_flow_path,
            expected_namespace=settings.kestra_namespace,
            expected_flow_id=settings.kestra_daily_flow_id,
        )
        summary = {
            "status": "VALID_LOCAL_YAML",
            "namespace": parsed["namespace"],
            "flow_id": parsed["id"],
            "tasks": [t["id"] for t in parsed["tasks"]],
        }
        if args.as_json:
            print(json.dumps(summary, ensure_ascii=False, indent=2))
        else:
            print(
                f"[KESTRA VALIDATION] {summary['namespace']}/{summary['flow_id']} : "
                f"{len(summary['tasks'])} taches conformes."
            )
        return 0

    output_payload: dict[str, Any] = {}
    if args.deploy:
        dep = deploy_flow_idempotent(settings=settings)
        output_payload["deployment"] = asdict(dep)
        if not args.as_json:
            print(
                f"[KESTRA DEPLOY] action={dep.action} flow={dep.namespace}/{dep.flow_id} "
                f"revision={dep.revision} tasks={dep.tasks_count} triggers={dep.triggers_count}"
            )

    if args.trigger:
        exec_res = trigger_and_wait_flow(
            wait=not args.no_wait,
            timeout_seconds=args.timeout_seconds,
            settings=settings,
        )
        output_payload["execution"] = exec_res.to_dict()
        if not args.as_json:
            print(
                f"[KESTRA EXECUTION] id={exec_res.execution_id} state={exec_res.state} "
                f"elapsed={exec_res.elapsed_seconds}s tasks_ran={len(exec_res.task_runs)}"
            )
            for tr in exec_res.task_runs:
                print(f"  - {tr.task_id} : {tr.state} ({tr.duration})")
        if not args.no_wait and exec_res.state not in ("SUCCESS", "WARNING"):
            if args.as_json:
                print(json.dumps(output_payload, ensure_ascii=False, indent=2))
            return 1

    if args.as_json:
        print(json.dumps(output_payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())