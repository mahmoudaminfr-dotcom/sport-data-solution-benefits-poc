from __future__ import annotations

from importlib import import_module
from typing import Any

_PIPELINE_MONITOR_EXPORTS = {
    "BI_READER_MONITORING_VIEWS",
    "MONITORING_VIEW_NAMES",
    "LayerFreshnessMetric",
    "PipelineAlertResult",
    "PipelineHealthMetric",
    "PlatformHealthReport",
    "QualityLayerSummary",
    "build_platform_health_report",
    "classify_pipeline_layer",
    "consolidate_monitoring",
    "ensure_monitoring_views",
    "evaluate_platform_health",
    "format_health_report_text",
    "format_pipeline_alert_message",
    "send_pipeline_slack_alert",
}

_KESTRA_ORCHESTRATOR_EXPORTS = {
    "EXPECTED_TASK_ORDER",
    "KestraDeploymentResult",
    "KestraExecutionResult",
    "KestraOrchestrationError",
    "deploy_flow_idempotent",
    "load_and_validate_flow_yaml",
    "trigger_and_wait_flow",
    "validate_flow_remotely",
}

__all__ = sorted(_PIPELINE_MONITOR_EXPORTS | _KESTRA_ORCHESTRATOR_EXPORTS)


def __getattr__(name: str) -> Any:
    if name in _PIPELINE_MONITOR_EXPORTS:
        module = import_module("src.monitoring.pipeline_monitor")
        return getattr(module, name)
    if name in _KESTRA_ORCHESTRATOR_EXPORTS:
        module = import_module("src.monitoring.kestra_orchestrator")
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")