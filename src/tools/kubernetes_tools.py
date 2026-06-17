"""
Kubernetes core query tools — extracted from server-mcp.py (issue #58).

Each function accepts injected Kubernetes API clients rather than relying on
module-level globals.  The thin ``@mcp.tool()`` wrappers that remain in
``server-mcp.py`` simply forward to these implementations.

Fixes #58
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

from kubernetes.client.rest import ApiException

from helpers.config import _namespace_cache, _NAMESPACE_CACHE_TTL
from helpers.utils import (
    calculate_utilization,
    format_detailed_output,
    format_summary_output,
    format_yaml_output,
    list_pods,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Resource-type dispatch tables
# ---------------------------------------------------------------------------

_CORE_RESOURCES: Dict[str, tuple] = {
    'pod': ('pods', 'v1'),
    'service': ('services', 'v1'),
    'configmap': ('config_maps', 'v1'),
    'secret': ('secrets', 'v1'),
    'pvc': ('persistent_volume_claims', 'v1'),
    'persistentvolumeclaim': ('persistent_volume_claims', 'v1'),
    'namespace': ('namespaces', 'v1'),
    'node': ('nodes', 'v1'),
    'serviceaccount': ('service_accounts', 'v1'),
    'endpoints': ('endpoints', 'v1'),
    'event': ('events', 'v1'),
    'persistentvolume': ('persistent_volumes', 'v1'),
    'pv': ('persistent_volumes', 'v1'),
    'resourcequota': ('resource_quotas', 'v1'),
    'limitrange': ('limit_ranges', 'v1'),
}

_APPS_RESOURCES: Dict[str, tuple] = {
    'deployment': ('deployments', 'apps/v1'),
    'replicaset': ('replica_sets', 'apps/v1'),
    'daemonset': ('daemon_sets', 'apps/v1'),
    'statefulset': ('stateful_sets', 'apps/v1'),
}

_BATCH_RESOURCES: Dict[str, tuple] = {
    'job': ('jobs', 'batch/v1'),
    'cronjob': ('cron_jobs', 'batch/v1'),
}

_NETWORKING_RESOURCES: Dict[str, tuple] = {
    'ingress': ('ingresses', 'networking.k8s.io/v1'),
}

_STORAGE_RESOURCES: Dict[str, tuple] = {
    'storageclass': ('storage_classes', 'storage.k8s.io/v1'),
    'sc': ('storage_classes', 'storage.k8s.io/v1'),
}

_AUTOSCALING_RESOURCES: Dict[str, tuple] = {
    'horizontalpodautoscaler': ('horizontal_pod_autoscalers', 'autoscaling/v2'),
    'hpa': ('horizontal_pod_autoscalers', 'autoscaling/v2'),
}

_TEKTON_RESOURCES: Dict[str, tuple] = {
    'pipelinerun': ('pipelineruns', 'tekton.dev/v1'),
    'taskrun': ('taskruns', 'tekton.dev/v1'),
    'pipeline': ('pipelines', 'tekton.dev/v1'),
    'task': ('tasks', 'tekton.dev/v1'),
    'clustertask': ('clustertasks', 'tekton.dev/v1beta1'),
}

_TEKTON_TRIGGERS_RESOURCES: Dict[str, tuple] = {
    'triggertemplate': ('triggertemplates', 'triggers.tekton.dev/v1beta1'),
    'triggerbinding': ('triggerbindings', 'triggers.tekton.dev/v1beta1'),
    'eventlistener': ('eventlisteners', 'triggers.tekton.dev/v1beta1'),
}

_MONITORING_RESOURCES: Dict[str, tuple] = {
    'podmonitor': ('podmonitors', 'monitoring.coreos.com/v1'),
    'servicemonitor': ('servicemonitors', 'monitoring.coreos.com/v1'),
    'prometheusrule': ('prometheusrules', 'monitoring.coreos.com/v1'),
    'alertmanager': ('alertmanagers', 'monitoring.coreos.com/v1'),
}

_ADMISSION_RESOURCES: Dict[str, tuple] = {
    'validatingadmissionwebhook': ('validatingadmissionwebhooks', 'admissionregistration.k8s.io/v1'),
    'mutatingadmissionwebhook': ('mutatingadmissionwebhooks', 'admissionregistration.k8s.io/v1'),
}

_KONFLUX_RESOURCES: Dict[str, tuple] = {
    'application': ('applications', 'appstudio.redhat.com/v1alpha1'),
    'component': ('components', 'appstudio.redhat.com/v1alpha1'),
    'snapshot': ('snapshots', 'appstudio.redhat.com/v1alpha1'),
    'release': ('releases', 'appstudio.redhat.com/v1alpha1'),
    'releaseplan': ('releaseplans', 'appstudio.redhat.com/v1alpha1'),
    'releaseplanadmission': ('releaseplanadmissions', 'appstudio.redhat.com/v1alpha1'),
    'integrationtestscenario': ('integrationtestscenarios', 'appstudio.redhat.com/v1beta2'),
}


def _all_supported_types() -> List[str]:
    """Return sorted list of all supported resource type names."""
    return sorted(
        list(_CORE_RESOURCES)
        + list(_APPS_RESOURCES)
        + list(_BATCH_RESOURCES)
        + list(_NETWORKING_RESOURCES)
        + list(_STORAGE_RESOURCES)
        + list(_AUTOSCALING_RESOURCES)
        + list(_TEKTON_RESOURCES)
        + list(_TEKTON_TRIGGERS_RESOURCES)
        + list(_MONITORING_RESOURCES)
        + list(_ADMISSION_RESOURCES)
        + list(_KONFLUX_RESOURCES)
    )


# ---------------------------------------------------------------------------
# Implementation functions
# ---------------------------------------------------------------------------

async def list_namespaces_impl(k8s_core_api: Any) -> List[str]:
    """List all namespaces in the Kubernetes cluster.

    Returns an alphabetically sorted list of namespace names.
    Empty list on access-denied or cluster-unreachable.
    """
    global _namespace_cache

    if not k8s_core_api:
        logger.warning("Kubernetes client not available, cannot list namespaces.")
        return []

    current_time = time.time()
    if (
        _namespace_cache["namespaces"] is not None
        and current_time - _namespace_cache["timestamp"] < _NAMESPACE_CACHE_TTL
    ):
        logger.debug("Returning cached namespace list")
        return _namespace_cache["namespaces"]

    try:
        logger.info("Retrieving all namespaces from Kubernetes cluster")
        namespaces = await asyncio.to_thread(k8s_core_api.list_namespace)
        ns_names = sorted(
            [ns.metadata.name for ns in namespaces.items if ns.metadata and ns.metadata.name]
        )
        _namespace_cache["namespaces"] = ns_names
        _namespace_cache["timestamp"] = current_time
        logger.info(f"Successfully retrieved {len(ns_names)} namespaces")
        return ns_names

    except ApiException as e:
        if e.status == 403:
            logger.warning(
                f"Insufficient permissions to list namespaces: {e.reason}. "
                "Check RBAC configuration."
            )
        elif e.status == 401:
            logger.error(
                f"Authentication failed while listing namespaces: {e.reason}. "
                "Check kubeconfig."
            )
        else:
            logger.error(f"API error while listing namespaces: {e.status} - {e.reason}")
        return []

    except Exception as e:
        logger.error(f"Unexpected error while listing namespaces: {str(e)}", exc_info=True)
        return []


async def list_pods_in_namespace_impl(namespace: str, k8s_core_api: Any) -> List[Dict[str, Any]]:
    """List all pods in a Kubernetes namespace with status and placement info.

    Returns a list of dicts with keys: name, status, ip, node_name,
    creation_timestamp, restart_count, container_states.
    """
    if not k8s_core_api:
        return [{"error": "Kubernetes client not available."}]

    pods_info: List[Dict[str, Any]] = []
    try:
        logger.info(f"Listing pods in namespace: {namespace}")
        pod_list_resp = await asyncio.to_thread(
            k8s_core_api.list_namespaced_pod, namespace=namespace
        )
        for pod in pod_list_resp.items:
            total_restart_count = 0
            container_states: List[str] = []

            if pod.status and pod.status.container_statuses:
                for cs in pod.status.container_statuses:
                    if cs.restart_count:
                        total_restart_count += cs.restart_count
                    if cs.state:
                        if cs.state.waiting and cs.state.waiting.reason:
                            container_states.append(cs.state.waiting.reason)
                        elif cs.state.terminated and cs.state.terminated.reason:
                            container_states.append(cs.state.terminated.reason)

            if pod.status and pod.status.init_container_statuses:
                for ics in pod.status.init_container_statuses:
                    if ics.restart_count:
                        total_restart_count += ics.restart_count
                    if ics.state:
                        if ics.state.waiting and ics.state.waiting.reason:
                            container_states.append(f"Init:{ics.state.waiting.reason}")
                        elif (
                            ics.state.terminated
                            and ics.state.terminated.reason
                            and ics.state.terminated.reason != "Completed"
                        ):
                            container_states.append(f"Init:{ics.state.terminated.reason}")

            pods_info.append({
                "name": pod.metadata.name,
                "status": pod.status.phase if pod.status else "Unknown",
                "ip": pod.status.pod_ip if pod.status else None,
                "node_name": pod.spec.node_name if pod.spec else "N/A",
                "creation_timestamp": (
                    pod.metadata.creation_timestamp.isoformat()
                    if pod.metadata.creation_timestamp
                    else "N/A"
                ),
                "restart_count": total_restart_count,
                "container_states": container_states,
            })

        logger.info(f"Found {len(pods_info)} pods in namespace '{namespace}'.")
        return pods_info

    except ApiException as e:
        logger.error(f"API error listing pods in namespace '{namespace}': {e}")
        return [{"error": f"API Error: {e.reason}", "namespace": namespace}]
    except Exception as e:
        logger.error(
            f"Unexpected error listing pods in namespace '{namespace}': {e}", exc_info=True
        )
        return [{"error": f"Unexpected Error: {str(e)}", "namespace": namespace}]


async def get_kubernetes_resource_impl(
    resource_type: str,
    name: str,
    namespace: str = "default",
    output_format: str = "summary",
    k8s_core_api: Any = None,
    k8s_apps_api: Any = None,
    k8s_batch_api: Any = None,
    k8s_custom_api: Any = None,
    k8s_storage_api: Any = None,
    k8s_autoscaling_api: Any = None,
) -> str:
    """Retrieve details about a Kubernetes / Tekton resource.

    Parameters
    ----------
    resource_type : str
        Case-insensitive resource kind (e.g. ``"pod"``, ``"pipelinerun"``).
    name : str
        Resource name.
    namespace : str
        Namespace (default ``"default"``).
    output_format : str
        ``"summary"``, ``"detailed"``, or ``"yaml"``.
    k8s_core_api, k8s_apps_api, ... : injected API clients (may be ``None``).
    """
    if not k8s_core_api:
        return "Error: Kubernetes client not available."

    try:
        resource_type = resource_type.lower().strip()
        resource_obj = None
        api_version = None

        if resource_type in _CORE_RESOURCES:
            method_name, api_version = _CORE_RESOURCES[resource_type]
            if resource_type in ('namespace', 'node', 'persistentvolume', 'pv'):
                method = getattr(k8s_core_api, f'read_{method_name[:-1]}')
                resource_obj = await asyncio.to_thread(method, name=name)
            elif resource_type == 'endpoints':
                resource_obj = await asyncio.to_thread(
                    k8s_core_api.read_namespaced_endpoints, name=name, namespace=namespace
                )
            else:
                method = getattr(k8s_core_api, f'read_namespaced_{method_name[:-1]}')
                resource_obj = await asyncio.to_thread(method, name=name, namespace=namespace)

        elif resource_type in _STORAGE_RESOURCES:
            resource_obj = await asyncio.to_thread(k8s_storage_api.read_storage_class, name=name)

        elif resource_type in _AUTOSCALING_RESOURCES:
            method_name, api_version = _AUTOSCALING_RESOURCES[resource_type]
            method = getattr(k8s_autoscaling_api, f'read_namespaced_{method_name[:-1]}')
            resource_obj = await asyncio.to_thread(method, name=name, namespace=namespace)

        elif resource_type in _APPS_RESOURCES:
            method_name, api_version = _APPS_RESOURCES[resource_type]
            method = getattr(k8s_apps_api, f'read_namespaced_{method_name[:-1]}')
            resource_obj = await asyncio.to_thread(method, name=name, namespace=namespace)

        elif resource_type in _BATCH_RESOURCES:
            method_name, _ = _BATCH_RESOURCES[resource_type]
            method = getattr(k8s_batch_api, f'read_namespaced_{method_name[:-1]}')
            resource_obj = await asyncio.to_thread(method, name=name, namespace=namespace)

        elif resource_type in _NETWORKING_RESOURCES:
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_namespaced_custom_object,
                group="networking.k8s.io",
                version="v1",
                namespace=namespace,
                plural="ingresses",
                name=name,
            )

        elif resource_type in _MONITORING_RESOURCES:
            method_name, api_version = _MONITORING_RESOURCES[resource_type]
            group, version = api_version.split('/')
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_namespaced_custom_object,
                group=group, version=version,
                namespace=namespace, plural=method_name, name=name,
            )

        elif resource_type in _ADMISSION_RESOURCES:
            method_name, api_version = _ADMISSION_RESOURCES[resource_type]
            group, version = api_version.split('/')
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_cluster_custom_object,
                group=group, version=version, plural=method_name, name=name,
            )

        elif resource_type in _TEKTON_RESOURCES:
            method_name, api_version = _TEKTON_RESOURCES[resource_type]
            group, version = api_version.split('/')
            if resource_type == 'clustertask':
                resource_obj = await asyncio.to_thread(
                    k8s_custom_api.get_cluster_custom_object,
                    group=group, version=version, plural=method_name, name=name,
                )
            else:
                resource_obj = await asyncio.to_thread(
                    k8s_custom_api.get_namespaced_custom_object,
                    group=group, version=version,
                    namespace=namespace, plural=method_name, name=name,
                )

        elif resource_type in _TEKTON_TRIGGERS_RESOURCES:
            method_name, api_version = _TEKTON_TRIGGERS_RESOURCES[resource_type]
            group, version = api_version.split('/')
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_namespaced_custom_object,
                group=group, version=version,
                namespace=namespace, plural=method_name, name=name,
            )

        elif resource_type in _KONFLUX_RESOURCES:
            method_name, api_version = _KONFLUX_RESOURCES[resource_type]
            group, version = api_version.split('/')
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_namespaced_custom_object,
                group=group, version=version,
                namespace=namespace, plural=method_name, name=name,
            )

        else:
            return (
                f"Error: Unsupported resource type '{resource_type}'. "
                f"Supported types: {', '.join(_all_supported_types())}"
            )

        if not resource_obj:
            return (
                f"Error: Resource '{name}' of type '{resource_type}' "
                f"not found in namespace '{namespace}'"
            )

        fmt = output_format.lower()
        if fmt == "yaml":
            return format_yaml_output(resource_obj, resource_type, name, namespace)
        elif fmt == "detailed":
            return format_detailed_output(resource_obj, resource_type, name, namespace)
        else:
            return format_summary_output(resource_obj, resource_type, name, namespace)

    except ApiException as e:
        if e.status == 404:
            return (
                f"Error: Resource '{name}' of type '{resource_type}' "
                f"not found in namespace '{namespace}'"
            )
        return f"Kubernetes API Error: {e.status} - {e.reason}"
    except Exception as e:
        return f"Error retrieving resource: {str(e)}"


async def check_resource_constraints_impl(
    namespace: str,
    k8s_core_api: Any,
) -> Dict[str, Any]:
    """Check for resource constraints in a namespace that may impact pipelines.

    Identifies: pending/unschedulable pods, OOMKilled containers,
    CrashLoopBackOff, ImagePullBackOff, high restart counts, and resource
    quota utilisation.
    """
    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}

    try:
        pods = await list_pods(namespace, k8s_core_api, logger)
        resource_quotas = await asyncio.to_thread(
            k8s_core_api.list_namespaced_resource_quota, namespace
        )

        resource_issues: List[Dict[str, Any]] = []
        pending_pods: List[Dict[str, Any]] = []
        oom_killed_pods: List[Dict[str, Any]] = []

        for pod in pods:
            pod_name = pod.get("name")
            pod_status = pod.get("status")

            if pod_status in ("Failed", "Pending", "Running"):
                detailed_pod = await asyncio.to_thread(
                    k8s_core_api.read_namespaced_pod, name=pod_name, namespace=namespace
                )

                if pod_status == "Pending" and detailed_pod.status and detailed_pod.status.conditions:
                    for condition in detailed_pod.status.conditions:
                        if condition.type == "PodScheduled" and condition.status == "False":
                            pending_pods.append({
                                "pod": pod_name,
                                "issue": "Unschedulable",
                                "reason": condition.reason or "Unknown",
                                "message": condition.message or "",
                            })
                            break
                    else:
                        pending_pods.append({
                            "pod": pod_name,
                            "issue": "Pending",
                            "reason": "Unknown",
                            "message": "Pod is pending without specific reason",
                        })
                elif pod_status == "Pending":
                    pending_pods.append({
                        "pod": pod_name,
                        "issue": "Pending",
                        "reason": "Unknown",
                        "message": "Pod is pending without specific reason",
                    })

                def _check_container_statuses(statuses: Any, prefix: str = "") -> None:
                    if not statuses:
                        return
                    for cs in statuses:
                        cname = f"{prefix}{cs.name}" if prefix else cs.name
                        if hasattr(cs, "state") and cs.state:
                            if cs.state.waiting:
                                reason = cs.state.waiting.reason
                                if reason in (
                                    "CrashLoopBackOff", "OOMKilled", "ImagePullBackOff",
                                    "ErrImagePull", "CreateContainerError",
                                    "CreateContainerConfigError", "ContainerCreating",
                                ):
                                    resource_issues.append({
                                        "pod": pod_name,
                                        "container": cname,
                                        "issue": reason,
                                        "message": cs.state.waiting.message or "",
                                    })
                        if hasattr(cs, "last_state") and cs.last_state:
                            if cs.last_state.terminated:
                                if cs.last_state.terminated.reason == "OOMKilled":
                                    oom_killed_pods.append({
                                        "pod": pod_name,
                                        "container": cname,
                                        "issue": "OOMKilled",
                                        "restart_count": cs.restart_count,
                                        "message": (
                                            f"Container was OOMKilled and restarted "
                                            f"{cs.restart_count} times"
                                        ),
                                    })
                        if cs.restart_count and cs.restart_count > 5:
                            resource_issues.append({
                                "pod": pod_name,
                                "container": cname,
                                "issue": "HighRestartCount",
                                "restart_count": cs.restart_count,
                                "message": f"Container has restarted {cs.restart_count} times",
                            })

                if detailed_pod.status:
                    _check_container_statuses(detailed_pod.status.container_statuses)
                    _check_container_statuses(
                        detailed_pod.status.init_container_statuses, prefix="init:"
                    )

        quota_data: List[Dict[str, Any]] = []
        for quota in resource_quotas.items:
            if quota.status.hard and quota.status.used:
                quota_item: Dict[str, Any] = {"name": quota.metadata.name, "resources": {}}
                for resource, hard_limit in quota.status.hard.items():
                    used = quota.status.used.get(resource, "0")
                    quota_item["resources"][resource] = {
                        "limit": hard_limit,
                        "used": used,
                        "utilization": calculate_utilization(used, hard_limit),
                    }
                quota_data.append(quota_item)

        high_utilization = [
            q for q in quota_data
            if any(r.get("utilization", 0) > 80 for r in q.get("resources", {}).values())
        ]

        status = "Healthy"
        summary_parts: List[str] = []

        if oom_killed_pods:
            status = "Critical"
            summary_parts.append(f"{len(oom_killed_pods)} OOMKilled containers")
        if pending_pods:
            status = "Critical" if status != "Critical" else status
            summary_parts.append(f"{len(pending_pods)} pending/unschedulable pods")
        if resource_issues:
            status = "Warning" if status == "Healthy" else status
            summary_parts.append(f"{len(resource_issues)} container issues")
        if high_utilization:
            status = "Warning" if status == "Healthy" else status
            summary_parts.append(f"{len(high_utilization)} quotas with high utilization")

        summary = (
            f"Found: {', '.join(summary_parts)}" if summary_parts
            else "No significant resource constraints detected"
        )

        recommendations: List[str] = []
        if oom_killed_pods:
            recommendations.append("Increase memory limits for OOMKilled containers")
            recommendations.append("Review application memory usage patterns")
        if pending_pods:
            if any(p.get("issue") == "Unschedulable" for p in pending_pods):
                recommendations.append(
                    "Check node resources - pods cannot be scheduled due to insufficient resources"
                )
            recommendations.append("Review pending pods and their resource requests")
        if resource_issues:
            if any(i.get("issue") == "CrashLoopBackOff" for i in resource_issues):
                recommendations.append(
                    "Investigate CrashLoopBackOff containers - check logs for errors"
                )
            if any(i.get("issue") in ("ImagePullBackOff", "ErrImagePull") for i in resource_issues):
                recommendations.append(
                    "Fix image pull issues - verify image names and registry access"
                )
            if any(
                i.get("issue") in ("CreateContainerError", "CreateContainerConfigError")
                for i in resource_issues
            ):
                recommendations.append(
                    "Fix container configuration errors - check secrets, configmaps, and volume mounts"
                )
            if any(i.get("issue") == "HighRestartCount" for i in resource_issues):
                recommendations.append("Investigate containers with high restart counts")
        if high_utilization:
            recommendations.append(
                "Monitor resource quota usage and consider increasing limits"
            )

        return {
            "status": status,
            "summary": summary,
            "resource_quotas": quota_data,
            "pending_pods_due_to_resources": pending_pods,
            "oom_killed_containers": oom_killed_pods,
            "container_issues": resource_issues,
            "high_utilization_quotas": high_utilization,
            "recommendations": recommendations,
        }

    except ApiException as e:
        logger.error(
            f"Kubernetes API error checking resource constraints in namespace {namespace}: {e}"
        )
        return {
            "status": "Error",
            "summary": f"Kubernetes API error: {str(e)}",
            "resource_quotas": [],
            "pending_pods_due_to_resources": [],
            "oom_killed_containers": [],
            "container_issues": [],
            "high_utilization_quotas": [],
            "recommendations": ["Check cluster connectivity and permissions"],
            "error": str(e),
        }
    except Exception as e:
        logger.error(
            f"Unexpected error checking resource constraints in namespace {namespace}: {e}"
        )
        return {
            "status": "Error",
            "summary": f"Unexpected error: {str(e)}",
            "resource_quotas": [],
            "pending_pods_due_to_resources": [],
            "oom_killed_containers": [],
            "container_issues": [],
            "high_utilization_quotas": [],
            "recommendations": ["Review logs for detailed error information"],
            "error": str(e),
        }
