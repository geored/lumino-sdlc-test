"""Topology tool implementations for LUMINO MCP Server.

Extracted from server-mcp.py as part of issue #58 (sub-task of #30).
"""

import asyncio
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import networkx as nx

from helpers.resource_topology import (
    analyze_owner_references,
    analyze_service_dependencies,
    analyze_volume_dependencies,
    calculate_dependency_weight,
    convert_to_graphviz,
    convert_to_mermaid,
    generate_node_id,
    get_multi_cluster_topology_clients,
    get_resource_metrics,
    handle_resource_fetch_error,
)


async def _process_namespace_topology(
    namespace: str,
    cluster_name: str,
    component_types: List[str],
    core_api,
    apps_api,
    custom_api,
    include_metrics: bool,
    skip_on_permission_denied: bool,
    logger,
) -> Dict[str, Any]:
    """Process a single namespace and return its topology data."""
    nodes = []
    edges = []
    permissions = {"accessible": [], "denied": [], "errors": []}
    stats = {"nodes": 0, "edges": 0}

    # Pre-fetch pods once to avoid N+1 queries in analyze_service_dependencies
    pods_list = None
    if "pods" in component_types or "services" in component_types:
        try:
            pods_result = await asyncio.to_thread(
                core_api.list_namespaced_pod, namespace=namespace
            )
            pods_list = pods_result.items
        except Exception as e:
            logger.debug(f"Could not pre-fetch pods for {namespace}: {e}")

    try:
        # Process Deployments
        if "deployments" in component_types:
            try:
                deployments = await asyncio.to_thread(
                    apps_api.list_namespaced_deployment, namespace=namespace
                )
                permissions["accessible"].append(
                    f"{cluster_name}/{namespace}/deployments"
                )

                for deployment in deployments.items:
                    node_id = generate_node_id(
                        cluster_name, namespace, "deployment", deployment.metadata.name
                    )

                    node = {
                        "id": node_id,
                        "type": "deployment",
                        "name": deployment.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": (
                            deployment.status.conditions[-1].type
                            if deployment.status.conditions
                            else "Unknown"
                        ),
                        "metadata": {
                            "replicas": deployment.spec.replicas or 0,
                            "ready_replicas": deployment.status.ready_replicas or 0,
                            "labels": deployment.metadata.labels or {},
                        },
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(
                            cluster_name,
                            "deployment",
                            namespace,
                            deployment.metadata.name,
                            logger,
                        )

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Analyze dependencies
                    deployment_dict = deployment.to_dict()
                    owner_edges = await analyze_owner_references(
                        deployment_dict, cluster_name, "deployment"
                    )
                    volume_edges = await analyze_volume_dependencies(
                        deployment_dict, cluster_name, "deployment", logger
                    )
                    edges.extend(owner_edges + volume_edges)
                    stats["edges"] += len(owner_edges + volume_edges)

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "deployments", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(
                        f"{cluster_name}/{namespace}/deployments"
                    )
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/deployments",
                            "error": error_info["error_message"],
                        }
                    )

        # Process ReplicaSets (needed for complete Deployment->ReplicaSet->Pod ownership chain)
        if "replicasets" in component_types:
            try:
                replicasets = await asyncio.to_thread(
                    apps_api.list_namespaced_replica_set, namespace=namespace
                )
                permissions["accessible"].append(
                    f"{cluster_name}/{namespace}/replicasets"
                )

                for replicaset in replicasets.items:
                    node_id = generate_node_id(
                        cluster_name, namespace, "replicaset", replicaset.metadata.name
                    )

                    node = {
                        "id": node_id,
                        "type": "replicaset",
                        "name": replicaset.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": (
                            "Active"
                            if (replicaset.status.ready_replicas or 0) > 0
                            else "Inactive"
                        ),
                        "metadata": {
                            "replicas": replicaset.spec.replicas or 0,
                            "ready_replicas": replicaset.status.ready_replicas or 0,
                            "labels": replicaset.metadata.labels or {},
                        },
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(
                            cluster_name,
                            "replicaset",
                            namespace,
                            replicaset.metadata.name,
                            logger,
                        )

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Analyze dependencies (ReplicaSet->Deployment ownership)
                    replicaset_dict = replicaset.to_dict()
                    owner_edges = await analyze_owner_references(
                        replicaset_dict, cluster_name, "replicaset"
                    )
                    edges.extend(owner_edges)
                    stats["edges"] += len(owner_edges)

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "replicasets", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(
                        f"{cluster_name}/{namespace}/replicasets"
                    )
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/replicasets",
                            "error": error_info["error_message"],
                        }
                    )

        # Process Services
        if "services" in component_types:
            try:
                services = await asyncio.to_thread(
                    core_api.list_namespaced_service, namespace=namespace
                )
                permissions["accessible"].append(f"{cluster_name}/{namespace}/services")

                for service in services.items:
                    node_id = generate_node_id(
                        cluster_name, namespace, "service", service.metadata.name
                    )

                    node = {
                        "id": node_id,
                        "type": "service",
                        "name": service.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "type": service.spec.type,
                            "cluster_ip": service.spec.cluster_ip,
                            "ports": [
                                {"port": p.port, "target_port": p.target_port}
                                for p in (service.spec.ports or [])
                            ],
                            "selector": service.spec.selector or {},
                        },
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(
                            cluster_name,
                            "service",
                            namespace,
                            service.metadata.name,
                            logger,
                        )

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Analyze service dependencies (pass pre-fetched pods to avoid N+1 queries)
                    service_dict = service.to_dict()
                    service_edges = await analyze_service_dependencies(
                        service_dict,
                        cluster_name,
                        core_api,
                        logger,
                        pods_list=pods_list,
                    )
                    edges.extend(service_edges)
                    stats["edges"] += len(service_edges)

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "services", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/services")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/services",
                            "error": error_info["error_message"],
                        }
                    )

        # Process Pods (use pre-fetched pods_list if available)
        if "pods" in component_types:
            try:
                if pods_list is not None:
                    pods_items = pods_list
                else:
                    pods_result = await asyncio.to_thread(
                        core_api.list_namespaced_pod, namespace=namespace
                    )
                    pods_items = pods_result.items
                for pod in pods_items:
                    node_id = generate_node_id(
                        cluster_name, namespace, "pod", pod.metadata.name
                    )

                    node = {
                        "id": node_id,
                        "type": "pod",
                        "name": pod.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": pod.status.phase or "Unknown",
                        "metadata": {
                            "node_name": pod.spec.node_name,
                            "labels": pod.metadata.labels or {},
                            "containers": len(pod.spec.containers or []),
                        },
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(
                            cluster_name, "pod", namespace, pod.metadata.name, logger
                        )

                    nodes.append(node)
                    stats["nodes"] += 1

                    pod_dict = pod.to_dict()
                    owner_edges = await analyze_owner_references(
                        pod_dict, cluster_name, "pod"
                    )
                    volume_edges = await analyze_volume_dependencies(
                        pod_dict, cluster_name, "pod", logger
                    )
                    edges.extend(owner_edges + volume_edges)
                    stats["edges"] += len(owner_edges + volume_edges)

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "pods", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/pods")
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/pods",
                            "error": error_info["error_message"],
                        }
                    )

        # Process PVCs
        if "persistentvolumeclaims" in component_types:
            try:
                pvcs = await asyncio.to_thread(
                    core_api.list_namespaced_persistent_volume_claim,
                    namespace=namespace,
                )
                for pvc in pvcs.items:
                    node_id = generate_node_id(
                        cluster_name,
                        namespace,
                        "persistentvolumeclaim",
                        pvc.metadata.name,
                    )

                    node = {
                        "id": node_id,
                        "type": "persistentvolumeclaim",
                        "name": pvc.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": pvc.status.phase or "Unknown",
                        "metadata": {
                            "capacity": (
                                pvc.status.capacity.get("storage")
                                if pvc.status.capacity
                                else None
                            ),
                            "access_modes": pvc.spec.access_modes or [],
                            "storage_class": pvc.spec.storage_class_name,
                        },
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(
                            cluster_name,
                            "persistentvolumeclaim",
                            namespace,
                            pvc.metadata.name,
                            logger,
                        )

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e,
                    "persistentvolumeclaims",
                    namespace,
                    skip_on_permission_denied,
                    logger,
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(
                        f"{cluster_name}/{namespace}/persistentvolumeclaims"
                    )
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/persistentvolumeclaims",
                            "error": error_info["error_message"],
                        }
                    )

        # Process ConfigMaps
        if "configmaps" in component_types:
            try:
                configmaps = await asyncio.to_thread(
                    core_api.list_namespaced_config_map, namespace=namespace
                )
                permissions["accessible"].append(
                    f"{cluster_name}/{namespace}/configmaps"
                )

                for cm in configmaps.items:
                    node_id = generate_node_id(
                        cluster_name, namespace, "configmap", cm.metadata.name
                    )

                    node = {
                        "id": node_id,
                        "type": "configmap",
                        "name": cm.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "data_keys": list(cm.data.keys()) if cm.data else []
                        },
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "configmaps", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(
                        f"{cluster_name}/{namespace}/configmaps"
                    )
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/configmaps",
                            "error": error_info["error_message"],
                        }
                    )

        # Process Secrets (NOT included in defaults due to common RBAC restrictions)
        if "secrets" in component_types:
            try:
                secrets = await asyncio.to_thread(
                    core_api.list_namespaced_secret, namespace=namespace
                )
                permissions["accessible"].append(f"{cluster_name}/{namespace}/secrets")

                for secret in secrets.items:
                    node_id = generate_node_id(
                        cluster_name, namespace, "secret", secret.metadata.name
                    )

                    node = {
                        "id": node_id,
                        "type": "secret",
                        "name": secret.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "type": secret.type,
                            "data_keys": (
                                list(secret.data.keys()) if secret.data else []
                            ),
                        },
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "secrets", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/secrets")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/secrets",
                            "error": error_info["error_message"],
                        }
                    )

        # Process Tekton PipelineRuns
        if "pipelineruns" in component_types:
            try:
                pipeline_runs = await asyncio.to_thread(
                    custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=namespace,
                    plural="pipelineruns",
                    limit=200,
                )

                for pr in pipeline_runs.get("items", []):
                    node_id = generate_node_id(
                        cluster_name,
                        namespace,
                        "pipelinerun",
                        pr.get("metadata", {}).get("name", ""),
                    )

                    node = {
                        "id": node_id,
                        "type": "pipelinerun",
                        "name": pr.get("metadata", {}).get("name", ""),
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": pr.get("status", {})
                        .get("conditions", [{}])[-1]
                        .get("type", "Unknown"),
                        "metadata": {
                            "pipeline_ref": pr.get("spec", {})
                            .get("pipelineRef", {})
                            .get("name", ""),
                            "labels": pr.get("metadata", {}).get("labels", {}),
                        },
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(
                            cluster_name, "pipelinerun", namespace, node["name"], logger
                        )

                    nodes.append(node)
                    stats["nodes"] += 1

                    pipeline_ref = pr.get("spec", {}).get("pipelineRef", {}).get("name")
                    if pipeline_ref:
                        pipeline_id = generate_node_id(
                            cluster_name, namespace, "pipeline", pipeline_ref
                        )
                        edges.append(
                            {
                                "source": node_id,
                                "target": pipeline_id,
                                "relationship": "runs",
                                "weight": calculate_dependency_weight(
                                    "pipelinerun", "pipeline", "runs"
                                ),
                            }
                        )
                        stats["edges"] += 1

            except Exception as e:
                logger.debug(f"Could not fetch PipelineRuns in {namespace}: {e}")

        # Process Tekton Pipelines
        if "pipelines" in component_types:
            try:
                pipelines = await asyncio.to_thread(
                    custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=namespace,
                    plural="pipelines",
                )

                for pipeline in pipelines.get("items", []):
                    node_id = generate_node_id(
                        cluster_name,
                        namespace,
                        "pipeline",
                        pipeline.get("metadata", {}).get("name", ""),
                    )

                    node = {
                        "id": node_id,
                        "type": "pipeline",
                        "name": pipeline.get("metadata", {}).get("name", ""),
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "tasks": len(pipeline.get("spec", {}).get("tasks", [])),
                            "labels": pipeline.get("metadata", {}).get("labels", {}),
                        },
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                logger.debug(f"Could not fetch Pipelines in {namespace}: {e}")

        # Process Tekton TaskRuns
        if "taskruns" in component_types:
            try:
                task_runs = await asyncio.to_thread(
                    custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=namespace,
                    plural="taskruns",
                    limit=500,
                )
                permissions["accessible"].append(f"{cluster_name}/{namespace}/taskruns")

                for tr in task_runs.get("items", []):
                    tr_name = tr.get("metadata", {}).get("name", "")
                    node_id = generate_node_id(
                        cluster_name, namespace, "taskrun", tr_name
                    )

                    conditions = tr.get("status", {}).get("conditions", [])
                    status = (
                        conditions[-1].get("reason", "Unknown")
                        if conditions
                        else "Unknown"
                    )

                    node = {
                        "id": node_id,
                        "type": "taskrun",
                        "name": tr_name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": status,
                        "metadata": {
                            "task_ref": tr.get("spec", {})
                            .get("taskRef", {})
                            .get("name", ""),
                            "pipeline_run": tr.get("metadata", {})
                            .get("labels", {})
                            .get("tekton.dev/pipelineRun", ""),
                            "labels": tr.get("metadata", {}).get("labels", {}),
                            "start_time": tr.get("status", {}).get("startTime"),
                        },
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

                    pipeline_run_name = (
                        tr.get("metadata", {})
                        .get("labels", {})
                        .get("tekton.dev/pipelineRun")
                    )
                    if pipeline_run_name:
                        pr_id = generate_node_id(
                            cluster_name, namespace, "pipelinerun", pipeline_run_name
                        )
                        edges.append(
                            {
                                "source": pr_id,
                                "target": node_id,
                                "relationship": "runs_task",
                                "weight": 0.85,
                            }
                        )
                        stats["edges"] += 1

                    task_ref = tr.get("spec", {}).get("taskRef", {}).get("name")
                    if task_ref:
                        task_id = generate_node_id(
                            cluster_name, namespace, "task", task_ref
                        )
                        edges.append(
                            {
                                "source": node_id,
                                "target": task_id,
                                "relationship": "uses",
                                "weight": calculate_dependency_weight(
                                    "taskrun", "task", "uses"
                                ),
                            }
                        )
                        stats["edges"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "taskruns", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/taskruns")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/taskruns",
                            "error": error_info["error_message"],
                        }
                    )

        # Process Tekton Tasks
        if "tasks" in component_types:
            try:
                tasks = await asyncio.to_thread(
                    custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=namespace,
                    plural="tasks",
                )
                permissions["accessible"].append(f"{cluster_name}/{namespace}/tasks")

                for task in tasks.get("items", []):
                    task_name = task.get("metadata", {}).get("name", "")
                    node_id = generate_node_id(
                        cluster_name, namespace, "task", task_name
                    )

                    node = {
                        "id": node_id,
                        "type": "task",
                        "name": task_name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "steps": len(task.get("spec", {}).get("steps", [])),
                            "labels": task.get("metadata", {}).get("labels", {}),
                        },
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(
                    e, "tasks", namespace, skip_on_permission_denied, logger
                )
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/tasks")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append(
                        {
                            "resource": f"{cluster_name}/{namespace}/tasks",
                            "error": error_info["error_message"],
                        }
                    )

    except Exception as e:
        logger.warning(
            f"Error processing namespace {namespace} in cluster {cluster_name}: {e}"
        )

    return {"nodes": nodes, "edges": edges, "permissions": permissions, "stats": stats}


async def live_system_topology_mapper_impl(
    cluster_names: Optional[List[str]],
    component_types: Optional[List[str]],
    namespace_filter: Optional[str],
    depth_limit: Optional[int],
    include_metrics: Optional[bool],
    output_format: Optional[str],
    skip_on_permission_denied: Optional[bool],
    k8s_core_api,
    k8s_custom_api,
    k8s_apps_api,
    k8s_storage_api,
    k8s_batch_api,
    logger,
) -> Dict[str, Any]:
    """
    Implementation of live_system_topology_mapper.

    Generate real-time dependency graph of Kubernetes/Tekton components and their interconnections.
    Maps Services, Deployments, Pipelines, PVCs, and their relationships via ownerReferences and selectors.
    """
    if (
        not k8s_core_api
        or not k8s_custom_api
        or not k8s_apps_api
        or not k8s_storage_api
        or not k8s_batch_api
    ):
        return {"error": "Kubernetes client not available."}
    try:
        logger.info(
            f"Starting live system topology mapping with filters: clusters={cluster_names}, "
            f"types={component_types}, namespace_filter={namespace_filter}"
        )

        start_time = time.time()

        cluster_clients = await get_multi_cluster_topology_clients(
            k8s_core_api, k8s_custom_api, k8s_apps_api, k8s_storage_api, k8s_batch_api
        )

        if not cluster_clients:
            return {
                "topology": {"nodes": [], "edges": []},
                "summary": {
                    "total_nodes": 0,
                    "total_relationships": 0,
                    "clusters_mapped": 0,
                    "potential_blast_radius": {},
                },
                "error": "No cluster clients available for topology mapping",
                "last_updated": datetime.now().isoformat(),
            }

        if cluster_names:
            cluster_clients = {
                k: v for k, v in cluster_clients.items() if k in cluster_names
            }

        if not component_types:
            component_types = [
                "deployments",
                "replicasets",
                "services",
                "pods",
                "persistentvolumeclaims",
                "configmaps",
                "pipelineruns",
                "pipelines",
                "taskruns",
                "tasks",
            ]

        nodes = []
        edges = []
        cluster_stats = {}
        permissions_report = {"accessible": [], "denied": [], "errors": []}

        for cluster_name, clients in cluster_clients.items():
            logger.info(f"Mapping topology for cluster: {cluster_name}")
            cluster_stats[cluster_name] = {"nodes": 0, "edges": 0}

            try:
                core_api = clients["core_api"]
                apps_api = clients["apps_api"]
                custom_api = clients["custom_api"]
                clients["storage_api"]

                all_namespaces = []
                try:
                    ns_list = await asyncio.to_thread(core_api.list_namespace)
                    all_namespaces = [ns.metadata.name for ns in ns_list.items]

                    if namespace_filter:
                        pattern = re.compile(namespace_filter)
                        all_namespaces = [
                            ns for ns in all_namespaces if pattern.search(ns)
                        ]

                except Exception as e:
                    logger.warning(
                        f"Failed to list namespaces in cluster {cluster_name}: {e}"
                    )
                    continue

                logger.info(
                    f"Processing {len(all_namespaces)} namespaces in cluster {cluster_name} in parallel"
                )

                namespace_tasks = [
                    _process_namespace_topology(
                        namespace=ns,
                        cluster_name=cluster_name,
                        component_types=component_types,
                        core_api=core_api,
                        apps_api=apps_api,
                        custom_api=custom_api,
                        include_metrics=include_metrics,
                        skip_on_permission_denied=skip_on_permission_denied,
                        logger=logger,
                    )
                    for ns in all_namespaces
                ]

                namespace_results = await asyncio.gather(
                    *namespace_tasks, return_exceptions=True
                )

                for i, result in enumerate(namespace_results):
                    if isinstance(result, Exception):
                        logger.warning(
                            f"Error processing namespace {all_namespaces[i]} in cluster {cluster_name}: {result}"
                        )
                        continue

                    nodes.extend(result["nodes"])
                    edges.extend(result["edges"])
                    cluster_stats[cluster_name]["nodes"] += result["stats"]["nodes"]
                    cluster_stats[cluster_name]["edges"] += result["stats"]["edges"]
                    permissions_report["accessible"].extend(
                        result["permissions"]["accessible"]
                    )
                    permissions_report["denied"].extend(result["permissions"]["denied"])
                    permissions_report["errors"].extend(result["permissions"]["errors"])

            except Exception as e:
                logger.error(f"Error processing cluster {cluster_name}: {e}")
                continue

        total_nodes = len(nodes)
        total_edges = len(edges)
        clusters_mapped = len([c for c in cluster_stats.values() if c["nodes"] > 0])

        blast_radius = {}
        if total_nodes > 0:
            G = nx.DiGraph()
            for node in nodes:
                G.add_node(node["id"], **node)
            for edge in edges:
                G.add_edge(edge["source"], edge["target"], **edge)

            if G.nodes():
                max_reachable = 0
                critical_nodes_list = []

                for node_id in G.nodes():
                    reachable = set()
                    queue = [(node_id, 0)]
                    visited = {node_id}

                    while queue:
                        current, current_depth = queue.pop(0)
                        if current_depth >= depth_limit:
                            continue
                        for neighbor in G.neighbors(current):
                            if neighbor not in visited:
                                visited.add(neighbor)
                                reachable.add(neighbor)
                                queue.append((neighbor, current_depth + 1))

                    if len(reachable) > max_reachable:
                        max_reachable = len(reachable)

                    if len(reachable) > 5:
                        critical_nodes_list.append(
                            {"node_id": node_id, "affected_count": len(reachable)}
                        )

                blast_radius = {
                    "depth_limit_used": depth_limit,
                    "most_connected_components": len(
                        list(nx.connected_components(G.to_undirected()))
                    ),
                    "average_degree": (
                        sum(dict(G.degree()).values()) / len(G.nodes())
                        if G.nodes()
                        else 0
                    ),
                    "critical_nodes": len(critical_nodes_list),
                    "max_blast_radius": max_reachable,
                    "critical_nodes_details": critical_nodes_list[:10],
                }

        execution_time = time.time() - start_time

        permissions_report["accessible"] = list(set(permissions_report["accessible"]))
        permissions_report["denied"] = list(set(permissions_report["denied"]))

        result = {
            "topology": {"nodes": nodes, "edges": edges},
            "summary": {
                "total_nodes": total_nodes,
                "total_relationships": total_edges,
                "clusters_mapped": clusters_mapped,
                "potential_blast_radius": blast_radius,
                "cluster_stats": cluster_stats,
                "execution_time_seconds": round(execution_time, 2),
            },
            "permissions": permissions_report,
            "last_updated": datetime.now().isoformat(),
        }

        if permissions_report["denied"]:
            logger.warning(
                f"Permission denied for {len(permissions_report['denied'])} resource types"
            )
        if permissions_report["errors"]:
            logger.warning(
                f"Errors encountered for {len(permissions_report['errors'])} resource types"
            )

        logger.info(
            f"Topology mapping completed: {total_nodes} nodes, {total_edges} edges across {clusters_mapped} clusters in {execution_time:.2f}s"
        )

        if output_format == "graphviz":
            result["graphviz"] = convert_to_graphviz(nodes, edges)
        elif output_format == "mermaid":
            result["mermaid"] = convert_to_mermaid(nodes, edges)

        return result

    except Exception as e:
        logger.error(
            f"Unexpected error during topology mapping: {str(e)}", exc_info=True
        )
        return {
            "topology": {"nodes": [], "edges": []},
            "summary": {
                "total_nodes": 0,
                "total_relationships": 0,
                "clusters_mapped": 0,
                "potential_blast_radius": {},
            },
            "error": f"Failed to generate topology: {str(e)}",
            "last_updated": datetime.now().isoformat(),
        }
