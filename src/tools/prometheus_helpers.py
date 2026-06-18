"""
LUMINO MCP Server - Prometheus Endpoint Discovery Helpers

This module contains the helper chain for discovering Prometheus and Thanos
Query endpoints from various sources (env vars, predefined config, OpenShift
Routes, Prometheus Operator CRDs, and Kubernetes services).

All discovery functions accept the Kubernetes API clients as parameters to
avoid module-level globals and make testing straightforward.
"""

import asyncio
import logging
from typing import Optional, Tuple

from kubernetes import client

from helpers.config import (OPENSHIFT_PROMETHEUS_ENDPOINTS,
                            _prometheus_endpoint_cache, get_prometheus_url,
                            get_thanos_url, is_running_in_cluster)

logger = logging.getLogger(__name__)


async def _discover_prometheus_via_routes(
    k8s_custom_api: Optional[client.CustomObjectsApi] = None,
) -> Optional[str]:
    """
    Discover Prometheus endpoint via OpenShift Routes.

    Looks for routes in openshift-monitoring namespace:
    - prometheus-k8s (primary Prometheus)
    - thanos-querier (Thanos frontend)

    Args:
        k8s_custom_api: Kubernetes CustomObjectsApi client (may be None).

    Returns:
        Endpoint URL string, or None if not found.
    """
    if not k8s_custom_api:
        logger.debug("CustomObjectsApi not available for route discovery")
        return None

    try:
        routes = await asyncio.to_thread(
            k8s_custom_api.list_namespaced_custom_object,
            group="route.openshift.io",
            version="v1",
            namespace="openshift-monitoring",
            plural="routes",
        )

        # Priority order: prefer Thanos (unified, deduplicated view) over direct Prometheus
        preferred_routes = ["thanos-querier", "prometheus-k8s"]

        route_items = routes.get("items", [])
        route_map = {r.get("metadata", {}).get("name"): r for r in route_items}

        for route_name in preferred_routes:
            if route_name in route_map:
                route = route_map[route_name]
                spec = route.get("spec", {})
                host = spec.get("host")
                if host:
                    tls = spec.get("tls")
                    protocol = "https" if tls else "http"
                    endpoint = f"{protocol}://{host}"
                    logger.info(
                        f"Discovered Prometheus via OpenShift route '{route_name}': {endpoint}"
                    )
                    return endpoint

        # Fallback: any route with 'prometheus' in the name
        for route in route_items:
            route_name = route.get("metadata", {}).get("name", "")
            if "prometheus" in route_name.lower():
                host = route.get("spec", {}).get("host")
                if host:
                    tls = route.get("spec", {}).get("tls")
                    protocol = "https" if tls else "http"
                    endpoint = f"{protocol}://{host}"
                    logger.info(
                        f"Discovered Prometheus via route '{route_name}': {endpoint}"
                    )
                    return endpoint

    except client.rest.ApiException as e:
        if e.status == 404:
            logger.debug(
                "OpenShift routes API not available (not an OpenShift cluster)"
            )
        else:
            logger.warning(f"Error querying OpenShift routes: {e}")
    except Exception as e:
        logger.warning(f"Error discovering Prometheus via routes: {e}")

    return None


async def _discover_prometheus_via_operator_crd(
    k8s_custom_api: Optional[client.CustomObjectsApi] = None,
    k8s_core_api: Optional[client.CoreV1Api] = None,
) -> Optional[str]:
    """
    Discover Prometheus via Prometheus Operator CRDs.

    Looks for:
    - Prometheus custom resources (monitoring.coreos.com/v1)
    - Associated services

    Args:
        k8s_custom_api: Kubernetes CustomObjectsApi client (may be None).
        k8s_core_api:   Kubernetes CoreV1Api client (may be None).

    Returns:
        Endpoint URL string, or None if not found.
    """
    if not k8s_custom_api or not k8s_core_api:
        logger.debug("API clients not available for Prometheus Operator CRD discovery")
        return None

    try:
        prometheus_resources = await asyncio.to_thread(
            k8s_custom_api.list_cluster_custom_object,
            group="monitoring.coreos.com",
            version="v1",
            plural="prometheuses",
        )

        for prom in prometheus_resources.get("items", []):
            metadata = prom.get("metadata", {})
            name = metadata.get("name")
            namespace = metadata.get("namespace")

            if not name or not namespace:
                continue

            service_name = f"prometheus-{name}"

            try:
                service = await asyncio.to_thread(
                    k8s_core_api.read_namespaced_service,
                    name=service_name,
                    namespace=namespace,
                )

                ports = service.spec.ports or []
                port = 9090
                for p in ports:
                    if p.name in ["web", "http", "prometheus"] or p.port == 9090:
                        port = p.port
                        break

                endpoint = f"http://{service_name}.{namespace}.svc.cluster.local:{port}"
                logger.info(f"Discovered Prometheus via Operator CRD: {endpoint}")
                return endpoint

            except client.rest.ApiException as e:
                logger.debug(f"Could not find service for Prometheus '{name}': {e}")
                continue

    except client.rest.ApiException as e:
        if e.status == 404:
            logger.debug("Prometheus Operator CRDs not available")
        else:
            logger.warning(f"Error querying Prometheus CRDs: {e}")
    except Exception as e:
        logger.warning(f"Error discovering Prometheus via Operator CRD: {e}")

    return None


async def _discover_prometheus_via_services(
    k8s_core_api: Optional[client.CoreV1Api] = None,
) -> Optional[str]:
    """
    Discover Prometheus by searching for services with prometheus-related labels/names.

    Search criteria:
    - Services with 'prometheus' in name
    - Services with label 'app=prometheus' or 'app.kubernetes.io/name=prometheus'
    - Services exposing port 9090

    Args:
        k8s_core_api: Kubernetes CoreV1Api client (may be None).

    Returns:
        Endpoint URL string, or None if not found.
    """
    if not k8s_core_api:
        logger.debug("CoreV1Api not available for service discovery")
        return None

    try:
        monitoring_namespaces = [
            "openshift-monitoring",
            "monitoring",
            "prometheus",
            "kube-prometheus",
            "observability",
        ]

        priority_names = ["prometheus-server", "prometheus-k8s", "prometheus"]
        excluded_suffixes = [
            "-alertmanager",
            "-pushgateway",
            "-node-exporter",
            "-kube-state-metrics",
            "-headless",
            "-operated",
        ]

        for namespace in monitoring_namespaces:
            try:
                services = await asyncio.to_thread(
                    k8s_core_api.list_namespaced_service, namespace=namespace
                )

                # First pass: look for priority names
                for priority_name in priority_names:
                    for service in services.items:
                        name = service.metadata.name
                        if name == priority_name:
                            ports = service.spec.ports or []
                            port = 9090
                            for p in ports:
                                if p.port in [9090, 80, 443] or (
                                    p.name and p.name in ["web", "http", "https"]
                                ):
                                    port = p.port
                                    break
                            endpoint = (
                                f"http://{name}.{namespace}.svc.cluster.local:{port}"
                            )
                            logger.info(
                                f"Discovered Prometheus service (priority match): {endpoint}"
                            )
                            return endpoint

                # Second pass: services with 'prometheus' (excluding non-server services)
                for service in services.items:
                    name = service.metadata.name
                    if "prometheus" in name.lower():
                        if any(
                            name.lower().endswith(suffix)
                            for suffix in excluded_suffixes
                        ):
                            continue
                        ports = service.spec.ports or []
                        port = 9090
                        for p in ports:
                            if p.port in [9090, 80, 443] or (
                                p.name and p.name in ["web", "http", "https"]
                            ):
                                port = p.port
                                break
                        endpoint = f"http://{name}.{namespace}.svc.cluster.local:{port}"
                        logger.info(f"Discovered Prometheus service: {endpoint}")
                        return endpoint

            except client.rest.ApiException as e:
                if e.status != 404:
                    logger.debug(f"Namespace '{namespace}' not accessible: {e}")
                continue

        # Cluster-wide label selectors
        label_selectors = [
            "app=prometheus",
            "app.kubernetes.io/name=prometheus",
            "app.kubernetes.io/component=prometheus",
        ]

        for label_selector in label_selectors:
            try:
                services = await asyncio.to_thread(
                    k8s_core_api.list_service_for_all_namespaces,
                    label_selector=label_selector,
                )
                if services.items:
                    service = services.items[0]
                    name = service.metadata.name
                    namespace = service.metadata.namespace
                    ports = service.spec.ports or []
                    port = 9090
                    for p in ports:
                        if p.port == 9090 or (p.name and p.name in ["web", "http"]):
                            port = p.port
                            break
                    endpoint = f"http://{name}.{namespace}.svc.cluster.local:{port}"
                    logger.info(
                        f"Discovered Prometheus via label selector '{label_selector}': {endpoint}"
                    )
                    return endpoint

            except client.rest.ApiException as e:
                logger.debug(f"Error with label selector '{label_selector}': {e}")
                continue

    except Exception as e:
        logger.warning(f"Error discovering Prometheus via services: {e}")

    return None


async def _discover_thanos_via_services(
    k8s_core_api: Optional[client.CoreV1Api] = None,
) -> Optional[str]:
    """
    Discover Thanos Query endpoint by searching for Thanos services.

    Thanos Query implements the Prometheus HTTP API, so once discovered
    it can be used interchangeably with Prometheus for PromQL queries.

    Search criteria:
    - Services with 'thanos-query' or 'thanos-querier' in name
    - Services with Thanos-related labels
    - Common Thanos Query ports: 9090, 10902 (HTTP), 9091

    Args:
        k8s_core_api: Kubernetes CoreV1Api client (may be None).

    Returns:
        Endpoint URL string, or None if not found.
    """
    if not k8s_core_api:
        logger.debug("CoreV1Api not available for Thanos service discovery")
        return None

    try:
        monitoring_namespaces = [
            "openshift-monitoring",
            "monitoring",
            "thanos",
            "observability",
            "kube-prometheus",
        ]

        priority_names = ["thanos-query-frontend", "thanos-querier", "thanos-query"]
        thanos_http_ports = [9090, 9091, 80, 443]

        # First pass: known monitoring namespaces, priority service names
        for namespace in monitoring_namespaces:
            try:
                services = await asyncio.to_thread(
                    k8s_core_api.list_namespaced_service, namespace=namespace
                )

                for priority_name in priority_names:
                    for service in services.items:
                        if service.metadata.name == priority_name:
                            ports = service.spec.ports or []
                            port = 9090
                            for p in ports:
                                if p.port in thanos_http_ports or (
                                    p.name and p.name in ["http", "web", "https"]
                                ):
                                    port = p.port
                                    break
                            endpoint = f"http://{priority_name}.{namespace}.svc.cluster.local:{port}"
                            logger.info(
                                f"Discovered Thanos Query service (priority match): {endpoint}"
                            )
                            return endpoint

                # Second pass: any service with 'thanos' and 'query' in the name
                for service in services.items:
                    sname = service.metadata.name.lower()
                    if "thanos" in sname and ("query" in sname or "querier" in sname):
                        ports = service.spec.ports or []
                        port = 9090
                        for p in ports:
                            if p.port in thanos_http_ports or (
                                p.name and p.name in ["http", "web", "https"]
                            ):
                                port = p.port
                                break
                        endpoint = f"http://{service.metadata.name}.{namespace}.svc.cluster.local:{port}"
                        logger.info(f"Discovered Thanos Query service: {endpoint}")
                        return endpoint

            except client.rest.ApiException as e:
                if e.status != 404:
                    logger.debug(
                        f"Namespace '{namespace}' not accessible for Thanos discovery: {e}"
                    )
                continue

        # Cluster-wide label-based search
        label_selectors = [
            "app.kubernetes.io/name=thanos-query",
            "app.kubernetes.io/component=query,app.kubernetes.io/name=thanos",
            "app=thanos-query",
            "app=thanos-querier",
        ]

        for label_selector in label_selectors:
            try:
                services = await asyncio.to_thread(
                    k8s_core_api.list_service_for_all_namespaces,
                    label_selector=label_selector,
                )
                if services.items:
                    service = services.items[0]
                    name = service.metadata.name
                    namespace = service.metadata.namespace
                    ports = service.spec.ports or []
                    port = 9090
                    for p in ports:
                        if p.port in thanos_http_ports or (
                            p.name and p.name in ["http", "web"]
                        ):
                            port = p.port
                            break
                    endpoint = f"http://{name}.{namespace}.svc.cluster.local:{port}"
                    logger.info(
                        f"Discovered Thanos Query via label selector '{label_selector}': {endpoint}"
                    )
                    return endpoint

            except client.rest.ApiException as e:
                logger.debug(
                    f"Error with Thanos label selector '{label_selector}': {e}"
                )
                continue

    except Exception as e:
        logger.warning(f"Error discovering Thanos Query via services: {e}")

    return None


async def discover_prometheus_endpoint(
    cluster_override: Optional[str] = None,
    k8s_core_api: Optional[client.CoreV1Api] = None,
    k8s_custom_api: Optional[client.CustomObjectsApi] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Discover Prometheus or Thanos Query endpoint using multiple strategies.

    Returns a (url, endpoint_type) tuple where endpoint_type is "thanos" or
    "prometheus".  Thanos Query is preferred when available since it provides a
    unified, deduplicated view.

    Priority order:
    0. THANOS_URL env var (explicit Thanos override)
    1. PROMETHEUS_URL env var (explicit Prometheus override)
    2. Predefined cluster endpoints
    3. Cache
    4. Auto-discovery (Thanos first, then Prometheus)
    5. Predefined fallback endpoints

    Args:
        cluster_override: Optional cluster name for predefined endpoint lookup.
        k8s_core_api:     Kubernetes CoreV1Api client (may be None).
        k8s_custom_api:   Kubernetes CustomObjectsApi client (may be None).

    Returns:
        (endpoint_url, endpoint_type) tuple, or (None, None) if not found.
    """
    # 0. THANOS_URL env var (highest priority)
    env_thanos_url = get_thanos_url()
    if env_thanos_url:
        logger.info(
            f"Using Thanos endpoint from THANOS_URL environment variable: {env_thanos_url}"
        )
        return (env_thanos_url, "thanos")

    # 1. PROMETHEUS_URL env var
    env_prometheus_url = get_prometheus_url()
    if env_prometheus_url:
        logger.info(
            f"Using Prometheus endpoint from PROMETHEUS_URL environment variable: {env_prometheus_url}"
        )
        return (env_prometheus_url, "prometheus")

    # 2. Predefined cluster endpoints
    if cluster_override and cluster_override in OPENSHIFT_PROMETHEUS_ENDPOINTS:
        endpoint = OPENSHIFT_PROMETHEUS_ENDPOINTS[cluster_override].get("url")
        if endpoint:
            endpoint_type = OPENSHIFT_PROMETHEUS_ENDPOINTS[cluster_override].get(
                "type", "prometheus"
            )
            logger.info(
                f"Using predefined {endpoint_type} endpoint for cluster '{cluster_override}': {endpoint}"
            )
            return (endpoint, endpoint_type)

    # 3. Cache
    cache_key = cluster_override or "default"
    cached = _prometheus_endpoint_cache.get(cache_key)
    if cached:
        logger.info(f"Using cached {cached[1]} endpoint: {cached[0]}")
        return cached

    # 4. Auto-discovery chain - order depends on runtime environment.
    # Coroutines are created lazily here so that each is only awaited once.
    if is_running_in_cluster():
        discovery_methods = [
            (
                "Thanos Query Services",
                _discover_thanos_via_services(k8s_core_api),
                "thanos",
            ),
            (
                "Prometheus Services",
                _discover_prometheus_via_services(k8s_core_api),
                "prometheus",
            ),
            (
                "Prometheus Operator CRD",
                _discover_prometheus_via_operator_crd(k8s_custom_api, k8s_core_api),
                "prometheus",
            ),
            ("OpenShift Routes", _discover_prometheus_via_routes(k8s_custom_api), None),
        ]
    else:
        discovery_methods = [
            ("OpenShift Routes", _discover_prometheus_via_routes(k8s_custom_api), None),
            (
                "Thanos Query Services",
                _discover_thanos_via_services(k8s_core_api),
                "thanos",
            ),
            (
                "Prometheus Operator CRD",
                _discover_prometheus_via_operator_crd(k8s_custom_api, k8s_core_api),
                "prometheus",
            ),
            (
                "Prometheus Services",
                _discover_prometheus_via_services(k8s_core_api),
                "prometheus",
            ),
        ]

    for method_name, discovery_coro, method_type in discovery_methods:
        try:
            logger.debug(f"Attempting discovery via: {method_name}")
            endpoint = await discovery_coro
            if endpoint:
                # For OpenShift Routes, detect type from the discovered URL
                if method_type is None:
                    endpoint_type = (
                        "thanos" if "thanos" in endpoint.lower() else "prometheus"
                    )
                else:
                    endpoint_type = method_type
                _prometheus_endpoint_cache.set(
                    endpoint, cache_key, endpoint_type=endpoint_type
                )
                return (endpoint, endpoint_type)
        except Exception as e:
            logger.warning(f"Discovery method '{method_name}' failed: {e}")
            continue

    # 5. Fallback to predefined endpoints (skip 'local')
    for cluster_name, cfg in OPENSHIFT_PROMETHEUS_ENDPOINTS.items():
        if cluster_name != "local":
            endpoint = cfg.get("url")
            if endpoint:
                endpoint_type = cfg.get("type", "prometheus")
                logger.info(f"Using fallback {endpoint_type} endpoint: {endpoint}")
                _prometheus_endpoint_cache.set(
                    endpoint, cache_key, endpoint_type=endpoint_type
                )
                return (endpoint, endpoint_type)

    logger.error("Could not discover Prometheus/Thanos endpoint via any method")
    return (None, None)
