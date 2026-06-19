"""Certificate health and TLS investigation tools — extracted from server-mcp.py.

Contains:
  - ``investigate_tls_certificate_issues_impl``
  - ``check_cluster_certificate_health_impl``

Both are called by thin ``@mcp.tool()`` wrappers that remain in server-mcp.py.

Fixes #162 (sub-task of #30).
"""

import asyncio
import base64
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from kubernetes.client.rest import ApiException

from helpers import (
    categorize_certificate_status,
    parse_certificate,
)

logger = logging.getLogger("lumino-mcp-server")


async def investigate_tls_certificate_issues_impl(
    time_range: str,
    max_namespaces: int,
    focus_on_system_namespaces: bool,
    *,
    k8s_core_api: Any,
    k8s_custom_api: Any,
    list_namespaces: Callable,
    detect_tekton_namespaces: Callable,
    list_pods_in_namespace: Callable,
    smart_summarize_pod_logs: Callable,
    smart_get_namespace_events: Callable,
) -> Dict[str, Any]:
    """
    Core implementation for TLS/certificate issue investigation.

    Args:
        time_range: Search time range (default: "24h").
        max_namespaces: Max namespaces to search (default: 20).
        focus_on_system_namespaces: Prioritize system namespaces (default: True).
        k8s_core_api: Kubernetes CoreV1Api client.
        k8s_custom_api: Kubernetes CustomObjectsApi client.
        list_namespaces: Callable returning list of namespace strings.
        detect_tekton_namespaces: Callable returning dict of categorised namespaces.
        list_pods_in_namespace: Callable(namespace) returning pod list.
        smart_summarize_pod_logs: Callable for pod log analysis.
        smart_get_namespace_events: Callable for namespace event retrieval.

    Returns:
        Dict: TLS issues, affected pods, certificate problems, and remediation suggestions.
    """
    if not k8s_core_api or not k8s_custom_api:
        return {"error": "Kubernetes client not available."}
    try:
        tool_name = "investigate_tls_certificate_issues"
        logger.info(f"[{tool_name}] Starting TLS certificate issue investigation")

        # Get namespaces to search, prioritizing system namespaces
        all_namespaces = await list_namespaces()

        if focus_on_system_namespaces:
            # Prioritize system namespaces where TLS issues commonly occur
            system_namespaces = [
                ns
                for ns in all_namespaces
                if any(
                    pattern in ns
                    for pattern in [
                        "openshift-",
                        "kube-",
                        "istio-",
                        "ingress",
                        "cert-",
                        "tls-",
                        "monitoring",
                        "logging",
                        "registry",
                        "authentication",
                    ]
                )
            ]
            # Add some Tekton/CI-CD namespaces
            tekton_ns = await detect_tekton_namespaces()
            for category in tekton_ns.values():
                system_namespaces.extend(category[:3])  # Top 3 from each category

            # Remove duplicates and limit
            target_namespaces = list(set(system_namespaces))[:max_namespaces]
        else:
            target_namespaces = all_namespaces[:max_namespaces]

        logger.info(
            f"[{tool_name}] Searching {len(target_namespaces)} namespaces for TLS issues"
        )

        # Search for TLS issues across target namespaces
        tls_issues = []
        affected_pods = []
        certificate_problems = []

        for namespace in target_namespaces:
            try:
                # Get pods in namespace
                pods_info = await list_pods_in_namespace(namespace)

                if not isinstance(pods_info, list) or not pods_info:
                    continue

                # Search pod logs for TLS patterns
                for pod_info in pods_info[:3]:  # Limit to 3 pods per namespace
                    if isinstance(pod_info, dict) and "error" not in pod_info:
                        pod_name = pod_info.get("name", "")

                        try:
                            # Use conservative log analysis focused on TLS issues
                            pod_analysis = await smart_summarize_pod_logs(
                                namespace=namespace,
                                pod_name=pod_name,
                                summary_level="brief",
                                focus_areas=["errors", "security"],
                                max_context_tokens=5000,
                                tail_lines=500,  # Conservative limit
                            )

                            if "error" not in pod_analysis:
                                # Check for TLS patterns in the analysis
                                patterns = pod_analysis.get("patterns", {})
                                error_patterns = patterns.get("errors", [])

                                tls_related_errors = []
                                for error in error_patterns:
                                    error_content = error.get("content", "").lower()
                                    if any(
                                        tls_pattern in error_content
                                        for tls_pattern in [
                                            "tls",
                                            "certificate",
                                            "x509",
                                            "ssl",
                                            "handshake",
                                            "bad certificate",
                                            "certificate verify failed",
                                            "certificate has expired",
                                            "certificate authority",
                                        ]
                                    ):
                                        tls_related_errors.append(error)

                                if tls_related_errors:
                                    tls_issues.extend(tls_related_errors)
                                    affected_pods.append(
                                        {
                                            "namespace": namespace,
                                            "pod_name": pod_name,
                                            "pod_status": pod_info.get(
                                                "status", "Unknown"
                                            ),
                                            "tls_errors": len(tls_related_errors),
                                            "sample_error": tls_related_errors[0].get(
                                                "content", ""
                                            )[:150]
                                            + "...",
                                        }
                                    )

                                    logger.info(
                                        f"[{tool_name}] Found {len(tls_related_errors)} TLS issues in pod {pod_name}"
                                    )

                        except Exception as e:
                            logger.debug(
                                f"Error analyzing pod {pod_name} in {namespace}: {e}"
                            )
                            continue

                # Also check namespace events for certificate-related events
                try:
                    events_result = await smart_get_namespace_events(
                        namespace=namespace,
                        time_period=time_range,
                        focus_areas=["errors", "warnings"],
                        max_context_tokens=3000,
                    )

                    if "events" in events_result and events_result["events"]:
                        for event in events_result["events"][:5]:  # Top 5 events
                            event_content = event.get("event_string", "").lower()

                            tls_patterns = [
                                "certificate",
                                "tls",
                                "x509",
                                "ssl",
                                "handshake",
                            ]
                            matched_pattern = None
                            for pattern in tls_patterns:
                                if pattern in event_content:
                                    matched_pattern = pattern
                                    break

                            if matched_pattern:
                                certificate_problems.append(
                                    {
                                        "namespace": namespace,
                                        "event_type": "kubernetes_event",
                                        "severity": event.get("severity", "UNKNOWN"),
                                        "content": event.get("event_string", "")[:200]
                                        + "...",
                                        "timestamp": event.get("timestamp", "unknown"),
                                    }
                                )

                except Exception as e:
                    logger.debug(f"Error checking events in {namespace}: {e}")

            except Exception as e:
                logger.debug(f"Error processing namespace {namespace}: {e}")
                continue

        # Generate analysis and recommendations
        total_issues = len(tls_issues)
        total_affected_pods = len(affected_pods)
        total_certificate_events = len(certificate_problems)

        analysis_summary = {
            "time_range": time_range,
            "namespaces_searched": len(target_namespaces),
            "total_tls_issues": total_issues,
            "affected_pods": total_affected_pods,
            "certificate_events": total_certificate_events,
            "investigation_focus": (
                "system_namespaces" if focus_on_system_namespaces else "all_namespaces"
            ),
        }

        # Generate specific recommendations for TLS issues
        recommendations = []
        if total_issues > 0:
            recommendations.append(
                f"Found {total_issues} TLS-related issues across {total_affected_pods} pods"
            )
            recommendations.append(
                "Check certificate expiration dates and CA trust chains"
            )
            recommendations.append("Verify service mesh and ingress TLS configurations")

            if any(
                "expired" in issue.get("content", "").lower() for issue in tls_issues
            ):
                recommendations.append(
                    "Certificate expiration detected - immediate renewal required"
                )

            if any(
                "authority" in issue.get("content", "").lower() for issue in tls_issues
            ):
                recommendations.append(
                    "Certificate authority issues detected - check CA trust store"
                )

        else:
            recommendations.append(
                "No TLS certificate issues found in searched namespaces"
            )

        if total_affected_pods > 5:
            recommendations.append(
                "Multiple pods affected - potential cluster-wide certificate issue"
            )

        return {
            "analysis_summary": analysis_summary,
            "tls_issues": tls_issues[:20],  # Limit to top 20 issues
            "affected_pods": affected_pods,
            "certificate_events": certificate_problems,
            "recommendations": recommendations,
            "search_metadata": {
                "tool_optimized_for": "tls_certificate_investigations",
                "token_budget_used": "conservative",
                "search_efficiency": f"{total_issues} issues found across {len(target_namespaces)} namespaces",
            },
        }

    except Exception as e:
        logger.error(
            f"[{tool_name}] Error in TLS investigation: {str(e)}", exc_info=True
        )
        return {
            "error": f"TLS investigation failed: {str(e)}",
            "suggestion": "Try using direct pod log analysis for specific pods with TLS issues",
        }


async def check_cluster_certificate_health_impl(
    warning_threshold_days: int,
    critical_threshold_days: int,
    include_system_certs: bool,
    namespaces: Optional[List[str]],
    certificate_types: Optional[List[str]],
    *,
    k8s_core_api: Any,
    k8s_custom_api: Any,
) -> Dict[str, Any]:
    """
    Core implementation for cluster certificate health scanning.

    Args:
        warning_threshold_days: Days before expiration for warning (default: 30).
        critical_threshold_days: Days before expiration for critical alert (default: 7).
        include_system_certs: Include system certificates (default: True).
        namespaces: Namespaces to scan (default: all accessible).
        certificate_types: Types to check: "tls", "ca", "client", "server" (default: all).
        k8s_core_api: Kubernetes CoreV1Api client.
        k8s_custom_api: Kubernetes CustomObjectsApi client.

    Returns:
        Dict: Certificate health with expiration timeline, recommendations, and security findings.
    """
    if not k8s_core_api or not k8s_custom_api:
        return {"error": "Kubernetes client not available."}
    try:
        logger.info(
            f"Starting cluster certificate health scan with thresholds: warning={warning_threshold_days}d, critical={critical_threshold_days}d"
        )

        # Initialize result structure
        result = {
            "scan_summary": {
                "total_certificates": 0,
                "healthy_certificates": 0,
                "warning_certificates": 0,
                "critical_certificates": 0,
                "expired_certificates": 0,
                "scan_timestamp": datetime.utcnow().isoformat(),
                "namespaces_scanned": 0,
                "namespaces_skipped_rbac": 0,
                "namespaces_total": 0,
            },
            "certificate_details": [],
            "system_certificates": [],
            "expiration_timeline": [],
            "renewal_recommendations": [],
            "security_findings": [],
            "certificate_authorities": [],
            "scan_coverage": {"scanned_namespaces": [], "skipped_namespaces_rbac": []},
        }

        # Determine namespaces to scan
        target_namespaces = namespaces or []
        if not target_namespaces:
            # Get all accessible namespaces
            try:
                all_ns = await asyncio.to_thread(k8s_core_api.list_namespace)
                target_namespaces = [
                    ns.metadata.name
                    for ns in all_ns.items
                    if ns.metadata and ns.metadata.name
                ]
                logger.info(
                    f"Scanning all {len(target_namespaces)} accessible namespaces"
                )
            except ApiException as e:
                logger.warning(
                    f"Could not list all namespaces, using default set: {e.reason}"
                )
                target_namespaces = [
                    "default",
                    "kube-system",
                    "openshift-config",
                    "openshift-ingress",
                ]

        # Set default certificate types
        if not certificate_types:
            certificate_types = ["tls", "ca", "client", "server"]

        certificates_found = []
        ca_certificates = {}
        scanned_namespaces = []
        skipped_namespaces_rbac = []

        # Scan for TLS secrets in each namespace
        for namespace in target_namespaces:
            try:
                logger.debug(f"Scanning namespace: {namespace}")
                secrets = await asyncio.to_thread(
                    k8s_core_api.list_namespaced_secret, namespace
                )
                scanned_namespaces.append(namespace)

                for secret in secrets.items:
                    if not secret.data:
                        continue

                    # Check if secret contains certificate data
                    cert_keys = [
                        "tls.crt",
                        "ca.crt",
                        "cert",
                        "certificate",
                        "client.crt",
                        "server.crt",
                    ]

                    for key in cert_keys:
                        if key in secret.data:
                            try:
                                # Decode base64 certificate data
                                cert_data = base64.b64decode(secret.data[key]).decode(
                                    "utf-8"
                                )

                                # Handle certificate chains (multiple certificates)
                                cert_blocks = cert_data.split(
                                    "-----END CERTIFICATE-----"
                                )

                                for i, cert_block in enumerate(cert_blocks):
                                    if "-----BEGIN CERTIFICATE-----" in cert_block:
                                        full_cert = (
                                            cert_block + "-----END CERTIFICATE-----"
                                        )
                                        cert_info = parse_certificate(full_cert)

                                        if cert_info:
                                            cert_details = {
                                                "certificate_info": {
                                                    "name": (
                                                        f"{secret.metadata.name}_{key}_{i}"
                                                        if i > 0
                                                        else f"{secret.metadata.name}_{key}"
                                                    ),
                                                    "namespace": namespace,
                                                    "secret_name": secret.metadata.name,
                                                    "key_name": key,
                                                    "type": secret.type or "Opaque",
                                                },
                                                "certificate_data": cert_info,
                                                "validity": {
                                                    "not_before": cert_info[
                                                        "not_before"
                                                    ],
                                                    "not_after": cert_info["not_after"],
                                                    "days_remaining": cert_info[
                                                        "days_remaining"
                                                    ],
                                                    "status": categorize_certificate_status(
                                                        cert_info["days_remaining"],
                                                        warning_threshold_days,
                                                        critical_threshold_days,
                                                    ),
                                                },
                                                "usage": {
                                                    "is_ca": cert_info.get(
                                                        "is_ca", False
                                                    )
                                                    or "ca" in key.lower(),
                                                    "is_client": "client"
                                                    in key.lower(),
                                                    "is_server": "server" in key.lower()
                                                    or "tls" in key.lower(),
                                                    "san_domains": cert_info.get(
                                                        "san", []
                                                    ),
                                                },
                                                "chain_validation": {
                                                    "is_self_signed": cert_info.get(
                                                        "subject_cn"
                                                    )
                                                    == cert_info.get("issuer_cn"),
                                                    "issuer": cert_info.get(
                                                        "issuer_cn", "Unknown"
                                                    ),
                                                    "chain_length": (
                                                        len(cert_blocks)
                                                        if len(cert_blocks) > 1
                                                        else 1
                                                    ),
                                                },
                                            }

                                            certificates_found.append(cert_details)

                                            # Track CA certificates
                                            if cert_details["usage"]["is_ca"]:
                                                ca_name = cert_info.get(
                                                    "subject_cn", "Unknown CA"
                                                )
                                                if ca_name not in ca_certificates:
                                                    ca_certificates[ca_name] = {
                                                        "ca_name": ca_name,
                                                        "issued_certificates": 0,
                                                        "ca_expiry": cert_info[
                                                            "not_after"
                                                        ],
                                                        "trust_status": (
                                                            "trusted"
                                                            if not cert_details[
                                                                "chain_validation"
                                                            ]["is_self_signed"]
                                                            else "self-signed"
                                                        ),
                                                    }
                                                ca_certificates[ca_name][
                                                    "issued_certificates"
                                                ] += 1

                            except Exception as e:
                                logger.debug(
                                    f"Could not parse certificate {key} in secret {secret.metadata.name}: {e}"
                                )
                                continue

            except ApiException as e:
                if e.status == 403:
                    logger.debug(f"Access denied to namespace {namespace}: {e.reason}")
                    if namespace not in skipped_namespaces_rbac:
                        skipped_namespaces_rbac.append(namespace)
                else:
                    logger.warning(f"Error scanning namespace {namespace}: {e.reason}")
                continue

        # Process OpenShift system certificates if requested
        # Always scan system cert namespaces when include_system_certs=True,
        # even when specific namespaces were provided (they may have been RBAC-blocked)
        if include_system_certs:
            try:
                # Try to get OpenShift cluster certificates
                system_cert_namespaces = [
                    "openshift-config",
                    "openshift-ingress",
                    "openshift-ingress-operator",
                    "openshift-kube-apiserver",
                    "openshift-etcd",
                ]

                for sys_ns in system_cert_namespaces:
                    if sys_ns not in scanned_namespaces:
                        try:
                            secrets = await asyncio.to_thread(
                                k8s_core_api.list_namespaced_secret, sys_ns
                            )
                            scanned_namespaces.append(sys_ns)
                            for secret in secrets.items:
                                if secret.data:
                                    for key in ["tls.crt", "ca.crt"]:
                                        if key in secret.data:
                                            try:
                                                # Properly parse the certificate
                                                cert_data = base64.b64decode(
                                                    secret.data[key]
                                                ).decode("utf-8")
                                                if (
                                                    "-----BEGIN CERTIFICATE-----"
                                                    in cert_data
                                                ):
                                                    cert_info = parse_certificate(
                                                        cert_data
                                                    )
                                                    if cert_info:
                                                        status = categorize_certificate_status(
                                                            cert_info["days_remaining"],
                                                            warning_threshold_days,
                                                            critical_threshold_days,
                                                        )
                                                        result[
                                                            "system_certificates"
                                                        ].append(
                                                            {
                                                                "component": sys_ns.replace(
                                                                    "openshift-", ""
                                                                ),
                                                                "certificate_purpose": secret.metadata.name,
                                                                "subject_cn": cert_info.get(
                                                                    "subject_cn",
                                                                    "Unknown",
                                                                ),
                                                                "expiry_date": cert_info.get(
                                                                    "not_after",
                                                                    "Unknown",
                                                                ),
                                                                "days_remaining": cert_info.get(
                                                                    "days_remaining", 0
                                                                ),
                                                                "status": status,
                                                                "auto_renewal": True,
                                                                "renewal_mechanism": "OpenShift Certificate Operator",
                                                            }
                                                        )
                                            except Exception as parse_err:
                                                logger.debug(
                                                    f"Could not parse system cert {secret.metadata.name}/{key}: {parse_err}"
                                                )
                        except ApiException as e:
                            if e.status == 403:
                                if sys_ns not in skipped_namespaces_rbac:
                                    skipped_namespaces_rbac.append(sys_ns)
                            continue

            except Exception as e:
                logger.debug(f"Could not scan system certificates: {e}")

        # Update scan summary
        total_certs = len(certificates_found)
        healthy_count = len(
            [c for c in certificates_found if c["validity"]["status"] == "healthy"]
        )
        warning_count = len(
            [c for c in certificates_found if c["validity"]["status"] == "warning"]
        )
        critical_count = len(
            [c for c in certificates_found if c["validity"]["status"] == "critical"]
        )
        expired_count = len(
            [c for c in certificates_found if c["validity"]["status"] == "expired"]
        )

        result["scan_summary"].update(
            {
                "total_certificates": total_certs,
                "healthy_certificates": healthy_count,
                "warning_certificates": warning_count,
                "critical_certificates": critical_count,
                "expired_certificates": expired_count,
                "namespaces_scanned": len(scanned_namespaces),
                "namespaces_skipped_rbac": len(skipped_namespaces_rbac),
                "namespaces_total": len(target_namespaces),
            }
        )

        # Update scan coverage
        result["scan_coverage"] = {
            "scanned_namespaces": scanned_namespaces,
            "skipped_namespaces_rbac": skipped_namespaces_rbac[
                :50
            ],  # Limit to first 50 to avoid huge output
        }

        # Add RBAC warning if many namespaces were skipped
        if len(skipped_namespaces_rbac) > len(scanned_namespaces):
            result["security_findings"].append(
                {
                    "type": "rbac_limitation",
                    "severity": "info",
                    "message": f"RBAC restrictions prevented scanning {len(skipped_namespaces_rbac)} namespaces. "
                    f"Only {len(scanned_namespaces)} namespaces were accessible. "
                    "Consider granting 'list secrets' permission for comprehensive certificate scanning.",
                }
            )

        # Filter certificates by type if specified
        if certificate_types and "all" not in certificate_types:
            filtered_certs = []
            for cert in certificates_found:
                cert_usage = cert["usage"]
                if (
                    ("tls" in certificate_types and cert_usage["is_server"])
                    or ("ca" in certificate_types and cert_usage["is_ca"])
                    or ("client" in certificate_types and cert_usage["is_client"])
                    or ("server" in certificate_types and cert_usage["is_server"])
                ):
                    filtered_certs.append(cert)
            certificates_found = filtered_certs

        result["certificate_details"] = certificates_found

        # Generate expiration timeline
        timeline_dict = defaultdict(list)
        for cert in certificates_found:
            if (
                cert["validity"]["days_remaining"] >= 0
            ):  # Don't include expired certs in timeline
                expiry_date = cert["certificate_data"]["not_after"][
                    :10
                ]  # Just the date part
                timeline_dict[expiry_date].append(
                    {
                        "name": cert["certificate_info"]["name"],
                        "namespace": cert["certificate_info"]["namespace"],
                        "days_remaining": cert["validity"]["days_remaining"],
                        "status": cert["validity"]["status"],
                    }
                )

        # Sort timeline by date
        sorted_timeline = []
        for date in sorted(timeline_dict.keys()):
            sorted_timeline.append(
                {"date": date, "certificates_expiring": timeline_dict[date]}
            )

        result["expiration_timeline"] = sorted_timeline[
            :30
        ]  # Limit to next 30 expiration dates

        # Generate renewal recommendations
        for cert in certificates_found:
            if cert["validity"]["status"] in ["critical", "warning", "expired"]:
                urgency = (
                    "immediate"
                    if cert["validity"]["status"] in ["critical", "expired"]
                    else "soon"
                )

                recommendation = {
                    "certificate": cert["certificate_info"]["name"],
                    "namespace": cert["certificate_info"]["namespace"],
                    "urgency": urgency,
                    "renewal_method": "manual",
                    "steps": [
                        f"Generate new certificate for {cert['certificate_data'].get('subject_cn', 'unknown subject')}",
                        f"Update secret {cert['certificate_info']['secret_name']} in namespace {cert['certificate_info']['namespace']}",
                        "Restart affected pods/services",
                    ],
                    "automation_available": cert["certificate_info"][
                        "namespace"
                    ].startswith("openshift-"),
                }

                if cert["certificate_info"]["namespace"].startswith("openshift-"):
                    recommendation["renewal_method"] = "OpenShift Certificate Operator"
                    recommendation["steps"] = [
                        "Certificate should auto-renew via OpenShift Certificate Operator",
                        "If not auto-renewing, check cluster operator status",
                        "Manual intervention may be required",
                    ]

                result["renewal_recommendations"].append(recommendation)

        # Generate security findings
        for cert in certificates_found:
            cert_data = cert["certificate_data"]

            # Check for weak algorithms
            if "sha1" in cert_data.get("signature_algorithm", "").lower():
                result["security_findings"].append(
                    {
                        "certificate": cert["certificate_info"]["name"],
                        "finding_type": "weak_algorithm",
                        "description": "Certificate uses weak SHA-1 signature algorithm",
                        "severity": "medium",
                        "recommendation": "Replace with SHA-256 or stronger algorithm",
                    }
                )

            # Check for self-signed certificates
            if (
                cert["chain_validation"]["is_self_signed"]
                and not cert["usage"]["is_ca"]
            ):
                result["security_findings"].append(
                    {
                        "certificate": cert["certificate_info"]["name"],
                        "finding_type": "self_signed",
                        "description": "Self-signed certificate detected",
                        "severity": "low",
                        "recommendation": "Consider using CA-signed certificate for production",
                    }
                )

            # Check for short validity periods
            if (
                cert["validity"]["days_remaining"] < critical_threshold_days
                and cert["validity"]["status"] != "expired"
            ):
                result["security_findings"].append(
                    {
                        "certificate": cert["certificate_info"]["name"],
                        "finding_type": "short_validity",
                        "description": f"Certificate expires in {cert['validity']['days_remaining']} days",
                        "severity": "high",
                        "recommendation": "Renew certificate immediately",
                    }
                )

        # Add CA information
        result["certificate_authorities"] = list(ca_certificates.values())

        logger.info(
            f"Certificate health scan completed: {total_certs} certificates found, {critical_count + expired_count} require immediate attention"
        )
        return result

    except Exception as e:
        logger.error(f"Error during certificate health check: {str(e)}", exc_info=True)
        return {
            "scan_summary": {
                "total_certificates": 0,
                "healthy_certificates": 0,
                "warning_certificates": 0,
                "critical_certificates": 0,
                "expired_certificates": 0,
                "scan_timestamp": datetime.utcnow().isoformat(),
                "error": str(e),
            },
            "certificate_details": [],
            "system_certificates": [],
            "expiration_timeline": [],
            "renewal_recommendations": [],
            "security_findings": [],
            "certificate_authorities": [],
        }
