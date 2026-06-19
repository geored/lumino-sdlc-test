"""
Log fetching, parsing, and analysis utilities for LUMINO MCP Server.
"""

import asyncio
import json
import logging
import re
import yaml
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("lumino-mcp")
_utils_logger = logging.getLogger("lumino-mcp")

def _strip_none_values(obj):
    """Recursively strip None values and empty dicts/lists from a nested dict for cleaner output."""
    if isinstance(obj, dict):
        return {
            k: _strip_none_values(v)
            for k, v in obj.items()
            if v is not None and _strip_none_values(v) not in (None, {}, [])
        }
    elif isinstance(obj, list):
        return [_strip_none_values(item) for item in obj if item is not None]
    return obj

def format_yaml_output(
    resource_obj: Any, resource_type: str, name: str, namespace: str
) -> str:
    """Format resource as YAML output."""
    try:
        if hasattr(resource_obj, "to_dict"):
            resource_dict = resource_obj.to_dict()
        else:
            resource_dict = resource_obj

        yaml_output = yaml.dump(resource_dict, default_flow_style=False, indent=2)
        return f"# {resource_type.title()} '{name}' in namespace '{namespace}'\n\n{yaml_output}"
    except Exception as e:
        return f"Error formatting YAML: {str(e)}"

def format_detailed_output(
    resource_obj: Any, resource_type: str, name: str, namespace: str
) -> str:
    """Format resource with detailed information."""
    try:
        if hasattr(resource_obj, "to_dict"):
            resource_dict = resource_obj.to_dict()
        else:
            resource_dict = resource_obj

        output = [f"=== {resource_type.upper()} DETAILS ==="]
        output.append(f"Name: {name}")
        output.append(f"Namespace: {namespace}")

        # Metadata
        metadata = resource_dict.get("metadata", {})
        if metadata:
            output.append("\n--- METADATA ---")
            output.append(f"UID: {metadata.get('uid', 'N/A')}")
            output.append(
                f"Resource Version: {metadata.get('resource_version', 'N/A')}"
            )

            # Creation timestamp
            created = metadata.get("creation_timestamp")
            if created:
                if isinstance(created, str):
                    output.append(f"Created: {created}")
                else:
                    output.append(f"Created: {created.isoformat()}")

            # Labels
            labels = metadata.get("labels", {})
            if labels:
                output.append("\nLabels:")
                for key, value in labels.items():
                    output.append(f"  {key}: {value}")

            # Annotations
            annotations = metadata.get("annotations", {})
            if annotations:
                output.append("\nAnnotations:")
                for key, value in annotations.items():
                    output.append(f"  {key}: {value}")

        # Spec
        spec = resource_dict.get("spec", {})
        if spec:
            output.append("\n--- SPECIFICATION ---")
            output.append(json.dumps(_strip_none_values(spec), indent=2, default=str))

        # Status
        status = resource_dict.get("status", {})
        if status:
            output.append("\n--- STATUS ---")
            output.append(json.dumps(_strip_none_values(status), indent=2, default=str))

        return "\n".join(output)

    except Exception as e:
        return f"Error formatting detailed output: {str(e)}"

def format_summary_output(
    resource_obj: Any, resource_type: str, name: str, namespace: str
) -> str:
    """Format resource with summary information."""
    try:
        if hasattr(resource_obj, "to_dict"):
            resource_dict = resource_obj.to_dict()
        else:
            resource_dict = resource_obj

        cluster_scoped_types = [
            "node",
            "namespace",
            "persistentvolume",
            "pv",
            "storageclass",
            "clustertask",
        ]
        output = [f"=== {resource_type.upper()} SUMMARY ==="]
        output.append(f"Name: {name}")
        if resource_type not in cluster_scoped_types:
            output.append(f"Namespace: {namespace}")

        metadata = resource_dict.get("metadata", {})

        # Creation time
        created = metadata.get("creation_timestamp")
        if created:
            if isinstance(created, str):
                output.append(f"Created: {created}")
            else:
                output.append(f"Created: {created.isoformat()}")

        # Labels (limited)
        labels = metadata.get("labels", {})
        if labels:
            label_summary = ", ".join([f"{k}={v}" for k, v in list(labels.items())[:3]])
            if len(labels) > 3:
                label_summary += f" (and {len(labels) - 3} more)"
            output.append(f"Labels: {label_summary}")

        # Resource-specific summary
        spec = resource_dict.get("spec", {})
        status = resource_dict.get("status", {})

        if resource_type == "deployment":
            replicas = spec.get("replicas", 0)
            ready_replicas = status.get("ready_replicas", 0)
            output.append(f"Replicas: {ready_replicas}/{replicas}")

        elif resource_type == "pod":
            phase = status.get("phase", "Unknown")
            output.append(f"Phase: {phase}")
            containers = spec.get("containers", [])
            output.append(f"Containers: {len(containers)}")

        elif resource_type == "service":
            service_type = spec.get("type", "ClusterIP")
            cluster_ip = spec.get("cluster_ip")
            output.append(f"Type: {service_type}")
            if cluster_ip:
                output.append(f"Cluster IP: {cluster_ip}")

        elif resource_type in ["pipelinerun", "taskrun"]:
            # Tekton-specific summary
            conditions = status.get("conditions", [])
            if conditions:
                latest_condition = conditions[-1]
                condition_type = latest_condition.get("type", "Unknown")
                condition_status = latest_condition.get("status", "Unknown")
                output.append(f"Status: {condition_type} - {condition_status}")

            start_time = status.get("start_time")
            completion_time = status.get("completion_time")
            if start_time:
                output.append(f"Started: {start_time}")
            if completion_time:
                output.append(f"Completed: {completion_time}")

        elif resource_type == "node":
            # Node-specific summary
            capacity = status.get("capacity", {})
            allocatable = status.get("allocatable", {})
            if capacity:
                output.append(
                    f"CPU: {allocatable.get('cpu', '?')}/{capacity.get('cpu', '?')} (allocatable/capacity)"
                )
                output.append(
                    f"Memory: {allocatable.get('memory', '?')}/{capacity.get('memory', '?')}"
                )
                output.append(
                    f"Pods: {allocatable.get('pods', '?')}/{capacity.get('pods', '?')}"
                )
            node_info = status.get("node_info", {})
            if node_info:
                output.append(f"Kubelet: {node_info.get('kubelet_version', 'unknown')}")
                output.append(f"OS: {node_info.get('os_image', 'unknown')}")
                output.append(
                    f"Container Runtime: {node_info.get('container_runtime_version', 'unknown')}"
                )
            # Show roles from labels
            roles = [
                k.replace("node-role.kubernetes.io/", "")
                for k in labels
                if k.startswith("node-role.kubernetes.io/")
            ]
            if roles:
                output.append(f"Roles: {', '.join(roles)}")
            # Taints
            taints = spec.get("taints", [])
            if taints:
                taint_strs = [
                    f"{t.get('key', '?')}={t.get('value', '')}:{t.get('effect', '?')}"
                    for t in taints[:3]
                ]
                output.append(f"Taints: {', '.join(taint_strs)}")

        elif resource_type in ["pipeline", "task"]:
            # Tekton pipeline/task summary
            params = spec.get("params", [])
            if params:
                output.append(f"Parameters: {len(params)}")

            if resource_type == "pipeline":
                tasks = spec.get("tasks", [])
                output.append(f"Tasks: {len(tasks)}")
            else:  # task
                steps = spec.get("steps", [])
                output.append(f"Steps: {len(steps)}")

        # Add any important status conditions
        if status.get("conditions"):
            conditions = status["conditions"]
            if isinstance(conditions, list) and conditions:
                latest = conditions[-1]
                cond_type = latest.get("type", "Unknown")
                cond_status = latest.get("status", "Unknown")
                if cond_type not in ["Ready"] or resource_type not in ["deployment"]:
                    output.append(f"Condition: {cond_type}={cond_status}")

        return "\n".join(output)

    except Exception as e:
        return f"Error formatting summary: {str(e)}"

def calculate_context_tokens(text: str) -> int:
    """
    Estimate token count for text (conservative approximation).

    Uses a simple heuristic: 1 token ≈ 3 characters.
    This is intentionally conservative to avoid exceeding limits.
    """
    return len(text) // 3

async def get_all_pod_logs(
    pod_name: str,
    namespace: str,
    k8s_core_api,
    tail_lines: Optional[int] = None,
    since_seconds: Optional[int] = None,
    since_time: Optional[str] = None,
    timestamps: bool = True,
    previous: bool = False,
) -> Dict[str, str]:
    """
    Reads logs from all containers in a specified pod with optional filtering.

    Args:
        pod_name: The name of the pod.
        namespace: The namespace of the pod.
        k8s_core_api: Kubernetes Core API client
        tail_lines: Number of lines to retrieve from the end of logs.
        since_seconds: Retrieve logs newer than this many seconds.
        since_time: Retrieve logs newer than this RFC3339 timestamp.
        timestamps: Include timestamps in log output.
        previous: Retrieve logs from previous container instance.

    Returns:
        Dictionary where keys are container names and values are their logs.

    Raises:
        ValueError: If more than one of since_seconds, since_time,
            or tail_lines is provided.  These parameters are mutually
            exclusive -- the Kubernetes log API only honours a single
            time/line filter per request.

    Note:
        since_seconds, since_time, and tail_lines are mutually exclusive.
        Provide at most one of them.  If since_time is given it is internally
        converted to since_seconds because the Kubernetes Python client does
        not support the sinceTime query parameter directly.
    """
    # --- Validate mutual exclusivity of time/line filters ----------------
    _time_filters = {
        "since_seconds": since_seconds,
        "since_time": since_time,
        "tail_lines": tail_lines,
    }
    _provided = [name for name, value in _time_filters.items() if value is not None]
    if len(_provided) > 1:
        raise ValueError(
            f"Only one of since_seconds, since_time, or tail_lines may be "
            f"provided at a time (got: {', '.join(_provided)})"
        )

    container_logs = {}

    try:
        # Get the pod object to find its containers
        pod = await asyncio.to_thread(
            k8s_core_api.read_namespaced_pod, name=pod_name, namespace=namespace
        )

        # Check if pod has containers
        if not pod.spec.containers:
            logger.warning(f"Pod {pod_name} has no containers defined")
            return {"no_containers": "Pod has no containers defined"}

        # Get the names of all containers in the pod
        container_names = [container.name for container in pod.spec.containers]
        logger.debug(
            f"Found {len(container_names)} containers in pod {pod_name}: {container_names}"
        )

        # Build log parameters
        log_params = {
            "name": pod_name,
            "namespace": namespace,
            "container": None,  # Will be set per container
            "timestamps": timestamps,
            "previous": previous,
        }

        # Add optional time/line filtering parameters
        # Note: Kubernetes Python client does NOT support since_time parameter
        # (see https://github.com/kubernetes-client/python/issues/1351)
        # We must convert since_time to since_seconds
        if since_time:
            try:
                # Parse RFC3339 timestamp and convert to seconds from now
                from datetime import timezone

                since_dt = datetime.fromisoformat(since_time.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                delta = now - since_dt
                computed_since_seconds = max(1, int(delta.total_seconds()))
                log_params["since_seconds"] = computed_since_seconds
                logger.debug(
                    f"Converted since_time '{since_time}' to since_seconds={computed_since_seconds}"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to parse since_time '{since_time}': {e}, ignoring filter"
                )
        elif since_seconds:
            log_params["since_seconds"] = since_seconds
        elif tail_lines:
            log_params["tail_lines"] = tail_lines

        # Loop through each container and fetch its logs
        for container_name in container_names:
            try:
                # Use a fresh copy per container to avoid leaking state
                container_params = log_params.copy()
                container_params["container"] = container_name

                # Read the logs for the specific container
                logs = await asyncio.to_thread(
                    k8s_core_api.read_namespaced_pod_log, **container_params
                )
                container_logs[container_name] = logs
            except Exception as e:
                if hasattr(e, "reason"):
                    logger.warning(
                        f"Error reading logs for container {container_name} in pod {pod_name}: {e}"
                    )
                    container_logs[container_name] = f"Error fetching logs: {e.reason}"
                else:
                    logger.warning(
                        f"Unexpected error fetching logs for container {container_name} in pod {pod_name}: {e}"
                    )
                    container_logs[container_name] = (
                        f"Unexpected error fetching logs: {str(e)}"
                    )

    except Exception as e:
        if hasattr(e, "reason"):
            logger.error(f"Error getting pod details for {pod_name}: {e}")
            return {"pod_error": f"Error getting pod details: {e.reason}"}
        else:
            logger.error(f"Unexpected error getting pod details for {pod_name}: {e}")
            return {"pod_error": f"Unexpected error getting pod details: {str(e)}"}

    # Ensure we always return something
    if not container_logs:
        return {"no_logs": "No logs found for any containers in this pod"}

    return container_logs

def clean_pipeline_logs(raw_logs: str) -> str:
    """
    Clean pipeline logs by removing escape characters, line continuation symbols,
    and properly formatting JSON log entries commonly found in CI/CD pipeline outputs.

    This function handles common issues with pipeline logs:
    1. Line continuation characters (│, ┌, └, etc.)
    2. Multiple levels of JSON escaping
    3. Escaped newlines and other characters
    4. Terminal formatting characters and ANSI codes

    Args:
        raw_logs: Raw log content from pipeline pods

    Returns:
        Cleaned and formatted log content
    """
    if not raw_logs or raw_logs.strip() == "":
        return raw_logs

    try:
        # Split logs into individual lines
        lines = raw_logs.strip().split("\n")
        cleaned_lines = []

        for line in lines:
            if not line:
                continue

            # Remove line continuation characters commonly found in pipeline logs
            cleaned_line = re.sub(
                r"[│┌└├┤┐┘┬┴┼─═║╒╓╔╕╖╗╘╙╚╛╜╝╞╟╠╡╢╣╤╥╦╧╨╩╪╫╬]", "", line
            )

            # Remove leading/trailing whitespace
            cleaned_line = cleaned_line.strip()

            if not cleaned_line:
                continue

            # Skip lines that are just separators or formatting
            if re.match(r"^[─═│┌└├┤┐┘┬┴┼\s]*$", cleaned_line):
                continue

            # Remove ANSI escape codes
            cleaned_line = re.sub(r"\x1b\[[0-9;]*m", "", cleaned_line)

            # Handle multiple levels of JSON escaping
            cleaned_line = cleaned_line.replace('\\\\"', '"')
            cleaned_line = cleaned_line.replace("\\n", "\n")
            cleaned_line = cleaned_line.replace("\\/", "/")
            cleaned_line = cleaned_line.replace("\\t", "\t")
            cleaned_line = cleaned_line.replace("\\r", "\r")

            # Try to identify and format JSON log entries
            try:
                json_match = re.search(r"\{.*\}", cleaned_line)
                if json_match:
                    json_part = json_match.group(0)
                    prefix = cleaned_line[: json_match.start()].strip()
                    suffix = cleaned_line[json_match.end() :].strip()

                    try:
                        json_obj = json.loads(json_part)

                        if isinstance(json_obj, dict):
                            # Check for Renovate/dependency bot logs
                            if (
                                "name" in json_obj
                                and json_obj.get("name") == "renovate"
                            ):
                                timestamp = json_obj.get("time", "")
                                level = json_obj.get("level", "info")
                                msg = json_obj.get("msg", "")
                                repository = json_obj.get("repository", "")

                                formatted_parts = []
                                if timestamp:
                                    formatted_parts.append(f"[{timestamp}]")
                                formatted_parts.append(f"[RENOVATE/{level.upper()}]")
                                if repository:
                                    formatted_parts.append(f"[{repository}]")
                                if msg:
                                    formatted_parts.append(msg)

                                for key, value in json_obj.items():
                                    if (
                                        key
                                        not in [
                                            "name",
                                            "time",
                                            "level",
                                            "msg",
                                            "repository",
                                            "hostname",
                                            "pid",
                                            "v",
                                        ]
                                        and value is not None
                                    ):
                                        if isinstance(value, (str, int, float, bool)):
                                            formatted_parts.append(f"{key}={value}")
                                        elif (
                                            isinstance(value, dict)
                                            and len(str(value)) < 200
                                        ):
                                            formatted_parts.append(
                                                f"{key}={json.dumps(value, separators=(',', ':'))}"
                                            )

                                formatted_line = " ".join(formatted_parts)
                            else:
                                formatted_json = json.dumps(
                                    json_obj, indent=2, separators=(",", ": ")
                                )
                                formatted_line = (
                                    f"{prefix} {formatted_json} {suffix}".strip()
                                )
                        else:
                            formatted_json = json.dumps(json_obj, separators=(",", ":"))
                            formatted_line = (
                                f"{prefix} {formatted_json} {suffix}".strip()
                            )

                        cleaned_lines.append(formatted_line)

                    except json.JSONDecodeError:
                        if prefix or suffix:
                            cleaned_line = f"{prefix} {json_part} {suffix}".strip()
                        else:
                            cleaned_line = json_part
                        cleaned_lines.append(cleaned_line)
                else:
                    cleaned_lines.append(cleaned_line)

            except Exception as e:
                logger.debug(f"Failed to process pipeline log line: {e}")
                cleaned_lines.append(cleaned_line)

        # Join the cleaned lines
        result = "\n".join(cleaned_lines)

        # Final cleanup - remove excessive whitespace and empty lines
        result = re.sub(r"\n\s*\n", "\n", result)
        result = re.sub(r" +", " ", result)

        return result.strip()

    except Exception as e:
        logger.error(f"Error cleaning pipeline logs: {e}")
        return raw_logs

def _normalize_log_newlines(log_text: str) -> str:
    """Normalize log text by converting literal backslash-n sequences to actual newlines.

    When log text is passed through JSON/MCP boundaries, actual newlines may arrive
    as literal two-character '\\n' sequences. This normalizes them so line splitting works.
    """
    # Replace literal \n (two chars: backslash + n) with actual newline,
    # but only when they appear between log lines (not inside JSON strings).
    # Heuristic: literal \n followed by a timestamp pattern or log-level keyword.
    import re

    # Replace literal \n that precede a timestamp (YYYY-MM-DD) or log-level prefix
    normalized = re.sub(
        r'\\n(?=\d{4}-\d{2}-\d{2}|level=|"level"|time=|"ts"|msg=|http:)', "\n", log_text
    )
    # Also handle remaining literal \n as a fallback if no actual newlines are present
    if "\n" not in normalized and "\\n" in normalized:
        normalized = normalized.replace("\\n", "\n")
    return normalized

def extract_error_patterns(log_text: str) -> List[str]:
    """
    Extract common error patterns from log text including Kubernetes-specific errors.

    Args:
        log_text: Raw log content to analyze

    Returns:
        List of error lines found in the logs (max 15)
    """
    if not log_text or log_text == "No pod logs available":
        return []

    # Normalize escaped newlines from MCP/JSON transport
    log_text = _normalize_log_newlines(log_text)

    # Common error patterns to look for (case-insensitive)
    patterns = [
        # General error indicators
        "Error:",
        "Exception:",
        "Failed:",
        "fatal:",
        "panic:",
        "cannot",
        "unable to",
        "failed to",
        "error",
        "invalid",
        "No such file",
        "Permission denied",
        "Out of memory",
        "Connection refused",
        "timed out",
        # Kubernetes-specific errors
        "OOMKilled",
        "CrashLoopBackOff",
        "ImagePullBackOff",
        "ErrImagePull",
        "CreateContainerConfigError",
        "CreateContainerError",
        "FailedMount",
        "FailedAttachVolume",
        "FailedScheduling",
        "Unschedulable",
        "BackOff",
        "Evicted",
        # Container runtime errors
        "container killed",
        "container exited",
        "restart count",
        "liveness probe failed",
        "readiness probe failed",
        # Network errors
        "dial tcp",
        "no route to host",
        "connection reset",
        # Resource errors
        "quota exceeded",
        "limit exceeded",
        "insufficient",
    ]

    # Find lines containing these patterns
    error_lines = []
    for line in log_text.split("\n"):
        line = line.strip()
        if (
            any(pattern.lower() in line.lower() for pattern in patterns)
            and len(line) > 10
        ):
            # Limit to a reasonable length for readability
            if len(line) > 200:
                line = line[:197] + "..."
            error_lines.append(line)

    # Return a limited number of most relevant error lines
    return error_lines[:15]

def categorize_errors(log_text: str, error_patterns: List[str]) -> Dict[str, int]:
    """
    Categorize errors into common types including Kubernetes-specific errors.

    Args:
        log_text: Raw log content
        error_patterns: List of extracted error patterns

    Returns:
        Dictionary mapping error categories to occurrence counts
    """
    categories = {
        # Kubernetes-specific categories
        "oom": [
            "oomkilled",
            "oom killed",
            "out of memory",
            "memory limit exceeded",
            "exceeded memory",
        ],
        "crash": [
            "crashloopbackoff",
            "crash loop",
            "container crashed",
            "backoff restarting",
        ],
        "image": [
            "imagepullbackoff",
            "errimagepull",
            "image pull",
            "pull image",
            "registry",
        ],
        "scheduling": [
            "unschedulable",
            "failedscheduling",
            "insufficient",
            "node affinity",
        ],
        "storage": [
            "failedmount",
            "volume mount",
            "pvc",
            "persistent volume",
            "mount failed",
        ],
        "config": [
            "createcontainerconfigerror",
            "configmap",
            "secret not found",
            "missing key",
            "invalid config",
        ],
        # General categories
        "resource_limits": [
            "memory limit",
            "cpu limit",
            "resource limit",
            "resource quota",
            "evicted",
        ],
        "network": [
            "timeout",
            "connection refused",
            "connection reset",
            "unreachable",
            "dns lookup",
            "dial tcp",
        ],
        "permissions": [
            "access denied",
            "permission denied",
            "forbidden",
            "unauthorized",
            "rbac",
        ],
        "configuration": [
            "invalid configuration",
            "missing parameter",
            "environment variable",
        ],
        "dependency": [
            "not found",
            "missing dependency",
            "version mismatch",
            "incompatible",
        ],
        "filesystem": [
            "no such file",
            "directory not found",
            "file not found",
            "read-only filesystem",
        ],
    }

    counts = {category: 0 for category in categories.keys()}

    # Combined text from logs and error patterns
    combined_text = log_text.lower() + " " + " ".join(error_patterns).lower()

    # Count occurrences
    for category, terms in categories.items():
        for term in terms:
            counts[category] += combined_text.count(term.lower())

    # Filter out categories with no matches
    return {category: count for category, count in counts.items() if count > 0}

def generate_log_summary(
    log_text: str, error_patterns: List[str], error_categories: Dict[str, int]
) -> str:
    """
    Generate a concise summary of log analysis.

    Args:
        log_text: Raw log content
        error_patterns: List of extracted error patterns
        error_categories: Dictionary of categorized errors

    Returns:
        Human-readable summary string
    """
    if not log_text or log_text == "No pod logs available":
        return "No logs available to analyze."

    # Normalize escaped newlines from MCP/JSON transport
    log_text = _normalize_log_newlines(log_text)

    # Count total lines in log
    total_lines = len(log_text.split("\n"))

    # Get first and last timestamp if available
    first_timestamp = None
    last_timestamp = None
    for line in log_text.split("\n"):
        if len(line.strip()) > 0:
            timestamps = re.findall(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}", line)
            if timestamps:
                if not first_timestamp:
                    first_timestamp = timestamps[0]
                last_timestamp = timestamps[0]

    # Build summary
    summary = []
    summary.append(f"Log contains {total_lines} lines.")

    if first_timestamp and last_timestamp:
        summary.append(f"Time span: {first_timestamp} to {last_timestamp}")

    # Add error summary
    error_count = len(error_patterns)
    if error_count > 0:
        summary.append(f"Found {error_count} potential errors.")

        # Add category breakdown
        if error_categories:
            summary.append("Error categories:")
            for category, count in sorted(
                error_categories.items(), key=lambda x: x[1], reverse=True
            ):
                summary.append(f"  - {category.replace('_', ' ').title()}: {count}")
    else:
        summary.append("No significant errors detected.")

    # List a few example errors
    if error_patterns:
        summary.append("\nExample errors:")
        for i, error in enumerate(error_patterns[:3]):  # Show top 3 errors
            summary.append(f"  {i+1}. {error}")
        if len(error_patterns) > 3:
            summary.append(f"  ... and {len(error_patterns) - 3} more")

    return "\n".join(summary)

def determine_root_cause(analysis_results: Dict[str, Any]) -> str:
    """
    Determine most likely root cause based on analysis results.

    Args:
        analysis_results: Dictionary containing failed task analysis

    Returns:
        Human-readable root cause description
    """
    if "failed_tasks" not in analysis_results or not analysis_results["failed_tasks"]:
        return "Unknown - No failed tasks identified"

    # Combine error categories across all failed tasks
    all_categories: Dict[str, int] = {}
    for task in analysis_results["failed_tasks"]:
        for category, count in task.get("error_categories", {}).items():
            all_categories[category] = all_categories.get(category, 0) + count

    # Find the most common category
    if all_categories:
        most_common = max(all_categories.items(), key=lambda x: x[1])
        category = most_common[0]

        # Map category to more specific root causes
        # Kubernetes-specific categories
        if category == "oom":
            return "Out of Memory (OOMKilled) - container exceeded memory limits and was killed"
        elif category == "crash":
            return "Container crash loop (CrashLoopBackOff) - container is repeatedly crashing on startup"
        elif category == "image":
            # Verify this is an actual image pull failure, not just a log mentioning "image"
            # Check if error_patterns contain definitive image pull indicators
            actual_errors = []
            for task in analysis_results.get("failed_tasks", []):
                actual_errors.extend(task.get("error_patterns", []))
            errors_text = " ".join(actual_errors).lower()
            image_pull_indicators = [
                "imagepullbackoff",
                "errimagepull",
                "failed to pull image",
                "pull access denied",
            ]
            if any(ind in errors_text for ind in image_pull_indicators):
                return "Image pull failure (ImagePullBackOff) - unable to pull container image from registry"
            # If "image" matched from log context but errors point to a step/build failure, use step details
            all_failed_steps = []
            for task in analysis_results.get("failed_tasks", []):
                for step in task.get("failed_steps", []):
                    all_failed_steps.append(step)
            if all_failed_steps:
                step_details = []
                for step in all_failed_steps[:3]:
                    step_name = step.get("step_name", "unknown")
                    exit_code = step.get("exit_code", "?")
                    step_details.append(f"'{step_name}' (exit code {exit_code})")
                steps_str = ", ".join(step_details)
                first_error = actual_errors[0][:120] if actual_errors else ""
                context = f" - {first_error}" if first_error else ""
                return f"Task step failure - step {steps_str} exited with non-zero code{context}"
            return "Image-related error - check container image references and registry access"
        elif category == "scheduling":
            return "Pod scheduling failure - insufficient resources or node constraints preventing scheduling"
        elif category == "storage":
            return "Storage/volume issues - failed to mount volume or PVC problems"
        elif category == "config":
            return "Container configuration error - missing ConfigMap, Secret, or environment variable"
        # General categories
        elif category == "resource_limits":
            return "Resource constraint issues - the pipeline is likely hitting memory or CPU limits"
        elif category == "network":
            return "Network connectivity issues - check network policies and external dependencies"
        elif category == "permissions":
            return "Permission or authorization issues - check RBAC settings and service account permissions"
        elif category == "configuration":
            return "Configuration errors - check pipeline parameters and ConfigMaps"
        elif category == "dependency":
            return "Dependency issues - check for missing dependencies or version mismatches"
        elif category == "step_failures":
            # Build description from failed step details
            failed_step_details = []
            for task in analysis_results["failed_tasks"]:
                for step in task.get("failed_steps", []):
                    step_name = step.get("step_name", "unknown")
                    exit_code = step.get("exit_code", "?")
                    failed_step_details.append(f"'{step_name}' (exit code {exit_code})")
            if failed_step_details:
                steps_str = ", ".join(failed_step_details[:3])
                return f"Task step failure - step {steps_str} exited with non-zero code"
            return "Task step failure - one or more steps exited with non-zero code"
        elif category == "filesystem":
            return "Filesystem issues - check for missing files or storage problems"

    # Fallback: check for failed_steps even without categorized errors
    all_failed_steps = []
    for task in analysis_results["failed_tasks"]:
        for step in task.get("failed_steps", []):
            all_failed_steps.append(step)
    if all_failed_steps:
        step_details = []
        for step in all_failed_steps[:3]:
            step_name = step.get("step_name", "unknown")
            exit_code = step.get("exit_code", "?")
            reason = step.get("reason", "")
            detail = f"'{step_name}' (exit code {exit_code})"
            if reason:
                detail += f" [{reason}]"
            step_details.append(detail)
        steps_str = ", ".join(step_details)
        return f"Task step failure - step {steps_str} exited with non-zero code"

    # Check task messages for additional context
    task_messages = []
    for task in analysis_results["failed_tasks"]:
        msg = task.get("message", "")
        if msg:
            task_messages.append(msg)
    if task_messages:
        return f"Pipeline task failure: {task_messages[0][:150]}"

    return "Indeterminate - multiple potential causes"

def recommend_actions(analysis_results: Dict[str, Any]) -> List[str]:
    """
    Recommend actions based on analysis results.

    Args:
        analysis_results: Dictionary containing failed task analysis

    Returns:
        List of recommended actions
    """
    if "error" in analysis_results:
        return ["Fix connection or permission issues with Kubernetes API"]

    recommendations = []

    # Based on root cause, suggest appropriate actions
    root_cause = analysis_results.get("probable_root_cause", "").lower()

    # Kubernetes-specific root causes
    if "oomkilled" in root_cause or "out of memory" in root_cause:
        recommendations.extend(
            [
                "Increase memory limits for the affected container/task",
                "Check if the build process has memory leaks",
                "Consider splitting large tasks into smaller steps",
                "Review memory requests vs limits ratio",
                "Monitor memory usage during pipeline execution",
            ]
        )
    elif "crashloopbackoff" in root_cause or "crash loop" in root_cause:
        recommendations.extend(
            [
                "Check container logs for crash reason before restart",
                "Verify container entrypoint and command are correct",
                "Check if required dependencies are available at startup",
                "Review liveness/readiness probe configurations",
                "Check for race conditions in container initialization",
            ]
        )
    elif "imagepullbackoff" in root_cause or "image pull" in root_cause:
        recommendations.extend(
            [
                "Verify the container image exists in the registry",
                "Check image pull secrets are configured correctly",
                "Verify registry credentials are valid and not expired",
                "Check network access to the container registry",
                "Verify image tag is correct and available",
            ]
        )
    elif "scheduling" in root_cause:
        recommendations.extend(
            [
                "Check node resources - ensure sufficient CPU/memory available",
                "Review node selectors and affinity rules",
                "Check for taints on nodes that may prevent scheduling",
                "Verify resource quotas in the namespace",
                "Check if required node labels exist",
            ]
        )
    elif "storage" in root_cause or "volume" in root_cause:
        recommendations.extend(
            [
                "Check PVC status and bound PV availability",
                "Verify storage class exists and is default or specified",
                "Check if the storage provisioner is healthy",
                "Review volume mount paths for conflicts",
                "Check storage quota limits in the namespace",
            ]
        )
    elif "container configuration" in root_cause:
        recommendations.extend(
            [
                "Verify referenced ConfigMaps exist and have required keys",
                "Check Secrets are available and properly referenced",
                "Review environment variable definitions",
                "Check volume mounts for ConfigMaps/Secrets",
                "Verify container security context settings",
            ]
        )
    # General categories
    elif "resource constraint" in root_cause:
        recommendations.extend(
            [
                "Check resource quotas and limits in the namespace",
                "Consider increasing CPU/memory limits for affected pods",
                "Review resource requests/limits in PipelineRun and TaskRun specs",
                "Monitor cluster resource utilization during pipeline runs",
            ]
        )
    elif "network" in root_cause:
        recommendations.extend(
            [
                "Verify network policies allow necessary connections",
                "Check external dependencies are accessible from the cluster",
                "Review DNS configuration in the cluster",
                "Check for timeouts in build configurations",
            ]
        )
    elif "permission" in root_cause or "authorization" in root_cause:
        recommendations.extend(
            [
                "Review RBAC permissions for service accounts used by Tekton pipelines",
                "Check if appropriate ClusterRoles and RoleBindings are in place",
                "Verify service account tokens are mounted correctly",
                "Check for recent changes to RBAC policies",
            ]
        )
    elif "configuration" in root_cause:
        recommendations.extend(
            [
                "Check ConfigMaps and Secrets referenced by pipelines",
                "Verify pipeline parameters are correctly specified",
                "Review task definitions for correctness",
                "Check CI/CD pipeline configuration for inconsistencies",
            ]
        )
    elif "dependency" in root_cause:
        recommendations.extend(
            [
                "Check image versions in TaskRuns and PipelineRuns",
                "Verify external dependencies are available",
                "Update task definitions if using deprecated features",
                "Check for version mismatches between components",
            ]
        )
    elif "filesystem" in root_cause:
        recommendations.extend(
            [
                "Check persistent volume claims and storage classes",
                "Verify file paths in task specifications",
                "Check if required files exist in workspace volumes",
                "Review storage provisioner logs",
            ]
        )
    elif "step failure" in root_cause or "task step" in root_cause:
        recommendations.extend(
            [
                "Check the failed step's output for specific error details",
                "Review the task definition for the failing step",
                "Verify input parameters and workspace contents are correct",
                "Compare with previous successful runs of the same pipeline",
            ]
        )
        # Add step-specific recommendations
        for task in analysis_results.get("failed_tasks", []):
            for step in task.get("failed_steps", []):
                step_name = step.get("step_name", "")
                if step_name:
                    recommendations.append(
                        f"Investigate step '{step_name}' - check its script/command logic"
                    )
    else:
        # Generic recommendations when root cause is unclear
        recommendations.extend(
            [
                "Review complete logs of failed tasks",
                "Check recent changes to pipeline definitions",
                "Compare with previous successful runs",
                "Review cluster events for relevant warnings or errors",
                "Check health of Tekton controller components",
            ]
        )

    # Add specific task-related recommendations
    failed_tasks = analysis_results.get("failed_tasks", [])
    if failed_tasks:
        task_names = [task.get("task_name") for task in failed_tasks]
        recommendations.append(
            f"Focus investigation on failed tasks: {', '.join(task_names)}"
        )

    return recommendations




def clean_etcd_logs(raw_logs: str) -> str:
    """
    Clean etcd logs by removing escape characters and properly formatting JSON log entries.

    This function handles the common issues with etcd logs fetched from Kubernetes:
    1. Multiple levels of JSON escaping (\\" becomes ")
    2. Escaped newlines (\\n becomes actual newlines)
    3. Duplicate timestamps (Kubernetes timestamp + etcd timestamp)
    4. Malformed JSON structure

    Args:
        raw_logs (str): Raw log content from Kubernetes API

    Returns:
        str: Cleaned and formatted log content
    """
    import json as _json
    import re

    if not raw_logs or raw_logs.strip() == "":
        return raw_logs

    try:
        lines = raw_logs.strip().split("\n")
        cleaned_lines = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.startswith(("ERROR:", "INFO:")):
                cleaned_lines.append(line)
                continue

            try:
                timestamp_match = re.match(
                    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)\s+(.*)$", line
                )

                if timestamp_match:
                    k8s_timestamp = timestamp_match.group(1)
                    json_part = timestamp_match.group(2)

                    json_part = json_part.replace('\\\\"', '"')
                    json_part = json_part.replace("\\n", "\n")
                    json_part = json_part.replace("\\/", "/")
                    json_part = json_part.replace("\\t", "\t")
                    json_part = json_part.replace("\\r", "\r")
                    json_part = json_part.replace("\\\\", "\\")

                    try:
                        json_obj = _json.loads(json_part)
                        level = json_obj.get("level", "unknown")
                        etcd_timestamp = json_obj.get("ts", "")
                        caller = json_obj.get("caller", "")
                        msg = json_obj.get("msg", "")
                        timestamp_to_use = (
                            etcd_timestamp if etcd_timestamp else k8s_timestamp
                        )
                        formatted_parts = []
                        if timestamp_to_use:
                            formatted_parts.append(f"[{timestamp_to_use}]")
                        if level:
                            formatted_parts.append(f"[{level.upper()}]")
                        if caller:
                            formatted_parts.append(f"[{caller}]")
                        if msg:
                            formatted_parts.append(msg)
                        for key, value in json_obj.items():
                            if (
                                key not in ["level", "ts", "caller", "msg"]
                                and value is not None
                            ):
                                if isinstance(value, (str, int, float, bool)):
                                    formatted_parts.append(f"{key}={value}")
                                else:
                                    formatted_parts.append(
                                        f"{key}={_json.dumps(value)}"
                                    )
                        cleaned_lines.append(" ".join(formatted_parts))
                    except _json.JSONDecodeError:
                        cleaned_line = json_part.replace('\\"', '"').replace(
                            "\\n", "\n"
                        )
                        if k8s_timestamp:
                            cleaned_line = f"[{k8s_timestamp}] {cleaned_line}"
                        cleaned_lines.append(cleaned_line)
                else:
                    cleaned_line = (
                        line.replace('\\\\"', '"')
                        .replace("\\n", "\n")
                        .replace("\\/", "/")
                        .replace("\\t", "\t")
                        .replace("\\r", "\r")
                        .replace("\\\\", "\\")
                    )
                    if cleaned_line.startswith("{") and cleaned_line.endswith("}"):
                        try:
                            json_obj = _json.loads(cleaned_line)
                            level = json_obj.get("level", "unknown")
                            timestamp = json_obj.get("ts", "")
                            caller = json_obj.get("caller", "")
                            msg = json_obj.get("msg", "")
                            formatted_parts = []
                            if timestamp:
                                formatted_parts.append(f"[{timestamp}]")
                            if level:
                                formatted_parts.append(f"[{level.upper()}]")
                            if caller:
                                formatted_parts.append(f"[{caller}]")
                            if msg:
                                formatted_parts.append(msg)
                            cleaned_lines.append(" ".join(formatted_parts))
                        except _json.JSONDecodeError:
                            cleaned_lines.append(cleaned_line)
                    else:
                        cleaned_lines.append(cleaned_line)
            except Exception as e:
                _utils_logger.debug(f"Failed to process log line: {e}")
                cleaned_lines.append(f"[UNPARSED] {line}")

        result = "\n".join(cleaned_lines)
        result = re.sub(r"\n\s*\n", "\n", result)
        result = re.sub(r" +", " ", result)
        return result.strip()

    except Exception as e:
        _utils_logger.error(f"Error cleaning etcd logs: {e}")
        return raw_logs

def _handle_api_exception(
    e: Any,
    tool_name: str,
    strategy: str,
    namespace: str,
    label_selector: str,
    results_dict: Dict,
) -> None:
    """Helper function to handle Kubernetes API exceptions consistently."""
    strategy_lower = strategy.lower()

    if e.status == 404:
        _utils_logger.warning(
            f"[{tool_name}] {strategy} strategy: 404 Not Found - namespace '{namespace}' or resources not found"
        )
        results_dict[f"info_{strategy_lower}_404"] = (
            f"Namespace '{namespace}' or pods with label '{label_selector}' not found"
        )
    elif e.status == 403:
        _utils_logger.warning(
            f"[{tool_name}] {strategy} strategy: 403 Forbidden - insufficient RBAC permissions"
        )
        results_dict[f"error_{strategy_lower}_403"] = (
            f"Insufficient permissions for namespace '{namespace}'. "
            f"Required: pods/list, pods/log permissions"
        )
    elif e.status == 401:
        _utils_logger.error(
            f"[{tool_name}] {strategy} strategy: 401 Unauthorized - authentication failed"
        )
        results_dict[f"error_{strategy_lower}_401"] = (
            "Authentication failed. Check kubeconfig and credentials"
        )
    else:
        _utils_logger.error(
            f"[{tool_name}] {strategy} strategy: API error {e.status} - {e.reason}"
        )
        results_dict[f"error_{strategy_lower}_api"] = (
            f"API error {e.status}: {e.reason}"
        )

async def _get_logs_with_k8s_client(
    k8s_core_api: Any,
    pod_names: List,
    namespace: str,
    container_name: str,
    target_logs_dict: Dict,
    log_params: Dict,
) -> bool:
    """
    Enhanced helper to fetch logs for a list of pod names with flexible time and line filtering.

    Args:
        k8s_core_api: Initialized CoreV1Api client
        pod_names: List of pod names to fetch logs from
        namespace: Namespace of the pods
        container_name: Name of the container within the pods
        target_logs_dict: Dictionary to populate with logs or error messages
        log_params: Dictionary containing log retrieval parameters

    Returns:
        bool: True if logs were successfully fetched for at least one pod
    """
    import asyncio

    try:
        from kubernetes.client.rest import ApiException
    except ImportError:
        ApiException = Exception

    _utils_logger.debug(
        f"Fetching logs for {len(pod_names)} pods in namespace '{namespace}', container '{container_name}'"
    )
    at_least_one_log_fetched = False

    for pod_name in pod_names:
        _utils_logger.info(
            f"Fetching logs for pod '{pod_name}' with params: {log_params}"
        )
        try:
            log_kwargs = {
                "name": pod_name,
                "namespace": namespace,
                "container": container_name,
                "timestamps": log_params.get("timestamps", True),
                "follow": log_params.get("follow", False),
                "previous": log_params.get("previous", False),
            }
            if log_params.get("since_time"):
                log_kwargs["since"] = log_params["since_time"]
            elif log_params.get("since_seconds"):
                log_kwargs["since_seconds"] = log_params["since_seconds"]
            elif log_params.get("tail_lines"):
                log_kwargs["tail_lines"] = log_params["tail_lines"]
            log_kwargs = {k: v for k, v in log_kwargs.items() if v is not None}

            log_content = await asyncio.to_thread(
                k8s_core_api.read_namespaced_pod_log, **log_kwargs
            )

            if log_content:
                if (
                    container_name == "etcd"
                    and (
                        "etcd" in pod_name.lower()
                        or namespace in ["openshift-etcd", "kube-system"]
                    )
                    and log_params.get("clean_logs", True)
                ):
                    cleaned_content = clean_etcd_logs(log_content)
                    target_logs_dict[pod_name] = cleaned_content
                    _utils_logger.info(
                        f"Successfully fetched and cleaned {len(cleaned_content)} characters of etcd logs for pod '{pod_name}'"
                    )
                else:
                    target_logs_dict[pod_name] = log_content
                    _utils_logger.info(
                        f"Successfully fetched {len(log_content)} characters of logs for pod '{pod_name}'"
                    )
                at_least_one_log_fetched = True
            else:
                target_logs_dict[pod_name] = (
                    "INFO: No logs available for the specified time period/criteria"
                )
                _utils_logger.info(
                    f"No logs found for pod '{pod_name}' with current criteria"
                )

        except ApiException as e:
            error_message = (
                f"API error fetching logs for pod '{pod_name}': {e.status} - {e.reason}"
            )
            if hasattr(e, "body") and e.body:
                error_message += f" | Details: {str(e.body)[:200]}"
            _utils_logger.warning(error_message)
            target_logs_dict[pod_name] = f"ERROR: {error_message}"
        except Exception as e:
            error_message = (
                f"Unexpected error fetching logs for pod '{pod_name}': {str(e)}"
            )
            _utils_logger.error(error_message, exc_info=True)
            target_logs_dict[pod_name] = f"ERROR: {error_message}"

    return at_least_one_log_fetched

def _filter_logs_by_time_range(logs: str, until_time: Any) -> str:
    """
    Filter log lines to only include entries before the specified until_time.

    Args:
        logs: Raw log content with timestamps
        until_time: Maximum timestamp (timezone-aware datetime)

    Returns:
        Filtered log content
    """
    from datetime import datetime

    if not logs or not until_time:
        return logs

    filtered_lines = []
    for line in logs.split("\n"):
        if not line.strip():
            continue
        try:
            timestamp_match = line.split()[0] if line else None
            if timestamp_match:
                if "T" in timestamp_match:
                    log_time = datetime.fromisoformat(
                        timestamp_match.replace("Z", "+00:00")
                    )
                else:
                    try:
                        parts = line.split()
                        if len(parts) >= 2:
                            datetime_str = f"{parts[0]} {parts[1]}"
                            log_time = datetime.fromisoformat(datetime_str)
                        else:
                            continue
                    except Exception:
                        continue
                if log_time <= until_time:
                    filtered_lines.append(line)
                else:
                    break
            else:
                filtered_lines.append(line)
        except (ValueError, IndexError):
            filtered_lines.append(line)

    return "\n".join(filtered_lines)
