"""Predictive log analysis tools — extracted from server-mcp.py.

Contains ``predictive_log_analyzer_impl``, the core implementation called by
the ``@mcp.tool()`` wrapper that remains in ``server-mcp.py``.

Fixes #160
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import numpy as np
from kubernetes.client.rest import ApiException

from helpers import (
    analyze_log_patterns_for_failure_prediction,
    extract_log_features,
    generate_failure_predictions,
    preprocess_log_data,
    train_anomaly_model,
    train_or_load_model,
)

logger = logging.getLogger("lumino-mcp-server")


async def predictive_log_analyzer_impl(
    prediction_window: str,
    confidence_threshold: float,
    log_sources: Optional[List[str]],
    namespaces: Optional[List[str]],
    max_namespaces: int,
    force_retrain: bool,
    *,
    k8s_core_api: Any,
    logger: Any,
    list_namespaces: Callable,
    detect_tekton_namespaces: Callable,
    get_namespace_events_as_dicts_impl: Callable,
) -> Dict[str, Any]:
    """
    Core implementation for predictive log analysis.

    Args:
        prediction_window: Time window - "1h", "6h", "24h", "7d".
        confidence_threshold: Min confidence for predictions 0.0-1.0.
        log_sources: Sources to analyze - pods, services, nodes.
        namespaces: Specific namespaces to analyze (None = auto-detect).
        max_namespaces: Maximum namespaces to scan when auto-detecting.
        force_retrain: Force model retraining even if cached model is valid.
        k8s_core_api: Injected Kubernetes CoreV1Api client.
        logger: Injected logger instance.
        list_namespaces: Injected async helper that returns all namespace names.
        detect_tekton_namespaces: Injected async helper for Tekton namespace detection.
        get_namespace_events_as_dicts_impl: Injected async helper for fetching events.

    Returns:
        Dict with keys: predictions, model_performance, anomaly_scores,
        trend_analysis, model_info.
    """
    if not k8s_core_api:
        return {"error": "Kubernetes client not available."}
    try:
        logger.info(
            f"Starting predictive log analysis with window: {prediction_window}, threshold: {confidence_threshold}"
        )

        valid_windows = ["1h", "6h", "24h", "7d"]
        if prediction_window not in valid_windows:
            raise ValueError(
                f"Invalid prediction_window. Must be one of: {valid_windows}"
            )

        if not 0.0 <= confidence_threshold <= 1.0:
            raise ValueError("confidence_threshold must be between 0.0 and 1.0")

        try:
            from helpers.ml_persistence import (
                FailureEventCollector,
                ModelPersistenceManager,
                ModelVersionManager,
                TrainingDataStore,
                build_labels_from_correlations,
            )

            model_manager = ModelPersistenceManager()
            training_store = TrainingDataStore()
            failure_collector = FailureEventCollector(training_store)
            version_manager = ModelVersionManager(model_manager, training_store)
            persistence_available = True
        except Exception as e:
            logger.warning(
                f"ML persistence not available, using ephemeral training: {e}"
            )
            persistence_available = False
            model_manager = None
            training_store = None
            failure_collector = None
            version_manager = None

        result = {
            "predictions": [],
            "model_performance": {
                "accuracy": 0.0,
                "precision": 0.0,
                "recall": 0.0,
                "last_training_time": datetime.now().isoformat(),
            },
            "anomaly_scores": [],
            "trend_analysis": {
                "error_rate_trend": "stable",
                "resource_trend": "stable",
                "performance_trend": "stable",
            },
            "model_info": {
                "model_id": None,
                "loaded_from_cache": False,
                "training_samples": 0,
                "has_failure_labels": False,
                "persistence_enabled": persistence_available,
            },
        }

        window_to_seconds = {"1h": 3600, "6h": 21600, "24h": 86400, "7d": 604800}
        window_seconds = window_to_seconds.get(prediction_window, 21600)

        log_sources = log_sources or ["pods", "services", "nodes"]
        all_logs = []
        target_namespaces = []

        for source in log_sources:
            try:
                if source == "pods":
                    if namespaces:
                        target_namespaces = namespaces
                        logger.info(
                            f"Using user-specified namespaces: {target_namespaces}"
                        )
                    else:
                        all_ns = await list_namespaces()
                        try:
                            tekton_ns = await detect_tekton_namespaces()
                            active_ns = []
                            for category in tekton_ns.values():
                                active_ns.extend(category)
                            target_namespaces = (
                                list(set(active_ns))[:max_namespaces]
                                if active_ns
                                else all_ns[:max_namespaces]
                            )
                        except Exception:
                            target_namespaces = all_ns[:max_namespaces]
                        logger.info(
                            f"Auto-detected {len(target_namespaces)} active namespaces"
                        )

                    for ns in target_namespaces:
                        try:
                            pods = await asyncio.to_thread(
                                k8s_core_api.list_namespaced_pod, namespace=ns, limit=50
                            )
                            for pod in pods.items:
                                if pod.status.phase in [
                                    "Running",
                                    "Failed",
                                    "Succeeded",
                                ]:
                                    try:
                                        pod_logs = await asyncio.to_thread(
                                            k8s_core_api.read_namespaced_pod_log,
                                            name=pod.metadata.name,
                                            namespace=ns,
                                            since_seconds=window_seconds,
                                        )
                                        all_logs.extend(pod_logs.split("\n"))
                                    except ApiException:
                                        continue
                        except ApiException:
                            continue

            except Exception as e:
                logger.warning(f"Failed to collect logs from {source}: {str(e)}")
                continue

        if not all_logs:
            logger.warning(
                "No logs collected from any source - returning insufficient data"
            )
            result["trend_analysis"]["error_rate_trend"] = "no_data"
            result["model_performance"] = {
                "accuracy": None,
                "precision": None,
                "recall": None,
                "note": "No log data available for analysis",
            }
            return result

        all_logs = [log for log in all_logs if log.strip()]

        if len(all_logs) < 10:
            logger.warning(f"Insufficient log data for analysis: {len(all_logs)} lines")
            result["trend_analysis"]["error_rate_trend"] = "insufficient_data"
            return result

        logger.info(f"Analyzing {len(all_logs)} log lines for predictive patterns")

        log_df = preprocess_log_data(all_logs)
        features = extract_log_features(log_df)

        labels = None
        if persistence_available and failure_collector and training_store:
            try:
                for ns in target_namespaces:
                    try:
                        events_as_dicts = await get_namespace_events_as_dicts_impl(
                            ns, limit=100,
                            k8s_core_api=k8s_core_api,
                        )
                        if events_as_dicts:
                            count = failure_collector.collect_from_events(
                                events_as_dicts, ns
                            )
                            logger.debug(
                                f"Collected {count} failure labels from events in {ns}"
                            )

                        pods = await asyncio.to_thread(
                            k8s_core_api.list_namespaced_pod, namespace=ns, limit=50
                        )
                        failure_collector.collect_from_pod_status(pods.items, ns)
                    except Exception as e:
                        logger.debug(f"Failed to collect failure events from {ns}: {e}")

                historical_failures = training_store.get_failure_labels_in_window(
                    start_time=datetime.now() - timedelta(hours=2),
                    end_time=datetime.now(),
                )

                stored_samples = []
                for idx, row in log_df.iterrows():
                    if idx < 500:
                        sample_data = {
                            "timestamp": row.get("timestamp"),
                            "namespace": (
                                target_namespaces[0] if target_namespaces else "unknown"
                            ),
                            "features": (
                                features[idx].tolist() if idx < len(features) else []
                            ),
                            "raw_message": str(row.get("raw_message", ""))[:500],
                            "log_level": row.get("log_level"),
                            "error_indicators": int(row.get("error_indicators", 0)),
                            "message_entropy": float(row.get("message_entropy", 0.0)),
                        }
                        sample_id = training_store.store_log_sample(sample_data)
                        if sample_id:
                            stored_samples.append(
                                {
                                    "id": sample_id,
                                    "timestamp": sample_data["timestamp"],
                                    "namespace": sample_data["namespace"],
                                }
                            )

                if historical_failures and stored_samples:
                    correlations = failure_collector.correlate_logs_with_failures(
                        stored_samples, historical_failures, time_window_minutes=30
                    )
                    if correlations:
                        labels = build_labels_from_correlations(
                            correlations, len(log_df)
                        )
                        logger.info(
                            f"Created {len(correlations)} log-failure correlations"
                        )

            except Exception as e:
                logger.warning(f"Failed to collect/correlate failure events: {e}")

        if persistence_available and model_manager and version_manager:
            try:
                anomaly_model, model_id, model_metadata = train_or_load_model(
                    features=features,
                    model_manager=model_manager,
                    version_manager=version_manager,
                    labels=labels,
                    force_retrain=force_retrain,
                )

                result["model_info"].update(
                    {
                        "model_id": model_id,
                        "loaded_from_cache": model_metadata.get(
                            "loaded_from_cache", False
                        ),
                        "training_samples": model_metadata.get(
                            "training_samples", len(features)
                        ),
                        "has_failure_labels": labels is not None and len(labels) > 0,
                        "created_at": model_metadata.get("created_at"),
                    }
                )

                perf = model_metadata.get("performance_metrics", {})
                if perf:
                    result["model_performance"].update(
                        {
                            "accuracy": perf.get("accuracy", 0.0),
                            "precision": perf.get("precision", 0.0),
                            "recall": perf.get("recall", 0.0),
                            "last_training_time": model_metadata.get(
                                "created_at", datetime.now().isoformat()
                            ),
                        }
                    )
            except Exception as e:
                logger.warning(
                    f"Persistence-based training failed, falling back to ephemeral: {e}"
                )
                anomaly_model = train_anomaly_model(features)
        else:
            anomaly_model = train_anomaly_model(features)

        anomaly_scores = anomaly_model.decision_function(features)
        anomaly_predictions = anomaly_model.predict(features)

        if result["model_performance"]["accuracy"] == 0.0:
            normal_predictions = anomaly_predictions == 1
            accuracy = (
                np.mean(normal_predictions) if len(normal_predictions) > 0 else 0.0
            )
            result["model_performance"].update(
                {
                    "accuracy": float(accuracy),
                    "precision": None,
                    "recall": None,
                    "note": "Precision/recall require labeled validation data - not available",
                }
            )

        if target_namespaces:
            lines_per_ns = max(1, len(anomaly_scores) // max(1, len(target_namespaces)))
            threshold = -0.5

            for i, ns in enumerate(
                target_namespaces[: min(10, len(target_namespaces))]
            ):
                start_idx = i * lines_per_ns
                end_idx = min(start_idx + lines_per_ns, len(anomaly_scores))
                if start_idx < len(anomaly_scores):
                    ns_scores = anomaly_scores[start_idx:end_idx]
                    mean_score = float(np.mean(ns_scores))
                    anomalous_lines = int(np.sum(ns_scores < threshold))
                    status = "anomalous" if mean_score < threshold else "normal"

                    result["anomaly_scores"].append(
                        {
                            "component": ns,
                            "score": mean_score,
                            "threshold": threshold,
                            "status": status,
                            "anomalous_log_lines": anomalous_lines,
                            "total_log_lines": len(ns_scores),
                        }
                    )

        historical_failures_for_analysis = []
        if persistence_available and training_store:
            try:
                historical_failures_for_analysis = (
                    training_store.get_failure_labels_in_window(
                        start_time=datetime.now() - timedelta(hours=24),
                        end_time=datetime.now(),
                    )
                )
            except Exception as e:
                logger.debug(f"Could not retrieve historical failures: {e}")

        pattern_analysis = analyze_log_patterns_for_failure_prediction(
            log_df, historical_failures_for_analysis
        )

        predictions = generate_failure_predictions(
            pattern_analysis,
            confidence_threshold,
            prediction_window,
            historical_failures=historical_failures_for_analysis,
            labels=labels,
        )
        result["predictions"] = predictions

        error_logs = log_df[log_df["log_level"].isin(["ERROR", "FATAL", "PANIC"])]
        error_rate = len(error_logs) / len(log_df) if len(log_df) > 0 else 0.0

        if error_rate > 0.15:
            result["trend_analysis"]["error_rate_trend"] = "increasing"
        elif error_rate < 0.05:
            result["trend_analysis"]["error_rate_trend"] = "decreasing"
        else:
            result["trend_analysis"]["error_rate_trend"] = "stable"

        resource_indicators = (
            log_df["raw_message"]
            .str.contains(r"memory|cpu|disk|storage|resource", case=False, na=False)
            .sum()
        )

        if resource_indicators > len(log_df) * 0.1:
            result["trend_analysis"]["resource_trend"] = "concerning"
        else:
            result["trend_analysis"]["resource_trend"] = "stable"

        performance_indicators = (
            log_df["raw_message"]
            .str.contains(
                r"timeout|slow|latency|performance|delay", case=False, na=False
            )
            .sum()
        )

        if performance_indicators > len(log_df) * 0.08:
            result["trend_analysis"]["performance_trend"] = "degrading"
        else:
            result["trend_analysis"]["performance_trend"] = "stable"

        result["model_info"]["has_failure_labels"] = (
            labels is not None and len(labels) > 0
        ) or (
            historical_failures_for_analysis
            and len(historical_failures_for_analysis) > 0
        )

        logger.info(
            f"Predictive analysis complete: {len(predictions)} predictions generated"
        )
        return result

    except Exception as e:
        logger.error(f"Error in predictive log analysis: {str(e)}", exc_info=True)
        return {
            "predictions": [],
            "model_performance": {
                "accuracy": 0.0,
                "precision": 0.0,
                "recall": 0.0,
                "last_training_time": datetime.now().isoformat(),
            },
            "anomaly_scores": [],
            "trend_analysis": {
                "error_rate_trend": "error",
                "resource_trend": "error",
                "performance_trend": "error",
            },
            "model_info": {
                "model_id": None,
                "loaded_from_cache": False,
                "training_samples": 0,
                "has_failure_labels": False,
                "persistence_enabled": False,
            },
            "error": str(e),
        }
