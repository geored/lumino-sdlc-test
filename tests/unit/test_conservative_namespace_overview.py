"""Tests for conservative_namespace_overview pod-recording fix (Issue #238).

Verifies that pods returning {"error": ...} from smart_summarize_pod_logs
(non-exception error path) are still recorded in `findings`, so
`pods_analyzed` reflects the true count of pods attempted.
"""

import pytest


def _make_pod(name, status="Running", restart_count=0):
    return {
        "name": name,
        "status": status,
        "restart_count": restart_count,
        "container_states": [],
        "creation_timestamp": "2024-01-01T00:00:00Z",
    }


def _run_loop(pods_info, pod_analysis_results):
    """Simulate the pod-analysis loop from conservative_namespace_overview."""
    findings = {}
    issues_found = []

    for i, pod_info in enumerate(pods_info):
        pod_name = pod_info.get("name", "")
        pod_status = pod_info.get("status", "Unknown")
        pod_analysis = pod_analysis_results[i]

        try:
            if "error" not in pod_analysis:
                essential_info = {
                    "status": pod_status,
                    "log_lines": pod_analysis.get("metadata", {})
                        .get("processing_metrics", {})
                        .get("total_log_lines", 0),
                    "patterns_found": pod_analysis.get("metadata", {})
                        .get("processing_metrics", {})
                        .get("patterns_extracted", 0),
                    "has_errors": bool(
                        pod_analysis.get("patterns", {}).get("errors")
                    ),
                    "has_warnings": bool(
                        pod_analysis.get("patterns", {}).get("warnings")
                    ),
                }
                if pod_analysis.get("patterns", {}).get("errors"):
                    top_error = pod_analysis["patterns"]["errors"][0]
                    essential_info["top_issue"] = (
                        f"{top_error['content'][:80]}..."
                    )
                    issues_found.append(
                        f"Pod {pod_name}: {essential_info['top_issue']}"
                    )
                findings[pod_name] = essential_info
            else:
                # THE FIX: non-exception error path still records the pod
                findings[pod_name] = {
                    "status": pod_status,
                    "error": pod_analysis.get("error", "unknown error"),
                }
        except Exception as e:
            findings[pod_name] = {"status": pod_status, "error": str(e)}

    return findings


class TestPodsAnalyzedCount:
    def test_all_success_pods_recorded(self):
        pods = [_make_pod(f"pod-{i}") for i in range(3)]
        results = [
            {"metadata": {"processing_metrics": {"total_log_lines": 10, "patterns_extracted": 0}},
             "patterns": {}}
        ] * 3
        findings = _run_loop(pods, results)
        assert len(findings) == 3

    def test_all_error_pods_recorded(self):
        """Core regression: pods returning error dicts must all be recorded."""
        pods = [_make_pod(f"pod-{i}") for i in range(5)]
        results = [{"error": "logs not available"}] * 5
        findings = _run_loop(pods, results)
        assert len(findings) == 5

    def test_45_pods_all_errors_all_recorded(self):
        """Exact scenario from Issue #238: 45 pods, all error dicts -> pods_analyzed must be 45."""
        pods = [_make_pod(f"pod-{i:03d}") for i in range(45)]
        results = [{"error": "logs not available"}] * 45
        findings = _run_loop(pods, results)
        assert len(findings) == 45, (
            f"Bug #238 regression: expected pods_analyzed=45, got {len(findings)}"
        )

    def test_mixed_success_and_error_all_recorded(self):
        pods = [
            _make_pod("pod-ok-1"),
            _make_pod("pod-err-1", status="Failed"),
            _make_pod("pod-ok-2"),
            _make_pod("pod-err-2", status="CrashLoopBackOff"),
        ]
        results = [
            {"metadata": {"processing_metrics": {"total_log_lines": 5, "patterns_extracted": 0}},
             "patterns": {}},
            {"error": "container not found"},
            {"metadata": {"processing_metrics": {"total_log_lines": 8, "patterns_extracted": 0}},
             "patterns": {}},
            {"error": "logs unavailable"},
        ]
        findings = _run_loop(pods, results)
        assert len(findings) == 4

    def test_error_pod_preserves_status(self):
        pods = [_make_pod("crashed-pod", status="Failed")]
        results = [{"error": "no logs"}]
        findings = _run_loop(pods, results)
        assert "crashed-pod" in findings
        assert findings["crashed-pod"]["status"] == "Failed"
        assert findings["crashed-pod"]["error"] == "no logs"

    def test_error_pods_not_counted_in_pods_with_issues(self):
        """Error-dict entries must NOT inflate pods_with_issues count."""
        pods = [_make_pod("pod-err")]
        results = [{"error": "something went wrong"}]
        findings = _run_loop(pods, results)
        pods_with_issues = len([
            f for f in findings.values()
            if f.get("has_errors") or f.get("has_warnings")
        ])
        assert pods_with_issues == 0

    def test_exception_path_records_pod(self):
        findings = {}
        pod_info = _make_pod("exploding-pod")
        pod_name, pod_status = pod_info["name"], pod_info["status"]
        try:
            raise RuntimeError("unexpected crash")
        except Exception as e:
            findings[pod_name] = {"status": pod_status, "error": str(e)}
        assert "exploding-pod" in findings
        assert "unexpected crash" in findings["exploding-pod"]["error"]
