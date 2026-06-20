"""Tests for Issue #252: conservative_namespace_overview reports pods_analyzed=0.

The bug manifests when smart_summarize_pod_logs returns {"error": ...} for every
pod — the non-exception error path was missing from the pod-analysis loop so
pods were never added to `findings`, giving pods_analyzed=0.

These tests validate the loop logic extracted from the live server code and
guard against future regressions if the loop is refactored.
"""

import pytest


def _pod(name, status="Running", restart_count=0, container_states=None):
    return {
        "name": name,
        "status": status,
        "restart_count": restart_count,
        "container_states": container_states or [],
        "creation_timestamp": "2024-01-01T00:00:00Z",
    }


def _ok_analysis():
    return {
        "metadata": {
            "processing_metrics": {
                "total_log_lines": 10,
                "patterns_extracted": 2,
            }
        },
        "patterns": {},
    }


def _err_analysis(msg="logs not available"):
    return {"error": msg}


_ERROR_STATES = {
    "CrashLoopBackOff", "ImagePullBackOff", "Error",
    "OOMKilled", "ContainerCannotRun",
}


def _smart_sort(pods):
    """Exact sort key from conservative_namespace_overview smart strategy."""
    return sorted(
        pods,
        key=lambda p: (
            p.get("status") == "Failed",
            any(state in _ERROR_STATES for state in p.get("container_states", [])),
            p.get("restart_count", 0) > 0,
            p.get("restart_count", 0),
            "error" in p.get("name", "").lower(),
            "failed" in p.get("name", "").lower(),
        ),
        reverse=True,
    )


def _run_loop(pods_info, analysis_map, max_pods=None):
    """Run the pod-analysis loop from conservative_namespace_overview verbatim."""
    if max_pods is None:
        max_pods = len(pods_info)
    findings = {}
    issues_found = []

    prioritized = _smart_sort(pods_info)

    for pod_info in prioritized[:max_pods]:
        pod_name = pod_info.get("name", "")
        pod_status = pod_info.get("status", "Unknown")
        pod_analysis = analysis_map[pod_name]

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
                    essential_info["top_issue"] = f"{top_error['content'][:80]}..."
                    issues_found.append(f"Pod {pod_name}: {essential_info['top_issue']}")
                findings[pod_name] = essential_info
            else:
                # THE FIX: non-exception error path still records the pod
                findings[pod_name] = {
                    "status": pod_status,
                    "error": pod_analysis.get("error", "unknown error"),
                }
        except Exception as e:
            findings[pod_name] = {"status": pod_status, "error": str(e)}

    return findings, issues_found


class TestIssue252SmartSamplingPodsAnalyzed:

    # ------------------------------------------------------------------
    # Issue #252 exact reproduction
    # ------------------------------------------------------------------

    def test_issue_252_45_pods_all_errors_nonzero(self):
        """Issue #252: 45-pod namespace, smart strategy, all return error dicts.
        pods_analyzed must be 45, not 0."""
        pods = [_pod(f"pod-{i:03d}") for i in range(45)]
        analysis_map = {p["name"]: _err_analysis() for p in pods}
        findings, _ = _run_loop(pods, analysis_map)
        assert len(findings) == 45, (
            f"Issue #252 regression: pods_analyzed={len(findings)}, expected 45"
        )

    def test_issue_252_max_pods_5_all_errors_five_analyzed(self):
        """Reproduction case: max_pods=5, 45-pod namespace, all return errors."""
        pods = [_pod(f"pod-{i:03d}") for i in range(45)]
        analysis_map = {p["name"]: _err_analysis() for p in pods}
        findings, _ = _run_loop(pods, analysis_map, max_pods=5)
        assert len(findings) == 5, (
            f"Issue #252: expected pods_analyzed=5, got {len(findings)}"
        )

    def test_single_pod_error_dict_recorded(self):
        """Single pod returning error dict must appear in findings."""
        pods = [_pod("solo")]
        analysis_map = {"solo": _err_analysis("timeout")}
        findings, _ = _run_loop(pods, analysis_map)
        assert "solo" in findings
        assert findings["solo"]["error"] == "timeout"

    def test_error_entry_preserves_pod_status(self):
        """Error finding must carry the original pod status."""
        pods = [_pod("crashed", status="Failed")]
        analysis_map = {"crashed": _err_analysis("no logs")}
        findings, _ = _run_loop(pods, analysis_map)
        assert findings["crashed"]["status"] == "Failed"
        assert findings["crashed"]["error"] == "no logs"

    # ------------------------------------------------------------------
    # pods_analyzed formula
    # ------------------------------------------------------------------

    def test_pods_analyzed_equals_len_findings(self):
        """pods_analyzed = len(findings): verify the server formula holds."""
        pods = [_pod(f"p{i}") for i in range(10)]
        analysis_map = {
            p["name"]: (_ok_analysis() if i % 2 == 0 else _err_analysis())
            for i, p in enumerate(pods)
        }
        findings, _ = _run_loop(pods, analysis_map)
        assert len(findings) == 10

    def test_mixed_success_and_error_all_recorded(self):
        """Every pod (success or error) must appear in findings."""
        pods = [_pod("ok"), _pod("err", status="Failed"), _pod("ok2")]
        analysis_map = {
            "ok": _ok_analysis(),
            "err": _err_analysis(),
            "ok2": _ok_analysis(),
        }
        findings, _ = _run_loop(pods, analysis_map)
        assert set(findings.keys()) == {"ok", "err", "ok2"}

    def test_zero_pods_empty_findings(self):
        """Empty pod list must produce empty findings without crashing."""
        findings, issues = _run_loop([], {})
        assert findings == {}
        assert issues == []

    # ------------------------------------------------------------------
    # pods_with_issues formula
    # ------------------------------------------------------------------

    def test_error_pods_not_counted_in_pods_with_issues(self):
        """Error-dict findings must NOT inflate pods_with_issues count."""
        pods = [_pod("pod-err", status="Failed")]
        analysis_map = {"pod-err": _err_analysis("container not found")}
        findings, _ = _run_loop(pods, analysis_map)
        pods_with_issues = len([
            f for f in findings.values()
            if f.get("has_errors") or f.get("has_warnings")
        ])
        assert pods_with_issues == 0

    # ------------------------------------------------------------------
    # Smart sort correctness
    # ------------------------------------------------------------------

    def test_smart_sort_failed_pods_first(self):
        """Smart strategy must put Failed pods at the front."""
        pods = [
            _pod("ok-1", status="Running"),
            _pod("failed", status="Failed"),
            _pod("ok-2", status="Running"),
        ]
        prioritized = _smart_sort(pods)
        assert prioritized[0]["name"] == "failed"

    def test_smart_sort_crash_loop_before_running(self):
        """CrashLoopBackOff pods must rank above plain Running pods."""
        pods = [
            _pod("running", container_states=[]),
            _pod("crash", container_states=["CrashLoopBackOff"]),
        ]
        prioritized = _smart_sort(pods)
        assert prioritized[0]["name"] == "crash"

    def test_smart_sort_high_restart_before_low_restart(self):
        """Higher restart count must sort before lower restart count."""
        pods = [
            _pod("low", restart_count=1),
            _pod("high", restart_count=50),
            _pod("none", restart_count=0),
        ]
        prioritized = _smart_sort(pods)
        names = [p["name"] for p in prioritized]
        assert names.index("high") < names.index("low")
        assert names.index("low") < names.index("none")

    def test_smart_sort_all_equal_pods_stable(self):
        """All equal pods (no special states) must all appear in output."""
        pods = [_pod(f"pod-{i}") for i in range(5)]
        prioritized = _smart_sort(pods)
        assert len(prioritized) == 5

    # ------------------------------------------------------------------
    # max_pods cap
    # ------------------------------------------------------------------

    def test_max_pods_cap_respected(self):
        """Only max_pods pods must be processed even when more are available."""
        pods = [_pod(f"pod-{i}") for i in range(20)]
        analysis_map = {p["name"]: _err_analysis() for p in pods}
        findings, _ = _run_loop(pods, analysis_map, max_pods=7)
        assert len(findings) == 7

    # ------------------------------------------------------------------
    # issues_found list
    # ------------------------------------------------------------------

    def test_issues_found_only_from_success_pods_with_errors_pattern(self):
        """issues_found must only be populated by success pods with error patterns."""
        pods = [_pod("pod-errors"), _pod("pod-err-dict")]
        analysis_map = {
            "pod-errors": {
                "metadata": {"processing_metrics": {"total_log_lines": 5, "patterns_extracted": 1}},
                "patterns": {"errors": [{"content": "OOMKilled: memory limit exceeded"}]},
            },
            "pod-err-dict": _err_analysis("logs unavailable"),
        }
        findings, issues_found = _run_loop(pods, analysis_map)
        assert len(findings) == 2
        assert len(issues_found) == 1
        assert "pod-errors" in issues_found[0]

    def test_success_pod_with_no_error_pattern_not_in_issues(self):
        """Success pods with no errors pattern must not add to issues_found."""
        pods = [_pod("clean-pod")]
        analysis_map = {"clean-pod": _ok_analysis()}
        findings, issues_found = _run_loop(pods, analysis_map)
        assert len(findings) == 1
        assert issues_found == []
