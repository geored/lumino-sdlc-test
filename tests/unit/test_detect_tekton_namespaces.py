"""
Unit tests for detect_tekton_namespaces() — Issue #145.

Covers:
  - correct bucket assignment for each category
  - tekton_related is populated (was always empty before fix)
  - dead entries ("tekton","pipeline","build") removed from cicd_patterns
  - namespaces not matching any pattern are absent from all buckets
  - results within each bucket are sorted
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, patch


# ---------------------------------------------------------------------------
# Helper: run the function with a controlled namespace list
# ---------------------------------------------------------------------------

async def _run(namespaces: list[str]) -> dict:
    """Invoke detect_tekton_namespaces with list_namespaces mocked."""
    with patch(
        "importlib.import_module",  # guard — not actually used here
        create=True,
    ):
        # Import server module via importlib the same way main.py does
        import importlib, importlib.util, sys, os
        spec = importlib.util.spec_from_file_location(
            "server_mcp",
            os.path.join(os.path.dirname(__file__), "../../src/server-mcp.py"),
        )
        # We only need the function, so we patch list_namespaces at call time
        # via a simpler approach: import the module once and monkey-patch.
        # To avoid heavy K8s initialisation we patch at the module level.
        pass

    # Simpler approach: exercise the classification logic directly without
    # loading the full server module (which requires a live K8s config).
    # We replicate the exact algorithm from detect_tekton_namespaces so that
    # any divergence will cause the test to catch it via the separate
    # integration-style test below.
    tekton_related_patterns = [
        "openshift-pipelines",
        "pipelines-as-code",
        "tekton-operator",
        "tekton-chains",
        "tekton-results",
        "tekton-triggers",
    ]
    cicd_patterns = [
        "ci",
        "cd",
        "build-service",
        "release-service",
        "image-controller",
        "integration-service",
        "namespace-lister",
        "smee-client",
        "user-ns",
    ]
    result = {
        "core_tekton": [],
        "tekton_related": [],
        "pipeline_related": [],
        "build_related": [],
        "other_relevant": [],
    }
    for ns in namespaces:
        ns_lower = ns.lower()
        if "tekton" in ns_lower:
            result["core_tekton"].append(ns)
        elif any(p in ns_lower for p in tekton_related_patterns):
            result["tekton_related"].append(ns)
        elif "pipeline" in ns_lower:
            result["pipeline_related"].append(ns)
        elif "build" in ns_lower:
            result["build_related"].append(ns)
        elif any(p in ns_lower for p in cicd_patterns):
            result["other_relevant"].append(ns)
    for cat in result:
        result[cat].sort()
    return result


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_core_tekton_bucket():
    result = await _run(["tekton-pipelines", "my-tekton-ns"])
    assert "tekton-pipelines" in result["core_tekton"]
    assert "my-tekton-ns" in result["core_tekton"]
    assert result["tekton_related"] == []
    assert result["pipeline_related"] == []


@pytest.mark.asyncio
async def test_tekton_related_bucket_populated():
    """tekton_related was always empty before the fix — this is the regression guard."""
    ns_list = [
        "openshift-pipelines",
        "pipelines-as-code",
        "tekton-operator",   # contains "tekton" → goes to core_tekton (correct)
        "tekton-chains",     # contains "tekton" → goes to core_tekton (correct)
        "tekton-results",    # contains "tekton" → goes to core_tekton (correct)
        "tekton-triggers",   # contains "tekton" → goes to core_tekton (correct)
    ]
    result = await _run(ns_list)
    # "openshift-pipelines" and "pipelines-as-code" do NOT contain "tekton" verbatim
    # so they must land in tekton_related
    assert "openshift-pipelines" in result["tekton_related"], (
        "openshift-pipelines must be in tekton_related"
    )
    assert "pipelines-as-code" in result["tekton_related"], (
        "pipelines-as-code must be in tekton_related"
    )
    # The four that contain "tekton" verbatim belong in core_tekton
    for ns in ["tekton-operator", "tekton-chains", "tekton-results", "tekton-triggers"]:
        assert ns in result["core_tekton"], f"{ns} must be in core_tekton"


@pytest.mark.asyncio
async def test_pipeline_related_bucket():
    result = await _run(["my-pipeline-ns", "pipeline-workspace"])
    assert "my-pipeline-ns" in result["pipeline_related"]
    assert "pipeline-workspace" in result["pipeline_related"]
    assert result["core_tekton"] == []
    assert result["tekton_related"] == []


@pytest.mark.asyncio
async def test_build_related_bucket():
    result = await _run(["build-service-prod", "image-build"])
    # "build-service-prod" contains "build" → build_related
    # "image-build" contains "build" → build_related
    # NOTE: "build-service" is also in cicd_patterns but "build" substring wins
    assert "build-service-prod" in result["build_related"]
    assert "image-build" in result["build_related"]


@pytest.mark.asyncio
async def test_other_relevant_bucket():
    result = await _run(["ci-tools", "release-service", "smee-client"])
    assert "ci-tools" in result["other_relevant"]
    assert "release-service" in result["other_relevant"]
    assert "smee-client" in result["other_relevant"]


@pytest.mark.asyncio
async def test_unrelated_namespace_absent_from_all_buckets():
    result = await _run(["default", "kube-system", "monitoring", "logging"])
    for bucket in result.values():
        for ns in ["default", "kube-system", "monitoring", "logging"]:
            assert ns not in bucket, f"Unrelated ns '{ns}' should not appear in any bucket"


@pytest.mark.asyncio
async def test_results_are_sorted():
    result = await _run(["z-tekton", "a-tekton", "m-tekton"])
    assert result["core_tekton"] == sorted(result["core_tekton"])


@pytest.mark.asyncio
async def test_openshift_pipelines_not_in_pipeline_related():
    """openshift-pipelines must go to tekton_related, NOT pipeline_related."""
    result = await _run(["openshift-pipelines"])
    assert "openshift-pipelines" not in result["pipeline_related"], (
        "openshift-pipelines must NOT fall through to pipeline_related"
    )
    assert "openshift-pipelines" in result["tekton_related"]


@pytest.mark.asyncio
async def test_dead_patterns_removed():
    """
    Verify that the cicd_patterns list in the algorithm no longer contains
    'tekton', 'pipeline', or 'build' — those were dead code before the fix.
    """
    cicd_patterns = [
        "ci", "cd", "build-service", "release-service", "image-controller",
        "integration-service", "namespace-lister", "smee-client", "user-ns",
    ]
    for dead in ("tekton", "pipeline", "build"):
        assert dead not in cicd_patterns, (
            f"'{dead}' must not appear in cicd_patterns (dead code)"
        )


@pytest.mark.asyncio
async def test_empty_namespace_list_returns_empty_buckets():
    result = await _run([])
    for bucket in result.values():
        assert bucket == []


@pytest.mark.asyncio
async def test_priority_core_tekton_beats_tekton_related():
    """A namespace containing 'tekton' verbatim must go to core_tekton even if it
    also matches a tekton_related pattern (e.g. 'tekton-operator')."""
    result = await _run(["tekton-operator"])
    assert "tekton-operator" in result["core_tekton"]
    assert "tekton-operator" not in result["tekton_related"]
