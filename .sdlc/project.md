# LUMINO MCP Server — Project Reference

## Language & Runtime

- **Language**: Python 3.10+ (tested on 3.11, 3.12)
- **Package manager**: `uv` / `hatchling`; pinned in `pyproject.toml`
- **Transport**: `stdio` (local) or `streamable-http` on port 8000 (Kubernetes)
- **MCP SDK**: `mcp[cli] >= 1.10.1` via `FastMCP`
- **Container base**: `registry.access.redhat.com/ubi9/ubi` → Python 3.11, non-root UID 1001

---

## Architecture

```
main.py                     ← entry point; transport selection via env vars
  └─ importlib loads src/server-mcp.py   (hyphen in filename requires importlib)
       ├─ FastMCP("lumino-mcp-server")   ← MCP framework instance (mcp)
       ├─ 39 @mcp.tool() functions       ← all public API surface
       ├─ Kubernetes API clients         ← initialised at module level
       │    k8s_core_api, k8s_apps_api, k8s_custom_api,
       │    k8s_batch_api, k8s_storage_api, k8s_autoscaling_api
       ├─ PrometheusEndpointCache        ← TTL=300s, in-memory
       ├─ AdaptiveLogProcessor           ← per-request token budget manager
       └─ src/helpers/                   ← pure helper modules (no I/O side-effects)
            constants.py                 ← SMART_EVENTS_CONFIG, LOG_ANALYSIS_CONFIG, …
            utils.py                     ← shared utilities (duration, log fetch, parse)
            log_analysis.py              ← strategy classes & stream processor
            event_analysis.py            ← EventSeverity, ProgressiveEventAnalyzer, MLPatternDetector
            failure_analysis.py          ← RCA helpers
            resource_topology.py         ← graph / topology mapping helpers
            semantic_search.py           ← NLP query interpretation
            ml_persistence.py            ← SQLite-backed model & label store
            kubearchive_integration.py   ← KubeArchive discovery & client
```

### Transport decision (main.py)
```
KUBERNETES_NAMESPACE or K8S_NAMESPACE set  →  mcp.run(transport='streamable-http')
otherwise                                  →  mcp.run()   # stdio
```

---

## File Map

| Path | Purpose |
|------|---------|
| `main.py` | Entry point; transport selection; imports server module via `importlib` |
| `src/server-mcp.py` | All 39 MCP tools; Kubernetes API init; Prometheus helpers; `AdaptiveLogProcessor`; `PrometheusEndpointCache` |
| `src/helpers/__init__.py` | Re-exports every public symbol from all helper modules |
| `src/helpers/constants.py` | `SMART_EVENTS_CONFIG`, `LOG_ANALYSIS_CONFIG`, `PIPELINE_ANALYSIS_CONFIG`, `SEMANTIC_SEARCH_CONFIG`, `KUBEARCHIVE_CONFIG` |
| `src/helpers/utils.py` | `get_all_pod_logs`, `calculate_duration*`, `parse_time_period`, `calculate_context_tokens`, `detect_anomalies_in_data`, simulation helpers, forecast helpers, cert helpers |
| `src/helpers/log_analysis.py` | `LogAnalysisStrategy` enum, `LogAnalysisContext`, `AnalysisCache` (TTL cache), `StrategySelector`, `LogStreamProcessor`, `truncate_to_token_limit`, ML log feature extraction |
| `src/helpers/event_analysis.py` | `EventSeverity`/`EventCategory` enums, `ProgressiveEventAnalyzer`, `MLPatternDetector`, `LogMetricsIntegrator`, `RunbookSuggestionEngine`, string-event helpers |
| `src/helpers/failure_analysis.py` | `identify_failure_context`, `analyze_pipeline_failure`, `analyze_pod_failure`, `perform_advanced_rca`, `generate_remediation_plan`, `assess_failure_severity` |
| `src/helpers/resource_topology.py` | `get_multi_cluster_clients`, `correlate_pipeline_events`, `follow_lifecycle_chain`, graph-viz converters, `analyze_bottlenecks` |
| `src/helpers/semantic_search.py` | `interpret_semantic_query`, `determine_search_strategy`, `extract_k8s_entities`, relevance ranking, async pod/event/Tekton search helpers |
| `src/helpers/ml_persistence.py` | `ModelPersistenceManager` (joblib model files), `TrainingDataStore` (SQLite), `FailureEventCollector`, `ModelVersionManager` |
| `src/helpers/kubearchive_integration.py` | `KubeArchiveEndpointDiscovery` (Route/Ingress/env), `KubeArchiveClient` (aiohttp), `query_kubearchive_resources` |
| `pyproject.toml` | Package metadata, all deps, dev deps (`pytest`, `pytest-asyncio`) |
| `server.json` | MCP Registry manifest (`io.github.geored/lumino`) |
| `Containerfile` | OCI image build (UBI9, non-root, port 8000) |

---

## Key Patterns

### 1. Module-level Kubernetes client initialization (graceful degradation)
Kubernetes config is loaded twice at module level (once before KubeArchive setup, once after — a known duplication). Both attempts follow the same pattern: `load_incluster_config()` → fallback to `load_kube_config()` → warning if neither works. Each API client variable may be `None`; every tool that uses them must guard before calling.

### 2. `@mcp.tool()` decorator wrapping with logging
`enhanced_tool_decorator` replaces `mcp.tool` before any tool definitions. It wraps each tool function with `log_tool_execution`, which emits `INFO` on entry/exit and `ERROR` on exception, then re-raises. Two wrappers exist (async and sync) selected via `asyncio.iscoroutinefunction`.

### 3. Adaptive log processing (token budget)
`AdaptiveLogProcessor` tracks consumption across all pods in a pipeline run:
1. Prioritize pods: `Failed` phase → container error states → high restart count → recency (`_prioritize_pipeline_pods`)
2. Sample 10% of lines to estimate token density (`_estimate_pod_log_tokens`)
3. Compute `tail_lines` from remaining budget (`_calculate_adaptive_tail_lines`): small pipeline ≤ 2000 lines, medium ≤ 1000, large ≤ 500
4. After fetch, hard-truncate if actual tokens exceed remaining budget (`_truncate_logs_to_token_limit`)
5. **Invariant**: first pod is always processed regardless of budget
6. Add `_metadata` key to result with processing stats

### 4. Prometheus endpoint discovery chain (6-step)
`_discover_prometheus_endpoint()` tries in priority order:
1. `THANOS_URL` env var
2. `PROMETHEUS_URL` env var
3. Predefined cluster dict keyed by `cluster_override`
4. In-memory TTL cache (`PrometheusEndpointCache`, 300 s)
5. Auto-discovery: Thanos services → Prometheus services → Operator CRD → OpenShift Routes (order differs inside/outside cluster)
6. Fallback predefined endpoints

Returns `(url, endpoint_type)` tuple where `endpoint_type` ∈ `{"prometheus", "thanos"}`. Thanos requests append `dedup=true`.

### 5. Log analysis strategy pattern
```
LogAnalysisStrategy: SMART_SUMMARY | STREAMING | HYBRID
StrategySelector.select_strategy(LogAnalysisContext)
  inputs: log_size_estimate, urgency ("low"/"medium"/"high"/"critical"),
          request_type ("investigation"/"troubleshooting"/"monitoring")
```
`AnalysisCache` provides per-pod result caching keyed by `(namespace, pod_name, params_hash)`. Results from hybrid strategy are `combine_analysis_results(summary, streaming)`.

### 6. Progressive event analysis
Four levels: `overview` → `detailed` → `correlation` → `deep_dive`. `deep_dive` internally calls all three prior levels. Each level is a distinct method on `ProgressiveEventAnalyzer`. Fallback: if no events found, expands time window through `["12h", "24h", "7d"]` before returning empty.

### 7. Kubernetes event pagination
`_get_namespace_events_internal` fetches pages of 5000 events (max 20 pages) using `_continue` tokens. Early exit when oldest event in a page is older than `cutoff_time`, or when sufficient events for count filtering have been collected. Expired tokens (HTTP 410) break the loop cleanly.

### 8. ML persistence (SQLite + joblib)
`TrainingDataStore` → SQLite (path from env or XDG default). Tables: `log_samples`, `failure_labels`, `log_failure_correlations`.
`ModelPersistenceManager` → joblib-serialised `IsolationForest` models. `FailureEventCollector` ingests Kubernetes event dicts, pod status objects, and pipeline run dicts; correlates within a configurable time window.
`train_or_load_model` → loads cached model by ID unless `force_retrain=True`.

### 9. KubeArchive integration
`KubeArchiveEndpointDiscovery` priority: `KUBEARCHIVE_HOST` env → OpenShift Route in `kubearchive` ns → Kubernetes Ingress → auto port-forward. `KubeArchiveClient` is `aiohttp`-based; results normalised to the same dict shape as live resources. Single entry point: `query_kubearchive` tool.

### 10. Resource type dispatch (`get_kubernetes_resource`)
Seven static dicts map lowercase resource type string → `(plural, api_version)`. Cluster-scoped resources (node, namespace, pv, clustertask) use non-namespaced API methods. Unknown types return a formatted error string listing all known types.

### 11. Namespace detection for Tekton
`detect_tekton_namespaces()` classifies all namespaces into five categories by substring matching: `core_tekton` (contains "tekton"), `pipeline_related`, `build_related`, `other_relevant`, `tekton_related` (unused placeholder). Used by RCA, predictive analyzer, and tracer to scope searches.

---

## Invariants

1. **Every `@mcp.tool()` returns on error** — never propagates exceptions to caller; returns `{"error": "..."}` dict or error string.
2. **First pod always processed** — `AdaptiveLogProcessor` guarantees ≥ 1 pod fetched regardless of token budget.
3. **`_namespace_cache` TTL = 86400 s** — stale for up to 24 hours; tools needing fresh data call `list_namespaces()` directly.
4. **`PrometheusEndpointCache` TTL = 300 s** — call `invalidate()` to force re-discovery.
5. **Kubernetes clients may be `None`** — every tool must guard: `if not k8s_core_api: return [{"error": ...}]`.
6. **`server-mcp.py` loaded via `importlib`** — hyphen in filename prevents normal `import`; `main.py` must never be changed to use `import`.
7. **Tekton CRD versions**: `tekton.dev/v1` for Pipeline/PipelineRun/Task/TaskRun; `tekton.dev/v1beta1` for ClusterTask (deprecated).
8. **No hardcoded secrets** — all tokens from kubeconfig, `/var/run/secrets/…/token`, or env vars (`PROMETHEUS_TOKEN`, `THANOS_URL`, `KUBEARCHIVE_HOST`).
9. **`MAX_SERIES_LIMIT = 500`** — Prometheus results hard-capped before formatting to prevent response bloat.
10. **Cluster-wide TaskRun LIST capped at 100** — prevents ~97 MB responses; per-namespace queries preferred.
11. **`asyncio.to_thread` for all blocking SDK calls** — the MCP event loop must not be blocked by synchronous Kubernetes SDK methods.
12. **`_metadata` key injected into log results** — callers and tests must treat `_metadata` as a reserved key in all `get_pipelinerun_logs` responses.
13. **`detect_tekton_namespaces` is not an `@mcp.tool()`** — it is an internal async helper; calling it as a tool will fail.

---

## Commands

```bash
# Install dependencies
uv sync

# Run locally (stdio transport)
python main.py

# Run in Kubernetes mode (streamable-http on :8000)
KUBERNETES_NAMESPACE=my-ns python main.py

# Syntax check all source files
python -m py_compile src/server-mcp.py src/helpers/utils.py \
  src/helpers/log_analysis.py src/helpers/event_analysis.py \
  src/helpers/failure_analysis.py src/helpers/resource_topology.py \
  src/helpers/semantic_search.py src/helpers/ml_persistence.py \
  src/helpers/kubearchive_integration.py src/helpers/constants.py main.py

# Run tests (currently 0 collected — see test strategy below)
pytest -v

# Run tests with asyncio support
pytest --asyncio-mode=auto -v

# Build container image
podman build -t quay.io/geored/lumino-mcp-server .

# Run container with local kubeconfig
podman run -it --rm -p 8000:8000 \
  -e KUBERNETES_NAMESPACE=default \
  -v ~/.kube:/home/lumino/.kube:ro \
  quay.io/geored/lumino-mcp-server
```

---

## Test Strategy

### Current state
**Zero tests exist** (`pytest` collects 0 items). This is the project's most critical quality gap. All 11 source files pass syntax checks. Dev dependencies are declared (`pytest >= 7.0.0`, `pytest-asyncio >= 0.21.0`) and installed.

### Recommended test layout

```
tests/
  unit/
    test_adaptive_log_processor.py     # token budget math, truncation, first-pod guarantee
    test_prometheus_endpoint_cache.py  # TTL expiry, invalidation, (url, type) tuple return
    test_prometheus_discovery.py       # 6-step chain, each step mocked
    test_duration_helpers.py           # calculate_duration, calculate_duration_seconds, edge cases
    test_event_classification.py       # EventSeverity/Category, smart_sample_string_events
    test_log_analysis_strategy.py      # StrategySelector inputs → correct Strategy enum
    test_analysis_cache.py             # TTL expiry, key hashing, cache hit/miss
    test_kubearchive_normalizer.py     # normalize_to_rfc3339 valid and invalid inputs
    test_certificate_helpers.py        # parse_certificate, categorize_certificate_status
    test_label_selector_builder.py     # build_advanced_label_selector all operator types
    test_detect_tekton_namespaces.py   # classification priority ordering
  integration/
    test_tools_mocked_k8s.py          # all 39 tools with MagicMock kubernetes clients
    test_kubearchive_client.py         # query_kubearchive with mocked aiohttp responses
    test_prometheus_query_tool.py      # prometheus_query happy path + all HTTP error codes
  error_paths/
    test_api_exceptions.py            # 401/403/404/410 ApiException per tool
    test_token_budget_exhaustion.py    # adaptive processor halts at budget; metadata accurate
    test_none_k8s_clients.py          # every tool returns {"error":...} when clients=None
    test_import_via_importlib.py      # main.py importlib loading does not regress
```

### Priority matrix (highest risk, zero coverage)

| Priority | Target | Risk if untested |
|----------|--------|-----------------|
| **P0** | `AdaptiveLogProcessor` budget math | Wrong math → truncation or OOM on large pipelines |
| **P0** | `get_pipelinerun_logs` adaptive vs manual branch | Core feature; complex branching and metadata injection |
| **P0** | `ApiException` 403/404/410 in list tools | Silent empty returns mask auth failures in production |
| **P0** | `_discover_prometheus_endpoint` fallback chain | Any broken step silently skips; metrics go dark |
| **P1** | `query_kubearchive` KubeArchive-unavailable path | Most common deployment state; must surface clearly |
| **P1** | `detect_anomalies` with < 3 data points | z-score undefined; potential `ZeroDivisionError` or `NaN` |
| **P1** | `PrometheusEndpointCache` TTL expiry | Stale entries cause silent query failures |
| **P1** | `LogStreamProcessor` finalize() with partial chunk | Edge case: last chunk < chunk_size |
| **P2** | `FailureEventCollector.correlate_logs_with_failures` | Correlation time-window arithmetic |
| **P2** | `KubeArchiveEndpointDiscovery` no route/ingress | Must return `None`, not raise |
| **P2** | `importlib` load path in `main.py` | Breakage here = total server startup failure |

### Testing conventions
- Use `@pytest.mark.asyncio` (or `asyncio_mode = "auto"` in `pyproject.toml`) for all `async def` tool tests.
- Mock Kubernetes clients with `unittest.mock.MagicMock(spec=client.CoreV1Api())` etc.
- Raise `ApiException` as `kubernetes.client.rest.ApiException(status=403, reason="Forbidden")`.
- Mock `aiohttp.ClientSession` using `aioresponses` or `unittest.mock.AsyncMock`.
- **Always assert error-path return values** — never assert that exceptions propagate from tools.
- Use `pytest.mark.parametrize` for the 6-step Prometheus discovery chain and all HTTP status codes.
- **Baseline rule**: every `@mcp.tool()` must have ≥ 1 happy-path test + ≥ 1 error-path test before merge.
