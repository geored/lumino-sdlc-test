# Rubric & Fitness Score Analysis

## Rubric File: .rubric/thresholds.yaml

```yaml
entropy:
  baseline_avg: 23.358620689655172   # avg lint errors per file baseline
gravity:
  baseline: {}
  port: 8000
  start_command: python app.py
  type: server
kinematics:
  max_pr_age_hours: 48
trust:
  baseline_coverage: 100.0           # target: 100% test coverage
```

## What Each Dimension Measures

### entropy (lint error density)
- Measures: average flake8/pylint errors per Python file
- Baseline: 23.36 errors/file
- Current state: 164 total errors across ~7 files = ~23.4/file → RIGHT AT BASELINE → low score
- Key errors: 62x F401 unused imports, 57x E501 line-too-long, 19x E402 import order,
  6x E722 bare except, 3x F821 undefined name 're', 4x E712 comparison-to-True

### gravity (server availability/reachability)
- Measures: whether the server starts and responds on port 8000
- Baseline: empty (no prior measurement)
- start_command: python app.py (note: actual entrypoint is main.py)
- type: server
- Status: may score low because start_command is wrong (app.py vs main.py)

### kinematics (PR velocity/age)
- Measures: PR age — must be < 48 hours
- Baseline: max_pr_age_hours: 48
- Status: PRs older than 48h score 0; fresh PRs score high

### trust (test coverage)
- Measures: pytest-cov branch/statement coverage
- Baseline target: 100.0%
- Current: 11% overall (108 tests pass but cover only 11% of code)
- WHY 0.0% was reported before: --cov was not specified or pointed at wrong path
- FIXED: --cov=src gives 11% (real number). Was showing 0.0% because no --cov arg.

## Current Lint State (flake8 --max-line-length=120)
Total errors: 164
- F401: 62  (unused imports — biggest driver)
- E501: 57  (line too long)
- E402: 19  (import not at top)
- E203: 8   (whitespace before ':')
- E722: 6   (bare except)
- E712: 4   (comparison to True)
- F821: 3   (undefined name 're')
- F824: 2   (unused global)
- F841: 2   (assigned but never used)
- F402: 1   (shadowed by loop var)

## Coverage State (pytest --cov=src)
Total: 11% (10331/11632 lines uncovered)
- 108 tests pass
- Key gaps: server-mcp.py 8%, prometheus_tools.py 6%, utils.py 11%
- Best covered: models.py 83%, middleware.py 65%, log_tools.py 34%

## Fitness Score = 0.8 — Likely Breakdown
entropy: ~0.5 (at baseline, not improving)
trust:   ~0.5 (11% vs 100% target → very low, but 108 tests exist)
gravity: ~0.9 (server probably starts OK in cluster mode)
kinematics: ~1.0 (PRs are fresh)
Combined weighted → ~0.8

## Action Items to Improve Fitness

### entropy (HIGHEST IMPACT — reduces 164 errors):
1. Fix F401: Remove 62 unused imports (especially in server-mcp.py, prometheus_tools.py)
2. Fix E501: Break long lines (57 violations, max-line-length=120)
3. Fix E402: Move imports to top of files (19 violations in prometheus_tools.py)
4. Fix E722: Replace bare `except:` with `except Exception:` (6 violations)
5. Fix F821: Add `import re` where missing (3 violations)

### trust (HIGHEST IMPACT on trust score):
- Add --cov=src to pytest config in pyproject.toml
- Write tests for server-mcp.py tools (currently 8% covered)
- Target: get from 11% to 30%+ quickly with integration tests

### gravity:
- Fix start_command in .rubric/thresholds.yaml: python app.py → python main.py
- Or ensure app.py exists as alias

### kinematics:
- Keep PRs under 48 hours from open to merge
