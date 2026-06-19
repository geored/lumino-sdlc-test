"""Smoke tests — verify syntax and basic importability of all source modules."""

import importlib.util
import py_compile

HELPER_MODULES = (
    "src/helpers/constants.py",
    "src/helpers/utils.py",
    "src/helpers/log_analysis.py",
    "src/helpers/event_analysis.py",
    "src/helpers/failure_analysis.py",
    "src/helpers/resource_topology.py",
    "src/helpers/semantic_search.py",
    "src/helpers/ml_persistence.py",
    "src/helpers/kubearchive_integration.py",
)

SERVER_FILES = ("main.py", "src/server-mcp.py")


def test_syntax_all_helper_modules():
    """All helper modules must pass py_compile (no syntax errors)."""
    errors = []
    for path in HELPER_MODULES:
        try:
            py_compile.compile(path, doraise=True)
        except py_compile.PyCompileError as exc:
            errors.append(path + ": " + str(exc))
    assert not errors, "Syntax errors found:\n" + "\n".join(errors)


def test_syntax_main_and_server():
    """main.py and server-mcp.py must pass py_compile."""
    errors = []
    for path in SERVER_FILES:
        try:
            py_compile.compile(path, doraise=True)
        except py_compile.PyCompileError as exc:
            errors.append(path + ": " + str(exc))
    assert not errors, "Syntax errors found:\n" + "\n".join(errors)


def test_constants_importable():
    """src/helpers/constants.py must be importable and expose key config dicts."""
    try:
        spec = importlib.util.spec_from_file_location(
            "helpers.constants", "src/helpers/constants.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    except Exception as exc:
        raise AssertionError("constants.py failed to import: " + str(exc))
    for name in ("SMART_EVENTS_CONFIG", "LOG_ANALYSIS_CONFIG"):
        assert hasattr(mod, name), "constants.py missing expected symbol: " + name


def test_main_uses_importlib():
    """main.py must use importlib to load server-mcp.py (not a direct import)."""
    with open("main.py", "r") as fh:
        source = fh.read()
    assert (
        "importlib" in source
    ), "main.py must use importlib to load src/server-mcp.py (hyphen in filename prevents normal import)"
    assert (
        "import server-mcp" not in source
    ), "main.py must not use 'import server-mcp' -- use importlib instead"
