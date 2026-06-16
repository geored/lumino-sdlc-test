"""
Smoke tests - must all pass (4/4) to reach fitness >= 0.75.
Tests verify: syntax validity, importability of helpers, main.py structure,
and server module compile-ability.
"""
import importlib.util
import py_compile
import sys
import os
import pytest


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_syntax_all_helper_modules():
    """All helper source files must compile without syntax errors."""
    helpers = [
        "src/helpers/constants.py",
        "src/helpers/utils.py",
        "src/helpers/log_analysis.py",
        "src/helpers/event_analysis.py",
        "src/helpers/failure_analysis.py",
        "src/helpers/resource_topology.py",
        "src/helpers/semantic_search.py",
        "src/helpers/ml_persistence.py",
        "src/helpers/kubearchive_integration.py",
    ]
    for path in helpers:
        py_compile.compile(path, doraise=True)


def test_syntax_main_and_server():
    """main.py and server-mcp.py must compile without syntax errors."""
    py_compile.compile("main.py", doraise=True)
    py_compile.compile("src/server-mcp.py", doraise=True)


def test_constants_importable():
    """constants.py must be importable and expose required config dicts."""
    mod = load_module("constants", "src/helpers/constants.py")
    assert hasattr(mod, "SMART_EVENTS_CONFIG"), "SMART_EVENTS_CONFIG missing"
    assert hasattr(mod, "LOG_ANALYSIS_CONFIG"), "LOG_ANALYSIS_CONFIG missing"


def test_main_uses_importlib():
    """main.py must use importlib to load server-mcp (hyphen in filename)."""
    with open("main.py") as f:
        source = f.read()
    assert "importlib" in source, "main.py must use importlib to load server-mcp.py"
    assert "server-mcp" in source, "main.py must reference server-mcp"
