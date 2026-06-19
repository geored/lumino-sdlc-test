# ============================================================================
# LUMINO MCP Server — Centralized Data Models and Type Definitions
# ============================================================================
#
# This module contains all dataclass definitions, Enum types, and pure data
# model classes used across the MCP server and helper modules.
#
# Constraints:
#   - Only imports from stdlib (dataclasses, typing, enum) — NO circular deps
#   - No imports from helpers/, server-mcp.py, or any project module
# ============================================================================

from dataclasses import dataclass
from enum import Enum

# ============================================================================
# LOG ANALYSIS MODELS
# ============================================================================


class LogAnalysisStrategy(Enum):
    """Available log analysis strategies."""

    SMART_SUMMARY = "smart_summary"
    STREAMING = "streaming"
    HYBRID = "hybrid"
    AUTO = "auto"


@dataclass
class LogAnalysisContext:
    """Context information for strategy selection."""

    log_size_estimate: int
    pod_name: str
    namespace: str
    request_type: str  # "troubleshooting", "monitoring", "investigation"
    urgency: str  # "low", "medium", "high", "critical"
    time_sensitivity: bool
    follow_up_analysis: bool
