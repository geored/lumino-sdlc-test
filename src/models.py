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


# ============================================================================
# EVENT ANALYSIS MODELS
# ============================================================================

class EventSeverity(Enum):
    """Event severity levels."""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class EventCategory(Enum):
    """Event functional categories."""
    FAILURE = "FAILURE"
    SCHEDULING = "SCHEDULING"
    NETWORKING = "NETWORKING"
    STORAGE = "STORAGE"
    SCALING = "SCALING"
    LIFECYCLE = "LIFECYCLE"
    HEALTH = "HEALTH"
    SECURITY = "SECURITY"
    CONFIGURATION = "CONFIGURATION"
    RESOURCE = "RESOURCE"
    IMAGE = "IMAGE"
    OTHER = "OTHER"


# ============================================================================
# ADAPTIVE LOG PROCESSING MODEL
# ============================================================================

class AdaptiveLogProcessor:
    """Helper class for adaptive log processing with token management."""

    def __init__(self, max_token_budget: int = 150000):
        self.max_token_budget = max_token_budget
        self.safety_buffer = 0.8  # Use 80% of budget for safety
        self.effective_budget = int(max_token_budget * self.safety_buffer)
        self.used_tokens = 0

    def can_process_more(self, estimated_tokens: int) -> bool:
        """Check if we can process more data within token budget."""
        return (self.used_tokens + estimated_tokens) <= self.effective_budget

    def record_usage(self, actual_tokens: int):
        """Record actual token usage."""
        self.used_tokens += actual_tokens

    def get_remaining_budget(self) -> int:
        """Get remaining token budget."""
        return max(0, self.effective_budget - self.used_tokens)

    def get_usage_percentage(self) -> float:
        """Get current token usage as percentage."""
        return (self.used_tokens / self.effective_budget) * 100
