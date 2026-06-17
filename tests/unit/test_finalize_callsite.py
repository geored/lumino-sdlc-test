"""Test that processor.finalize() result is correctly handled at the call site."""
import sys
sys.path.insert(0, '.')

import pytest
from src.helpers.log_analysis import LogStreamProcessor


def test_finalize_returns_session_summary_not_chunk():
    """finalize() must return a session-summary dict, not a chunk-result dict."""
    processor = LogStreamProcessor(chunk_size=100, analysis_mode="errors_and_warnings")
    processor.add_line("ERROR: something failed")
    processor.add_line("INFO: normal line")

    result = processor.finalize()

    assert "finalized" in result, "finalize() must return dict with 'finalized' key"
    assert result["finalized"] is True
    assert "last_chunk" in result, "finalize() must return dict with 'last_chunk' key"
    assert "all_chunks" in result, "finalize() must return dict with 'all_chunks' key"
    assert "chunk_summary" not in result, (
        "finalize() session-summary MUST NOT have 'chunk_summary' at top level"
    )


def test_finalize_last_chunk_is_valid_chunk_result_when_lines_remain():
    """last_chunk inside the summary must be a properly shaped chunk result."""
    processor = LogStreamProcessor(chunk_size=100, analysis_mode="errors_and_warnings")
    processor.add_line("ERROR: something failed")

    result = processor.finalize()
    last_chunk = result["last_chunk"]

    assert last_chunk is not None, "last_chunk should be non-None when lines remain"
    assert "chunk_summary" in last_chunk, "last_chunk must have 'chunk_summary'"
    assert "patterns" in last_chunk, "last_chunk must have 'patterns'"


def test_finalize_last_chunk_is_none_when_no_lines_remain():
    """last_chunk must be None when no partial chunk lines remain."""
    processor = LogStreamProcessor(chunk_size=100, analysis_mode="errors_and_warnings")
    result = processor.finalize()
    assert result["last_chunk"] is None


def _buggy_callsite(processor, chunk_results):
    """Reproduce the buggy call site (before fix)."""
    final_chunk = processor.finalize()
    if final_chunk:
        chunk_results.append(final_chunk)
    return chunk_results


def _fixed_callsite(processor, chunk_results):
    """The corrected call site."""
    final_summary = processor.finalize()
    if final_summary.get("last_chunk"):
        chunk_results.append(final_summary["last_chunk"])
    return chunk_results


def test_buggy_callsite_appends_wrong_dict():
    """Demonstrate the bug: the old call site appends the session-summary (wrong shape)."""
    processor = LogStreamProcessor(chunk_size=100, analysis_mode="errors_and_warnings")
    processor.add_line("ERROR: partial chunk line")

    chunk_results = []
    _buggy_callsite(processor, chunk_results)

    # The bug: a non-empty dict is always truthy, so it's always appended.
    # What's appended is the session-summary dict, NOT a proper chunk result.
    assert len(chunk_results) == 1  # something was appended...
    appended = chunk_results[0]
    # ...but it's the wrong thing: has 'finalized' and no 'chunk_summary'
    assert "finalized" in appended, "Bug confirmed: session-summary was appended, not last_chunk"
    assert "chunk_summary" not in appended, "Bug confirmed: chunk_summary missing from appended dict"


def test_corrected_callsite_appends_last_chunk_not_summary():
    """The fixed call site must append last_chunk (a proper chunk result), not the session summary."""
    processor = LogStreamProcessor(chunk_size=100, analysis_mode="errors_and_warnings")
    processor.add_line("ERROR: partial chunk line")

    chunk_results = []
    _fixed_callsite(processor, chunk_results)

    assert len(chunk_results) == 1, "Exactly one chunk result should be appended"
    appended = chunk_results[0]
    assert "chunk_summary" in appended, (
        "Appended item must be a chunk result with 'chunk_summary', "
        "not the session-summary dict"
    )
    assert "finalized" not in appended, (
        "Appended item must NOT be the session-summary dict"
    )


def test_corrected_callsite_appends_nothing_when_no_partial_chunk():
    """When no partial lines remain, the fixed call site appends nothing."""
    processor = LogStreamProcessor(chunk_size=100, analysis_mode="errors_and_warnings")

    chunk_results = []
    _fixed_callsite(processor, chunk_results)

    assert len(chunk_results) == 0, "No chunk should be appended when last_chunk is None"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
