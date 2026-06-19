"""Unit tests for AdaptiveLogProcessor — issue #144."""
import pytest
from helpers.k8s_client import AdaptiveLogProcessor as K8sALP
from models import AdaptiveLogProcessor as ModelsALP

@pytest.fixture(params=[K8sALP, ModelsALP])
def cls(request):
    return request.param

class TestInitValidation:
    def test_zero_budget_raises(self, cls):
        with pytest.raises(ValueError, match="max_token_budget must be a positive integer"):
            cls(0)
    def test_negative_budget_raises(self, cls):
        with pytest.raises(ValueError, match="max_token_budget must be a positive integer"):
            cls(-1)
    def test_default_budget_ok(self, cls):
        proc = cls()
        assert proc.max_token_budget == 150000
    def test_positive_budget_ok(self, cls):
        proc = cls(1000)
        assert proc.max_token_budget == 1000

class TestGetUsagePercentage:
    def test_returns_zero_when_effective_budget_zero(self, cls):
        # budget=1 passes __init__ but int(1*0.8)=0
        proc = cls(1)
        assert proc.effective_budget == 0
        assert proc.get_usage_percentage() == 0.0
    def test_normal_percentage(self, cls):
        proc = cls(1000)
        proc.record_usage(400)
        assert abs(proc.get_usage_percentage() - 50.0) < 0.001
    def test_zero_used_is_zero_pct(self, cls):
        proc = cls(1000)
        assert proc.get_usage_percentage() == 0.0
    def test_full_budget_is_100_pct(self, cls):
        proc = cls(1000)
        proc.record_usage(proc.effective_budget)
        assert abs(proc.get_usage_percentage() - 100.0) < 0.001

class TestHelpers:
    def test_can_process_more_within(self, cls):
        proc = cls(1000)
        assert proc.can_process_more(100) is True
    def test_can_process_more_over(self, cls):
        proc = cls(1000)
        proc.record_usage(proc.effective_budget)
        assert proc.can_process_more(1) is False
    def test_remaining_decreases(self, cls):
        proc = cls(1000)
        initial = proc.get_remaining_budget()
        proc.record_usage(100)
        assert proc.get_remaining_budget() == initial - 100
    def test_remaining_never_negative(self, cls):
        proc = cls(1000)
        proc.record_usage(proc.effective_budget + 9999)
        assert proc.get_remaining_budget() == 0
