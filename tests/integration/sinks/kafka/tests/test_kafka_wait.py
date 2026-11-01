"""Unit checks for kafka_it topic catch-up wait (no broker required)."""

from ..harness.kafka_it import peak_msg_min_increase, topic_catchup_done


def test_topic_catchup_ignores_historical_end():
    # Topic already has 100 messages; still need +50 from the operation under test.
    assert topic_catchup_done(100, baseline=100, min_increase=50) is False
    assert topic_catchup_done(149, baseline=100, min_increase=50) is False
    assert topic_catchup_done(150, baseline=100, min_increase=50) is True


def test_topic_catchup_from_empty_baseline():
    assert topic_catchup_done(0, baseline=0, min_increase=100) is False
    assert topic_catchup_done(100, baseline=0, min_increase=100) is True


def test_topic_catchup_zero_increase():
    assert topic_catchup_done(50, baseline=50, min_increase=0) is True


def test_peak_wait_when_baseline_exceeds_src_rows():
    """Historical Kafka end can exceed cumulative source rows; still wait for this peak."""
    src_before, src_after = 1_000, 1_100
    baseline = 500_000  # prior topic history ≫ source row count
    need = peak_msg_min_increase(src_before, src_after)
    assert need == 99  # 0.99 × 100 new rows
    # Must not treat "baseline already large" as done.
    assert topic_catchup_done(baseline, baseline=baseline, min_increase=need) is False
    assert topic_catchup_done(baseline + 98, baseline=baseline, min_increase=need) is False
    assert topic_catchup_done(baseline + 99, baseline=baseline, min_increase=need) is True


def test_peak_msg_min_increase_from_delta_only():
    assert peak_msg_min_increase(10_000, 10_000) == 0
    assert peak_msg_min_increase(10_000, 10_050) == 49
    # Cumulative source size must not affect the required increase.
    assert peak_msg_min_increase(100, 200) == peak_msg_min_increase(1_000_000, 1_000_100)
