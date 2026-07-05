from logrisk.ai_harness.context_budget import estimate_tokens_from_chars


def test_estimate_tokens_from_chars_is_simple_and_non_zero():
    assert estimate_tokens_from_chars(0) == 1
    assert estimate_tokens_from_chars(180) == 100
