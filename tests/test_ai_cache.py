from logrisk.ai_harness.cache import AICache, cache_signature


def test_cache_reads_written_value(tmp_path):
    cache = AICache(tmp_path / "ai_cache.json")

    cache.set("sig-1", {"features": [{"title": "OOM"}]})

    assert cache.get("sig-1") == {"features": [{"title": "OOM"}]}
    assert cache.get("missing") is None


def test_cache_signature_changes_with_prompt_or_model():
    first = cache_signature("evidence", "prompt-a", "ollama", "qwen3:1.7b")
    second = cache_signature("evidence", "prompt-b", "ollama", "qwen3:1.7b")
    third = cache_signature("evidence", "prompt-a", "ollama", "qwen3:4b")
    fourth = cache_signature("evidence", "prompt-a", "ollama", "qwen3:1.7b", thinking_enabled=False)

    assert len(first) == 64
    assert first != second
    assert first != third
    assert first != fourth
