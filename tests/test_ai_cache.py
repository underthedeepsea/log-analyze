from logrisk.ai_harness.cache import AICache, cache_signature, safe_generation_options


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


def test_cache_signature_includes_order_independent_effective_options_and_schema():
    first = cache_signature(
        "evidence", "prompt-a", "ollama", "qwen3:1.7b",
        generation_options={"temperature": 0, "num_predict": 1200, "structured_output_mode": "json_schema"},
        schema_digest="schema-a",
    )
    same = cache_signature(
        "evidence", "prompt-a", "ollama", "qwen3:1.7b",
        generation_options={"structured_output_mode": "json_schema", "num_predict": 1200, "temperature": 0},
        schema_digest="schema-a",
    )
    warmer = cache_signature(
        "evidence", "prompt-a", "ollama", "qwen3:1.7b",
        generation_options={"temperature": 0.2, "num_predict": 1200, "structured_output_mode": "json_schema"},
        schema_digest="schema-a",
    )
    more_output = cache_signature(
        "evidence", "prompt-a", "ollama", "qwen3:1.7b",
        generation_options={"temperature": 0, "num_predict": 1600, "structured_output_mode": "json_schema"},
        schema_digest="schema-a",
    )
    json_object = cache_signature(
        "evidence", "prompt-a", "ollama", "qwen3:1.7b",
        generation_options={"temperature": 0, "num_predict": 1200, "structured_output_mode": "json_object"},
        schema_digest="schema-a",
    )
    changed_schema = cache_signature(
        "evidence", "prompt-a", "ollama", "qwen3:1.7b",
        generation_options={"temperature": 0, "num_predict": 1200, "structured_output_mode": "json_schema"},
        schema_digest="schema-b",
    )

    assert first == same
    assert len(first) == 64
    assert len({first, warmer, more_output, json_object, changed_schema}) == 5


def test_cache_signature_excludes_secret_generation_options():
    first = cache_signature(
        "evidence", "prompt-a", "ollama", "qwen3:1.7b",
        generation_options={
            "temperature": 0,
            "api_key": "first-test-secret",
            "nested": {"token": "nested-first-secret", "top_p": 0.9},
        },
    )
    second = cache_signature(
        "evidence", "prompt-a", "ollama", "qwen3:1.7b",
        generation_options={
            "temperature": 0,
            "api_key": "second-test-secret",
            "nested": {"token": "nested-second-secret", "top_p": 0.9},
        },
    )

    assert first == second


def test_generation_option_allowlist_drops_nested_and_hyphenated_credentials():
    first_options = {
        "temperature": 0,
        "top_p": 0.9,
        "x-api-key": "hyphenated-first-secret",
        "credentials": {"bearer": "nested-first-secret"},
        "auth": {"access-token": "nested-first-token"},
        "provider_options": {"api-key": "another-first-secret"},
    }
    second_options = {
        "temperature": 0,
        "top_p": 0.9,
        "x-api-key": "hyphenated-second-secret",
        "credentials": {"bearer": "nested-second-secret"},
        "auth": {"access-token": "nested-second-token"},
        "provider_options": {"api-key": "another-second-secret"},
    }

    assert safe_generation_options(first_options) == {"temperature": 0, "top_p": 0.9}
    assert cache_signature(
        "evidence", "prompt-a", "ollama", "qwen3:1.7b", generation_options=first_options,
    ) == cache_signature(
        "evidence", "prompt-a", "ollama", "qwen3:1.7b", generation_options=second_options,
    )
