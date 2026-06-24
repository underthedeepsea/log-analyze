import json

import pytest

from logrisk.input_parser import parse_log_content


def test_plain_text_uses_each_non_empty_line_as_a_record():
    rows = parse_log_content("events.log", "first error\n\nsecond error\n")

    assert rows == [{"message": "first error"}, {"message": "second error"}]


def test_jsonl_content_is_detected_without_relying_on_suffix():
    rows = parse_log_content(
        "upload.txt",
        '{"message":"one"}\n{"message":"two"}',
    )

    assert [row["message"] for row in rows] == ["one", "two"]


def test_json_container_and_string_items_are_normalized():
    rows = parse_log_content(
        "logs.json",
        json.dumps({"logs": ["one", {"message": "two"}]}),
    )

    assert rows == [{"message": "one"}, {"message": "two"}]


def test_empty_content_is_rejected():
    with pytest.raises(ValueError, match="不能为空"):
        parse_log_content("empty.log", " \n ")


def test_structured_suffix_rejects_malformed_json_instead_of_treating_as_text():
    with pytest.raises(ValueError, match="JSON"):
        parse_log_content("broken.json", "{not json}")
