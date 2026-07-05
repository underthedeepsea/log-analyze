import gzip

import pytest

from logrisk.stream_input_parser import iter_log_records_from_file


def test_iter_log_records_from_extensionless_text(tmp_path):
    path = tmp_path / "messages"
    path.write_text("one\n\ntwo\n", encoding="utf-8")

    rows = list(iter_log_records_from_file(path, filename="messages"))

    assert [row["message"] for row in rows] == ["one", "two"]


def test_iter_jsonl_bad_line_as_plain_text_and_strict(tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_text('{"message":"one"}\nnot-json\n', encoding="utf-8")

    rows = list(iter_log_records_from_file(path))

    assert rows[0]["message"] == "one"
    assert rows[1]["_parse_error"] == "jsonl_decode_failed"
    with pytest.raises(Exception):
        list(iter_log_records_from_file(path, jsonl_bad_line_policy="strict"))


def test_iter_gz_checks_decompressed_limit(tmp_path):
    path = tmp_path / "messages.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write("hello\n")

    assert list(iter_log_records_from_file(path))[0]["message"] == "hello"
    with pytest.raises(ValueError, match="Decompressed"):
        list(iter_log_records_from_file(path, max_decompressed_bytes=1))
