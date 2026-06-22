from logrisk.normalizer import normalize_record


def test_syslog_klog_normalize():
    record = {
        "timestamp": "2026-06-22T10:01:01+08:00",
        "cluster": "prod-a",
        "message": "Jun 22 10:01:01 node-a kubelet[2145]: E0622 10:01:01.123456 eviction_manager.go:350] eviction manager: pods ranked for eviction",
    }
    out = normalize_record(record)
    assert out["node"] == "node-a"
    assert out["component"] == "kubelet"
    assert out["severity"] == "ERROR"
    assert out["message_core"] == "eviction manager: pods ranked for eviction"
