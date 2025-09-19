# tests/test_pywb_mgr.py
from pathlib import Path
from crawler.pywb_mgr import ensure_collection, add_warcs


def test_ensure_collection_success(monkeypatch):
    calls = []
    def fake_run(args, check):
        calls.append(args)
        class R: pass
        return R()
    monkeypatch.setattr("subprocess.run", fake_run)
    ensure_collection("fma")
    assert calls and calls[0][:2] == ["wb-manager", "init"]


def test_add_warcs(monkeypatch, tmp_path):
    calls = []
    def fake_run(args, check):
        calls.append(args)
    monkeypatch.setattr("subprocess.run", fake_run)
    w1 = tmp_path / "a.warc.gz"; w1.write_bytes(b"x")
    w2 = tmp_path / "b.warc.gz"; w2.write_bytes(b"y")
    add_warcs("fma", [w1, w2])
    assert calls and calls[0][0:3] == ["wb-manager", "add", "fma"]
