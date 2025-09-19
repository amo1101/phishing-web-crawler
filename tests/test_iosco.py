# tests/test_iosco.py
from pathlib import Path
from datetime import date
from crawler.iosco import fetch_iosco_csv


def test_fetch_iosco_csv_full(tmp_path, fake_requests):
    csv_root = tmp_path / "iosco"
    base = "https://www.iosco.org/i-scan/?export-to-csv"
    params = "&SUBSECTION=main&NCA_ID=64"
    fake_requests.register_get(base + params, fake_requests.FakeResp(
        status=200, content=b"URL\nhttps://example.com/\n",
        headers={"Content-Type": "text/csv"}, stream=True
    ))
    out = fetch_iosco_csv(csv_root, None, None, nca_id=64, subsection="main", timeout=10)
    assert out.exists()
    assert out.read_text(encoding="utf-8").startswith("URL")


def test_fetch_iosco_csv_incremental(tmp_path, fake_requests):
    csv_root = tmp_path / "iosco"
    base = "https://www.iosco.org/i-scan/?export-to-csv"
    params = "&SUBSECTION=main&NCA_ID=64&ValidationDateStart=2025-09-01&ValidationDateEnd=2025-09-13"
    fake_requests.register_get(base + params, fake_requests.FakeResp(
        status=200, text="URL\nhttps://example.org/\n",
        headers={"Content-Type": "text/csv"}, stream=True
    ))
    out = fetch_iosco_csv(csv_root, date(2025, 9, 1), date(2025, 9, 13), nca_id=64, subsection="main")
    assert out.exists()
    assert "2025-09-01_to_2025-09-13" in out.name
    assert "example.org" in out.read_text(encoding="utf-8")
