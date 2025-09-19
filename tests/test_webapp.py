# tests/test_webapp.py
from crawler.webapp import create_app


def test_webapp_api_and_html(monkeypatch, tmp_config, state, jobs_dir, patch_templates):
    # Seed state with a domain row
    state.conn.execute(
        "INSERT OR REPLACE INTO domains(domain,last_live_status,last_seen,last_heritrix_launch,job_kind,wayback_timestamps)"
        " VALUES (?,?,?,?,?,?)",
        ("example.nz", "live", "2025-09-13T00:00:00Z", "2025-09-12T00:00:00Z", "live", "20250102030405,20241211121314"),
    )

    # Fake Heritrix for status
    class FakeHeri:
        def __init__(self, *a, **k): pass
        def job_exists(self, name): return True
        def get_job_status(self, name): return "RUNNING"

    monkeypatch.setattr("crawler.webapp.Heritrix", FakeHeri)
    app = create_app(tmp_config["state_db"], tmp_config["heritrix"], tmp_config["web"]["basic_auth"])
    client = app.test_client()

    r = client.get("/api/domains")
    assert r.status_code == 200
    data = r.get_json()
    assert data[0]["domain"] == "example.nz"
    assert data[0]["live_status"] == "RUNNING"
    assert data[0]["wb_status"] == ["RUNNING", "RUNNING"]

    html = client.get("/").data.decode("utf-8")
    assert "FMA Crawl Status" in html and "example.nz" in html
