# tests/test_heritrix.py
from pathlib import Path
from crawler.heritrix import Heritrix, _surt_for_registrable_domain
import re


def test_surt_for_registrable_domain():
    assert _surt_for_registrable_domain("example.co.nz") == "http://(nz,co,example,)/"
    assert _surt_for_registrable_domain("example.nz") == "http://(nz,example,)/"


def test_create_or_update_live_job(monkeypatch, tmp_config, jobs_dir, patch_templates):
    # avoid real HTTP
    monkeypatch.setattr("requests.post", lambda *a, **k: type("R", (), {"raise_for_status": lambda self: None})())
    h = Heritrix(tmp_config["heritrix"]["base_url"], "u", "p", str(jobs_dir), False)
    job = h.create_or_update_live_job(
        "example.nz",
        ["https://example.nz/start", "https://a.example.nz/foo"],
        tmp_config["heritrix"],
    )
    job_dir = jobs_dir / job
    assert job_dir.exists()

    seeds = (job_dir / "seeds.txt").read_text(encoding="utf-8")
    assert "https://example.nz/start" in seeds and "https://a.example.nz/foo" in seeds

    surts = (job_dir / "surts.txt").read_text(encoding="utf-8")
    assert surts.strip() == "http://(nz,example,)/"

    cxml = (job_dir / "crawler-beans.cxml").read_text(encoding="utf-8")
    assert "maxTimeSeconds" in cxml


def test_append_seeds(monkeypatch, tmp_config, jobs_dir, patch_templates):
    # Setup job dir manually
    job_name = "live-example-nz"
    (jobs_dir / job_name / "action").mkdir(parents=True)
    h = Heritrix(tmp_config["heritrix"]["base_url"], "u", "p", str(jobs_dir), False)
    h.append_seeds(job_name, ["https://new.example.nz/new"])
    files = list((jobs_dir / job_name / "action").glob("append-*.seeds"))
    assert files and "https://new.example.nz/new" in files[0].read_text(encoding="utf-8")


def test_create_wayback_job_with_seeds(monkeypatch, tmp_config, jobs_dir, patch_templates):
    monkeypatch.setattr("requests.post", lambda *a, **k: type("R", (), {"raise_for_status": lambda self: None})())
    h = Heritrix(tmp_config["heritrix"]["base_url"], "u", "p", str(jobs_dir), False)
    name = h.create_wayback_job_with_seeds(
        "example.nz",
        "20250102030405",
        ["https://example.nz/profile.php?id=123", "https://web.archive.org/web/20250102030405/https://example.nz/foo"],
        tmp_config["heritrix"],
    )
    job_dir = jobs_dir / name
    seeds = (job_dir / "seeds.txt").read_text(encoding="utf-8").splitlines()
    # seeds must be non-id_ replay URLs for traversal
    assert any(s.startswith("https://web.archive.org/web/20250102030405/https://example.nz/") for s in seeds)
    cxml = (job_dir / "crawler-beans.cxml").read_text(encoding="utf-8")
    assert "timestamp" in cxml and "domain" in cxml
