# tests/test_scheduler.py
from pathlib import Path
from crawler.scheduler import run_once
from crawler.normalize import registrable_domain


def test_run_once_live_append_and_dead_wayback(monkeypatch, tmp_config, state, tmp_path, sample_csv, patch_templates):
    # 1) Fake CSV fetch (full run -> our sample)
    monkeypatch.setattr("crawler.scheduler.fetch_iosco_csv", lambda *a, **k: sample_csv)

    # 2) Liveness: mark live.example.co.nz as live; dead.example.nz as dead
    def fake_classify(urls, timeout, treat_4xx_as_live, max_workers):
        # map each URL to its domain status
        res = {}
        for u in urls:
            d = registrable_domain(u)
            if "live.example.co.nz" in u:
                res[d] = "live"
            elif "dead.example.nz" in u:
                res[d] = "dead"
        return res
    monkeypatch.setattr("crawler.scheduler.classify_domains", fake_classify)

    # 3) Heritrix: simulate job existence and collect effects
    created = {"live": [], "wb": []}
    class FakeHeri:
        def __init__(self, *a, **k): pass
        def job_exists(self, name): return False
        def append_seeds(self, job_name, seeds):
            print(f"append_seeds: job name: {job_name}, seeds: {seeds}")
            created["live"].append(("append", job_name, tuple(seeds)))
        def create_or_update_live_job(self, domain, seeds, cfg):
            print(f"create_or_update_live_job: domain {domain}, seeds: {seeds}, cfg {cfg}")
            created["live"].append(("create", domain, tuple(sorted(set(seeds)))))
            return f"live-{domain.replace('.','-')}"
        def create_wayback_job_with_seeds(self, domain, ts, url_seeds, cfg):
            print(f"create_wayback_job_with_seeds: domain {domain}, ts {ts}, url_seeds: {url_seeds}, cfg {cfg}")
            created["wb"].append((domain, ts, tuple(sorted(set(url_seeds)))))
            return f"wb-{domain.replace('.','-')}-{ts}"
    monkeypatch.setattr("crawler.scheduler.Heritrix", FakeHeri)

    # 4) Wayback CDX per-URL (latest 2)
    def fake_cdx(url, n, cdx_endpoint, base_params, rps):
        return ["20250102030405", "20241211121314"]
    monkeypatch.setattr("crawler.scheduler.cdx_latest_snapshots_for_url", fake_cdx)

    # 5) Pywb collection no-op
    monkeypatch.setattr("crawler.scheduler.ensure_collection", lambda *a, **k: None)

    # 6) Run once (full)
    run_once(tmp_config, state)

    # Check: live job created from provided URLs (seed list contains the exact CSV URL)
    assert any(op[0] == "create" and op[1] == "example.co.nz" for op in created["live"])
    # Check: dead URL was grouped into 2 timestamps for domain example.nz
    assert any(wb[0] == "example.nz" and wb[1] == "20250102030405" for wb in created["wb"])
    assert any(wb[0] == "example.nz" and wb[1] == "20241211121314" for wb in created["wb"])
