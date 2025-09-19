# tests/test_jobqueue.py
import time
from crawler.jobqueue import JobQueueWorker, LIVE_CREATE, LIVE_RELAUNCH, WAYBACK_CREATE


def test_jobqueue_handlers(monkeypatch, tmp_config, state, jobs_dir, patch_templates):
    # Fake Heritrix methods
    created = {"live": [], "wb": [], "relaunch": []}

    class FakeHeri:
        def __init__(self, *a, **k): pass
        def job_exists(self, name): return False
        def create_or_update_live_job(self, domain, seeds, cfg):
            created["live"].append((domain, tuple(seeds))); return f"live-{domain.replace('.','-')}"
        def build_job(self, job_name): created["relaunch"].append(("build", job_name))
        def launch_job(self, job_name): created["relaunch"].append(("launch", job_name))
        def create_wayback_job_with_seeds(self, domain, ts, url_seeds, cfg):
            created["wb"].append((domain, ts, tuple(url_seeds))); return f"wb-{domain.replace('.','-')}-{ts}"
        def get_job_status(self, name): return "FINISHED"

    monkeypatch.setattr("crawler.jobqueue.Heritrix", FakeHeri)

    w = JobQueueWorker(tmp_config, state)

    # enqueue 3 jobs
    jid1 = state.enqueue_job_unique(LIVE_CREATE, "example.nz", {"seeds": ["https://example.nz/start"]}, 10)
    jid2 = state.enqueue_job_unique(LIVE_RELAUNCH, "example.nz", {}, 20)
    jid3 = state.enqueue_job_unique(WAYBACK_CREATE, "example.nz", {"timestamp":"20250102030405","url_seeds":["https://example.nz/p"]}, 30)

    # run one iteration manually: mark RUNNING and handle jobs
    # emulate a tiny slice of run_forever
    for job in state.fetch_next_pending(limit=3):
        state.mark_running(job["id"])
        w._handle_job(job)

    # reconcile: mark SUCCEEDED
    w._reconcile()
    rows = state.conn.execute("SELECT status FROM jobs ORDER BY id").fetchall()
    assert all(r[0] == "SUCCEEDED" for r in rows), rows

    # effects
    assert created["live"]
    assert created["wb"]
    assert created["relaunch"]  # build + launch called
    row = state.conn.execute(
    "SELECT wayback_timestamps FROM domains WHERE domain=?", ("example.nz",)
    ).fetchone()
    assert row and "20250102030405" in (row[0] or "")
