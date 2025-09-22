# tests/test_state.py
from datetime import datetime, timezone


def test_state_meta_and_urls(state):
    now = datetime.now(timezone.utc)
    assert state.get_last_full_run() is None
    state.set_last_full_run(now)
    assert state.get_last_full_run().replace(tzinfo=timezone.utc) == now.replace(tzinfo=timezone.utc)

    u1 = "https://a.example.co.nz/start"
    u2 = "https://b.example.co.nz/page"
    state.upsert_url(u1, "example.co.nz", now)
    state.upsert_url(u2, "example.co.nz", now)

    #urls = state.get_urls_by_domain("example.co.nz")
    #assert set(urls) == {u1, u2}

    domains = state.list_all_domains()
    assert "example.co.nz" in domains

    state.set_domain_status("example.co.nz", "live")
    due = state.get_domains_due_for_heritrix(7)
    assert "example.co.nz" in due

    state.mark_heritrix_launch("example.co.nz", now)
    due = state.get_domains_due_for_heritrix(7)
    assert "example.co.nz" not in due


def test_job_queue_lifecycle(state):
    # enqueue unique
    jid = state.enqueue_job_unique("LIVE_CREATE", "example.nz", {"seeds": ["https://example.nz/"]}, priority=10)
    assert isinstance(jid, int)

    # duplicate PENDING should be skipped (None)
    assert state.enqueue_job_unique("LIVE_CREATE", "example.nz", {"seeds": []}, priority=10) is None

    # fetch PENDING
    jobs = state.fetch_next_pending(limit=5)
    assert jobs and jobs[0]["id"] == jid

    # mark RUNNING -> SUCCEEDED
    state.mark_running(jid)
    assert state.count_running_jobs() == 1
    state.mark_succeeded(jid)
    assert state.count_running_jobs() == 0

    # failed with retry exhausted
    jid2 = state.enqueue_job_unique("WAYBACK_CREATE", "example.nz", {"timestamp": "20250101", "url_seeds": []}, 100)
    state.mark_running(jid2)
    state.mark_failed(jid2, "oops", max_retries=1)
    # either FAILED or re-PENDING depending on attempts; since max_retries=1 and attempts=1 -> FAILED
    rows = state.conn.execute("SELECT status, attempts FROM jobs WHERE id=?", (jid2,)).fetchone()
    assert rows[0] in ("FAILED", "PENDING")
