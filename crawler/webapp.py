from __future__ import annotations
import os
from flask import Flask, jsonify, render_template_string, request, Response
from datetime import datetime
from .state import State
from .heritrix import Heritrix

TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>FMA Crawl Status</title>
  https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css
</head>
<body class="bg-light">
<div class="container-fluid py-4">
  <h1 class="mb-3">FMA Crawl Status</h1>
  <p class="text-muted">Last updated: {{ now }}</p>
  <table class="table table-sm table-striped table-hover align-middle">
    <thead class="table-dark">
      <tr>
        <th>Domain</th>
        <th>Liveness</th>
        <th>Last Seen</th>
        <th>Job Kind</th>
        <th>Live Job</th>
        <th>Live Status</th>
        <th>Wayback Jobs</th>
        <th>Wayback Status</th>
        <th>Last Heritrix Launch</th>
      </tr>
    </thead>
    <tbody>
      {% for row in rows %}
      <tr>
        <td><code>{{ row.domain }}</code></td>
        <td>
          {% if row.last_live_status == 'live' %}
            <span class="badge bg-success">live</span>
          {% elif row.last_live_status == 'dead' %}
            <span class="badge bg-secondary">dead</span>
          {% else %}
            <span class="badge bg-light text-dark">unknown</span>
          {% endif %}
        </td>
        <td>{{ row.last_seen or '' }}</td>
        <td>{{ row.job_kind or '' }}</td>
        <td><code>{{ row.live_job or '' }}</code></td>
        <td>{{ row.live_status or '' }}</td>
        <td style="max-width: 260px;">
          {% for j in row.wb_jobs %}
            <div><code>{{ j }}</code></div>
          {% endfor %}
        </td>
        <td>
          {% for s in row.wb_status %}
            <div>{{ s }}</div>
          {% endfor %}
        </td>
        <td>{{ row.last_heritrix_launch or '' }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>
</body>
</html>
"""

def create_app(db_path: str, heritrix_cfg: dict, auth: dict | None = None) -> Flask:
    app = Flask(__name__)
    st = State(db_path)
    heri = Heritrix(
        base_url=heritrix_cfg["base_url"],
        username=heritrix_cfg["username"],
        password=heritrix_cfg["password"],
        jobs_dir=heritrix_cfg["jobs_dir"],
        tls_verify=heritrix_cfg.get("tls_verify", True)
    )
    basic_auth = auth or {"enabled": False}

    def _check_auth():
        if not basic_auth.get("enabled"):
            return True
        u = request.authorization.username if request.authorization else None
        p = request.authorization.password if request.authorization else None
        return (u == basic_auth.get("username") and p == basic_auth.get("password"))

    def _auth_required():
        return Response("Authentication required", 401, {"WWW-Authenticate": 'Basic realm="FMA"'})

    @app.route("/api/domains")
    def api_domains():
        if not _check_auth():
            return _auth_required()
        rows = st.conn.execute("""
            SELECT domain, last_live_status, last_seen, last_heritrix_launch, job_kind, wayback_timestamps
            FROM domains ORDER BY domain
        """).fetchall()
        data = []
        for (domain, last_live_status, last_seen, last_launch, job_kind, wb_stamps) in rows:
            live_job = f"live-{domain.replace('.', '-')}"
            live_status = heri.get_job_status(live_job) if heri.job_exists(live_job) else "NONE"
            wb_jobs, wb_status = [], []
            if wb_stamps:
                for ts in wb_stamps.split(","):
                    j = f"wb-{domain.replace('.', '-')}-{ts}"
                    wb_jobs.append(j)
                    wb_status.append(heri.get_job_status(j) if heri.job_exists(j) else "NONE")
            data.append({
                "domain": domain,
                "last_live_status": last_live_status,
                "last_seen": last_seen,
                "last_heritrix_launch": last_launch,
                "job_kind": job_kind,
                "live_job": live_job if heri.job_exists(live_job) else None,
                "live_status": live_status,
                "wb_jobs": wb_jobs,
                "wb_status": wb_status,
            })
        return jsonify(data)

    @app.route("/")
    def index():
        if not _check_auth():
            return _auth_required()
        # Reuse API data for rendering
        data = app.test_client().get("/api/domains").get_json()
        return render_template_string(TEMPLATE, rows=data, now=datetime.utcnow().isoformat(timespec="seconds")+"Z")

    return app

if __name__ == "__main__":
    # Quick manual run: python -m fma.webapp
    from .config import Config
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    cfg = Config.load(args.config)
    app = create_app(
        db_path=cfg["state_db"],
        heritrix_cfg=cfg["heritrix"],
        auth=cfg["web"]["basic_auth"]
    )
    app.run(host=cfg["web"]["host"], port=cfg["web"]["port"])
