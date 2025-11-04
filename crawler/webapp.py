from __future__ import annotations
import os
import logging
from .config import Config
import argparse
from flask import Flask, jsonify, request, Response, render_template
from datetime import datetime, timezone
from .state import State

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

def create_app(db_path: str, auth: dict | None = None) -> Flask:
    """Create and configure the Flask web application."""
    app = Flask(__name__)
    st = State(db_path)
    basic_auth = auth or {"enabled": False}

    def _check_auth():
        if not basic_auth.get("enabled"):
            return True
        u = request.authorization.username if request.authorization else None
        p = request.authorization.password if request.authorization else None
        return (u == basic_auth.get("username") and p == basic_auth.get("password"))

    def _auth_required():
        return Response("Authentication required", 401, {"WWW-Authenticate": 'Basic realm="Phishing Web Crawler"'})

    @app.route("/api/jobs")
    def api_jobs():
        if not _check_auth():
            return _auth_required()
        log.debug("GET /api/jobs")
        rows = st.conn.execute("""
            SELECT url,
                type AS job_type,
                link,
                status,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS last_update
            FROM jobs ORDER BY created_at
        """).fetchall()
        log.debug(f"{len(rows)} jobs fetched")
        data = []
        for (url, job_type, link, status, created_at, last_update) in rows:
            data.append({
                "url": url,
                "type": job_type,
                "status": status,
                "created_at": created_at,
                "last_update": last_update
            })
        log.info("Returned status for %d url", len(data))
        return jsonify(data)

    @app.route("/")
    def index():
        if not _check_auth():
            return _auth_required()
        log.debug("GET /")
        # Reuse API data for rendering
        data = app.test_client().get("/api/jobs").get_json()
        return render_template("index.html", rows=data, now=datetime.now(timezone.utc).isoformat(timespec="seconds")+"Z")

    return app

if __name__ == "__main__":
    # Quick manual run: python -m fma.webapp
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    cfg = Config.load(args.config)
    app = create_app(
        db_path=cfg["state_db"],
        auth=cfg["web"]["basic_auth"]
    )
    app.run(host=cfg["web"]["host"], port=cfg["web"]["port"])
