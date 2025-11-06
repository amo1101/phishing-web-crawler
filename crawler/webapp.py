from __future__ import annotations
import os
import logging
from .config import Config
import argparse
from flask import Flask, jsonify, request, Response, render_template
from datetime import datetime, timezone
from .state import State
from urllib.parse import urlencode

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

def create_app(db_path: str, auth: dict | None = None) -> Flask:
    """Create and configure the Flask web application."""
    app = Flask(__name__)
    app.jinja_env.add_extension('jinja2.ext.do')
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

    @app.template_filter('dateformat')
    def dateformat(value, format='%Y-%m-%d'):
        if value is None:
            return ''
        return value.strftime(format)

    @app.route("/api/jobs")
    def api_jobs():
        if not _check_auth():
            return _auth_required()
            
        page = int(request.args.get('page', 1))
        status = request.args.get('status')
        job_type = request.args.get('job_type')
        jurisdiction = request.args.get('jurisdiction')
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        
        jobs, total = st.get_filtered_jobs(
            page=page,
            status=status,
            job_type=job_type,
            jurisdiction=jurisdiction, 
            date_from=date_from,
            date_to=date_to
        )
        
        return jsonify({
            "jobs": jobs,
            "total": total,
            "page": page,
            "pages": (total + 19) // 20  # ceil(total/20)
        })

    @app.route("/")
    def index():
        if not _check_auth():
            return _auth_required()
            
        # Get filter params
        page = int(request.args.get('page', 1))
        status = request.args.get('status')
        job_type = request.args.get('job_type')
        jurisdiction = request.args.get('jurisdiction')
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        
        # Get filtered data
        data = app.test_client().get("/api/jobs?" + urlencode({
            'page': page,
            'status': status,
            'job_type': job_type,
            'jurisdiction': jurisdiction,
            'date_from': date_from, 
            'date_to': date_to
        })).get_json()
        
        # Get jurisdictions for filter dropdown
        jurisdictions = st.get_jurisdictions()
    
        return render_template(
            "index.html",
            rows=data['jobs'],
            total=data['total'],
            current_page=page,
            pages=data['pages'],
            jurisdictions=jurisdictions,
            filters={
                'status': status,
                'job_type': job_type,
                'jurisdiction': jurisdiction,
                'date_from': date_from,
                'date_to': date_to
            },
            now=datetime.now(timezone.utc).isoformat(timespec="seconds")+"Z"
        )

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
