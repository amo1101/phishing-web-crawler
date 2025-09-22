# tests/conftest.py
from __future__ import annotations
from pathlib import Path
import types
import json
import pytest
from datetime import datetime, timezone

from crawler.config import Config
from crawler.state import State


@pytest.fixture
def tmp_config(tmp_path) -> Config:
    cfg = {
        "state_db": str(tmp_path / "state.db"),
        "warc_root": str(tmp_path / "warcs"),
        "schedule": {
            "daily_run_time": "02:00",
            "heritrix_job_interval_days": 7,
        },
        "liveness": {
            "timeout_seconds": 2,
            "treat_http_4xx_as_live": True,
            "max_parallel_probes": 4,
        },
        "heritrix": {
            "base_url": "https://localhost:8443",
            "username": "admin",
            "password": "admin",
            "jobs_dir": str(tmp_path / "jobs"),
            "max_toe_threads": 10,
            "robots_policy": "obey",
            "limits": {
                "max_time_seconds": 60,
                "max_documents": 1000,
                "max_bytes": 104857600,
            },
            "tls_verify": False,
        },
        "pywb": {
            "collection": "fma",
            "auto_index": True,
            "wb_manager_bin": "wb-manager",
        },
        "wayback": {
            "snapshots_per_domain": 2,  # used per-URL in our final code
            "rps": 2.0,
            "cdx_endpoint": "https://web.archive.org/cdx/search/cdx",
            "cdx_params": {
                "matchType": "exact",
                "output": "json",
                "fl": "timestamp,original,statuscode,digest",
                "filter": "statuscode:200",
                "collapse": "digest",
            },
        },
        "iosco": {
            "csv_root": str(tmp_path / "iosco"),
            "nca_id": 64,
            "subsection": "main",
            "request_timeout_seconds": 10,
        },
        "queue": {
            "max_parallel_jobs": 2,
            "reconcile_interval_seconds": 1,
            "max_retries": 1,
            "retry_backoff_seconds": 1,
        },
        "web": {
            "enable_status_page": True,
            "host": "127.0.0.1",
            "port": 8090,
            "basic_auth": {"enabled": False},
        },
    }
    return Config(cfg)


@pytest.fixture
def state(tmp_config) -> State:
    return State(tmp_config["state_db"])


@pytest.fixture
def jobs_dir(tmp_config) -> Path:
    p = Path(tmp_config["heritrix"]["jobs_dir"])
    p.mkdir(parents=True, exist_ok=True)
    return p


# --- Simple fake response object for requests.get/post ---
class FakeResp:
    def __init__(self, status=200, text="", content=b"", headers=None, json_data=None, stream=False):
        self.status_code = status
        self.text = text
        self._content = content
        self.headers = headers or {}
        self._json = json_data
        self._stream = stream
        self._iter = None
        if stream:
            buf = content if content else text.encode("utf-8")
            chunks = [buf[i:i+65536] for i in range(0, len(buf), 65536)]
            self._iter = iter(chunks)

    def raise_for_status(self):
        if not (200 <= self.status_code < 300):
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        if self._json is not None:
            return self._json
        return json.loads(self.text)

    def iter_content(self, chunk_size=65536):
        if self._iter is None:
            yield self._content if self._content else self.text.encode("utf-8")
        else:
            yield from self._iter

    def __enter__(self): return self
    def __exit__(self, *exc): return False


@pytest.fixture
def fake_requests(monkeypatch):
    """
    Registry-based stub for requests.get/post that supports ?params.
    """
    registry_get = {}
    registry_post = {}

    def _get(url, *args, **kwargs):
        # Build key with params (simple encoder for stable tests)
        if "params" in kwargs and kwargs["params"]:
            from urllib.parse import urlencode
            q = urlencode(kwargs["params"], doseq=True)
            key = f"{url}?{q}"
            print(f"--->key: {key} <---")
            return registry_get.get(key, registry_get.get(url, FakeResp(404, "not found")))
        return registry_get.get(url, FakeResp(404, "not found"))

    def _post(url, *args, **kwargs):
        return registry_post.get(url, FakeResp(200, "ok"))

    def register_get(url, resp: FakeResp):
        registry_get[url] = resp

    def register_post(url, resp: FakeResp):
        registry_post[url] = resp

    monkeypatch.setattr("requests.get", _get)
    monkeypatch.setattr("requests.post", _post)
    ns = types.SimpleNamespace(
        register_get=register_get, register_post=register_post, FakeResp=FakeResp
    )
    return ns


@pytest.fixture
def sample_csv(tmp_path) -> Path:
    p = tmp_path / "iosco.csv"
    p.write_text(
        "URL\n"
        "https://live.example.co.nz/start\n"
        "https://dead.example.nz/profile.php?id=123\n",
        encoding="utf-8",
    )
    return p


@pytest.fixture
def patch_templates(monkeypatch, tmp_path):
    """
    Patch Heritrix templates to real temp files containing tokens used in .replace().
    """
    live_t = tmp_path / "crawler-beans-live.cxml.j2"
    way_t = tmp_path / "crawler-beans-wayback.cxml.j2"
    live_t.write_text(
        """<beans>
           <bean id="metadata"><property name="jobName" value="${job_name}"/></bean>
           <bean id="crawlLimitEnforcer">
             <property name="maxTimeSeconds" value="${max_time_seconds}"/>
             <property name="maxDocumentsDownload" value="${max_documents}"/>
             <property name="maxBytesDownload" value="${max_bytes}"/>
           </bean>
           <bean id="scope"><!-- uses surts.txt in runtime --></bean>
         </beans>""",
        encoding="utf-8",
    )
    way_t.write_text(
        """<beans>
           <bean id="metadata"><property name="jobName" value="${job_name}"/></bean>
           <bean id="crawlLimitEnforcer">
             <property name="maxTimeSeconds" value="${max_time_seconds}"/>
             <property name="maxDocumentsDownload" value="${max_documents}"/>
             <property name="maxBytesDownload" value="${max_bytes}"/>
           </bean>
           <bean id="scope"><!-- timestamp=${timestamp}, domain=${domain} --></bean>
         </beans>""",
        encoding="utf-8",
    )
    monkeypatch.setattr("crawler.heritrix.LIVE_TEMPLATE", str(live_t))
    monkeypatch.setattr("crawler.heritrix.WAYBACK_TEMPLATE", str(way_t))
