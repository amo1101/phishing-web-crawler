from __future__ import annotations
import os, time, re
from pathlib import Path
from typing import List, Dict, Optional
import logging, requests

log = logging.getLogger(__name__)

LIVE_TEMPLATE = "crawler/job_templates/crawler-beans-live.cxml.j2"
WAYBACK_TEMPLATE = "crawler/job_templates/crawler-beans-wayback.cxml.j2"

def _surt_for_registrable_domain(domain: str) -> str:
    # example.co.nz -> http://(nz,co,example,)/
    parts = domain.strip(".").split(".")
    rev = ",".join(reversed(parts)) + ","
    surt = f"http://({rev})/"
    log.debug("Built SURT for domain %s -> %s", domain, surt)
    return surt

class Heritrix:
    def __init__(self, base_url: str, username: str, password: str, jobs_dir: str, tls_verify: bool):
        self.base = base_url.rstrip("/")
        self.auth = (username, password)
        self.jobs_dir = Path(jobs_dir)
        self.tls_verify = tls_verify
        log.info("Heritrix client initialized: base=%s, jobs_dir=%s, verify=%s", self.base, self.jobs_dir, self.tls_verify)

    def _post(self, url: str, data: Dict[str, str]) -> None:
        log.debug("POST %s data=%s", url, data)
        r = requests.post(url, data=data, auth=self.auth, verify=self.tls_verify, timeout=60)
        log.debug("POST %s -> %s", url, r.status_code)
        r.raise_for_status()

    def add_job_dir(self, job_dir: Path) -> None:
        self._post(f"{self.base}/engine", {"action":"add", "addpath": str(job_dir)})

    def build_job(self, job_name: str) -> None:
        self._post(f"{self.base}/engine/job/{job_name}", {"action":"build"})

    def launch_job(self, job_name: str) -> None:
        self._post(f"{self.base}/engine/job/{job_name}", {"action":"launch"})
        log.info("Launched Heritrix job %s", job_name)

    def job_exists(self, job_name: str) -> bool:
        return (self.jobs_dir / job_name).exists()

    def append_seeds(self, job_name: str, seeds: List[str]) -> None:
        from datetime import datetime
        action_dir = self.jobs_dir / job_name / "action"
        action_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        p = action_dir / f"append-{stamp}.seeds"
        p.write_text("\n".join(sorted(set(seeds))) + "\n", encoding="utf-8")
        log.info("Appended %d seeds to %s: %s", len(seeds), job_name, p)

    # --- LIVE job: provided URLs + domain SURT scope ---
    def create_or_update_live_job(self, domain: str, seeds: List[str], cfg: Dict) -> str:
        job_name = f"live-{domain.replace('.', '-')}"
        job_dir = self.jobs_dir / job_name
        (job_dir / "action").mkdir(parents=True, exist_ok=True)
        (job_dir / "seeds.txt").write_text("\n".join(sorted(set(seeds))) + "\n", encoding="utf-8")
        (job_dir / "surts.txt").write_text(_surt_for_registrable_domain(domain) + "\n", encoding="utf-8")
        log.info("Prepared live job %s seeds=%d dir=%s", job_name, len(seeds), job_dir)

        cxml = (Path(LIVE_TEMPLATE).read_text(encoding="utf-8")
                .replace("${job_name}", job_name)
                .replace("${robots_policy}", cfg["robots_policy"])
                .replace("${max_time_seconds}", str(cfg["limits"]["max_time_seconds"]))
                .replace("${max_documents}", str(cfg["limits"]["max_documents"]))
                .replace("${max_bytes}", str(cfg["limits"]["max_bytes"]))
                .replace("${max_toe_threads}", str(cfg["max_toe_threads"])))
        (job_dir / "crawler-beans.cxml").write_text(cxml, encoding="utf-8")

        self.add_job_dir(job_dir); self.build_job(job_name); self.launch_job(job_name)
        return job_name

    # --- WAYBACK job: per-(domain,timestamp) with URL-level seeds ---
    def create_wayback_job_with_seeds(self, domain: str, ts: str, url_seeds: List[str], cfg: Dict) -> str:
        job_name = f"wb-{domain.replace('.', '-')}-{ts}"
        job_dir = self.jobs_dir / job_name
        (job_dir / "action").mkdir(parents=True, exist_ok=True)

        # non-id_ replay seeds for traversal
        replay_seeds = []
        for u in sorted(set(url_seeds)):
            if u.startswith("http://") or u.startswith("https://"):
                replay_seeds.append(f"https://web.archive.org/web/{ts}/{u}")
            elif u.startswith("https://web.archive.org/web/"):
                replay_seeds.append(u)
        (job_dir / "seeds.txt").write_text("\n".join(replay_seeds) + "\n", encoding="utf-8")
        log.info("Prepared wayback job %s ts=%s seeds=%d dir=%s", job_name, ts, len(replay_seeds), job_dir)

        cxml = (Path(WAYBACK_TEMPLATE).read_text(encoding="utf-8")
                .replace("${job_name}", job_name)
                .replace("${robots_policy}", cfg["robots_policy"])
                .replace("${max_time_seconds}", str(cfg["limits"]["max_time_seconds"]))
                .replace("${max_documents}", str(cfg["limits"]["max_documents"]))
                .replace("${max_bytes}", str(cfg["limits"]["max_bytes"]))
                .replace("${max_toe_threads}", str(cfg["max_toe_threads"]))
                .replace("${timestamp}", ts)
                .replace("${domain}", domain))
        (job_dir / "crawler-beans.cxml").write_text(cxml, encoding="utf-8")

        self.add_job_dir(job_dir); self.build_job(job_name); self.launch_job(job_name)
        return job_name

    def get_job_status(self, job_name: str) -> str:
        """
        Best-effort: query job page and regex out status label.
        Returns: RUNNING | PAUSED | FINISHED | UNBUILT | UNKNOWN
        """
        try:
            r = requests.get(f"{self.base}/engine/job/{job_name}",
                             auth=self.auth, verify=self.tls_verify, timeout=10)
            r.raise_for_status()
            # Heritrix UI contains status token; we try to capture it
            m = re.search(r"(?i)Status:\s*([A-Z]+)", r.text)
            return m.group(1) if m else "UNKNOWN"
        except Exception:
            return "UNKNOWN"
