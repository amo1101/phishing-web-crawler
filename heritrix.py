from __future__ import annotations
import os, time, re
from pathlib import Path
from typing import List, Dict, Optional
import requests

LIVE_TEMPLATE = "job_templates/crawler-beans-live.cxml.j2"
WAYBACK_TEMPLATE = "job_templates/crawler-beans-wayback.cxml.j2"

class Heritrix:
    def __init__(self, base_url: str, username: str, password: str, jobs_dir: str, tls_verify: bool):
        self.base = base_url.rstrip("/")
        self.auth = (username, password)
        self.jobs_dir = Path(jobs_dir)
        self.tls_verify = tls_verify

    def _post(self, url: str, data: Dict[str, str]) -> requests.Response:
        return requests.post(url, data=data, auth=self.auth, verify=self.tls_verify, allow_redirects=True)

    def add_job_dir(self, job_dir: Path) -> None:
        r = self._post(f"{self.base}/engine", {"action":"add", "addpath": str(job_dir)})
        r.raise_for_status()

    def build_job(self, job_name: str) -> None:
        r = self._post(f"{self.base}/engine/job/{job_name}", {"action":"build"})
        r.raise_for_status()

    def launch_job(self, job_name: str) -> None:
        r = self._post(f"{self.base}/engine/job/{job_name}", {"action":"launch"})
        r.raise_for_status()

    def create_or_update_live_job(self, domain: str, seeds: List[str], cfg: Dict) -> str:
        """
        Create/overwrite a job directory with a live-domain template and seeds.txt, then add/build/launch.
        Returns job name.
        """
        job_name = f"live-{domain.replace('.', '-')}"
        job_dir = self.jobs_dir / job_name
        action_dir = job_dir / "action"
        job_dir.mkdir(parents=True, exist_ok=True)
        action_dir.mkdir(parents=True, exist_ok=True)

        # seeds.txt
        (job_dir / "seeds.txt").write_text("\n".join(sorted(set(seeds))) + "\n", encoding="utf-8")

        # write crawler-beans.cxml from template with your knobs
        cxml = (Path(LIVE_TEMPLATE).read_text(encoding="utf-8")
                .replace("${job_name}", job_name)
                .replace("${robots_policy}", cfg["robots_policy"])
                .replace("${max_time_seconds}", str(cfg["limits"]["max_time_seconds"]))
                .replace("${max_documents}", str(cfg["limits"]["max_documents"]))
                .replace("${max_bytes}", str(cfg["limits"]["max_bytes"]))
                .replace("${max_toe_threads}", str(cfg["max_toe_threads"]))
               )
        (job_dir / "crawler-beans.cxml").write_text(cxml, encoding="utf-8")

        # Register or rescan
        self.add_job_dir(job_dir)
        self.build_job(job_name)
        self.launch_job(job_name)
        return job_name

    def create_wayback_job(self, domain: str, timestamps: List[str], cfg: Dict) -> List[str]:
        """
        For each timestamp, create a separate job targeting the snapshot on web.archive.org and launch it.
        Returns list of job names.
        """
        job_names = []
        for ts in timestamps:
            job_name = f"wb-{domain.replace('.', '-')}-{ts}"
            job_dir = self.jobs_dir / job_name
            action_dir = job_dir / "action"
            job_dir.mkdir(parents=True, exist_ok=True)
            action_dir.mkdir(parents=True, exist_ok=True)

            # Seeds: both http:// and https:// roots under that timestamp
            seeds = [
                f"https://web.archive.org/web/{ts}id_/http://{domain}/",
                f"https://web.archive.org/web/{ts}id_/https://{domain}/",
            ]
            (job_dir / "seeds.txt").write_text("\n".join(seeds) + "\n", encoding="utf-8")

            # Template
            cxml = (Path(WAYBACK_TEMPLATE).read_text(encoding="utf-8")
                    .replace("${job_name}", job_name)
                    .replace("${robots_policy}", cfg["robots_policy"])
                    .replace("${max_time_seconds}", str(cfg["limits"]["max_time_seconds"]))
                    .replace("${max_documents}", str(cfg["limits"]["max_documents"]))
                    .replace("${max_bytes}", str(cfg["limits"]["max_bytes"]))
                    .replace("${max_toe_threads}", str(cfg["max_toe_threads"]))
                    .replace("${timestamp}", ts)
                    .replace("${domain}", domain)
                   )
            (job_dir / "crawler-beans.cxml").write_text(cxml, encoding="utf-8")

            self.add_job_dir(job_dir)
            self.build_job(job_name)
            self.launch_job(job_name)
            job_names.append(job_name)
        return job_names

    def job_exists(self, job_name: str) -> bool:
        return (self.jobs_dir / job_name).exists()

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
