from __future__ import annotations
import os, time, re
from pathlib import Path
from typing import List, Dict, Optional
import logging
import subprocess

log = logging.getLogger(__name__)

class WBDownloader:
    """ Manages downloading the entire website from wayback machine
    using wayback machine downloader tool:
    https://github.com/StrawberryMaster/wayback-machine-downloader.git
    """
    def __init__(self, output_dir: str, concurrency: int):
        self.output_dir = Path(output_dir)
        self.concurrency = concurrency
        self._jobs = {}
        log.info("WBDownloader initialized with output_dir=%s, concurrency=%d",
                 self.output_dir, self.concurrency)

    def job_exists(self, job_name: str) -> bool:
        """ check if a wayback download job already exists (directory present) """
        return (self.output_dir / job_name).exists()

    def create_wayback_job(self, domain) -> str:
        """Create download job for domain, the job runs in a separate process.
        Returns the job name.
        """
        job_name = f"wb-{domain.replace('.', '-')}"
        job_dir = self.output_dir / job_name
        job_dir.mkdir(parents=True, exist_ok=True)

        cmd = ["wayback_machine_downloader", domain,
               "--directory", str(job_dir),
               "--concurrency", str(self.concurrency)]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
        self._jobs[job_name] = proc
        log.info("Started wayback download job %s for domain %s", job_name, domain)
        return job_name

    def get_job_status(self, job_name: str) -> str:
        """
        check job status.
        Returns: RUNNING | PAUSED | FINISHED | UNBUILT | UNKNOWN
        """
        if job_name not in self._jobs:
            return "UNBUILT"
        proc = self._jobs[job_name]
        retcode = proc.poll()
        if retcode is None:
            return "RUNNING"
        output = proc.stdout.read().strip()
        with open(self.output_dir / job_name / "download.log", "w", encoding="utf-8") as f:
            f.write(output)
        if 'Download finished' in output:
            return "FINISHED"
        if 'found 0 snapshots' in output:
            return "EMPTY"
        return "UNKNOWN"

    def destroy(self) -> None:
        """Terminate all running jobs."""
        for job_name, proc in self._jobs.items():
            if proc.poll() is None:
                proc.terminate()
                log.info("Terminated job %s", job_name)
        self._jobs.clear()
        log.info("All jobs terminated.")
