from __future__ import annotations
import os, time, re
from pathlib import Path
from typing import List, Dict, Optional
import logging
import subprocess
import asyncio
import multiprocessing

log = logging.getLogger(__name__)

class WBDownloader:
    """ Manages downloading the entire website from wayback machine
    using wayback machine downloader tool:
    https://github.com/StrawberryMaster/wayback-machine-downloader.git
    """
    def __init__(self, output_dir: str, concurrency: int):
        self.output_dir = Path(output_dir)
        self.concurrency = concurrency
        self._jobs = []
        self._job_status = {}
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

        def run_job(domain, job_dir, concurrency, job_name, job_status):
            job_status[job_name] = "RUNNING"
            cmd = [
                "wayback_machine_downloader",
                domain,
                "--directory", str(job_dir),
                "--concurrency", str(concurrency)
            ]
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = process.communicate()
            with open(job_dir / "download.log", "wb") as f:
                f.write(stdout)
            if process.returncode != 0:
                log.error("Job %s failed: %s", job_name, stderr.decode())
                job_status[job_name] = "FAILED"
            else:
                log.info("Job %s finished successfully.", job_name)
                if "Downloaded finished" in stdout.decode():
                    job_status[job_name] = "FINISHED"
                elif "No files found" in stdout.decode():
                    job_status[job_name] = "EMPTY"
                else:
                    job_status[job_name] = "UNKNOWN"

        # Use a multiprocessing.Manager dict for shared state
        if not hasattr(self, '_manager'):
            self._manager = multiprocessing.Manager()
            self._job_status = self._manager.dict(self._job_status)
        process = multiprocessing.Process(
            target=run_job,
            args=(domain, job_dir, self.concurrency, job_name, self._job_status)
        )
        process.start()
        self._jobs.append(process)
        return job_name

    def get_job_status(self, job_name: str) -> str:
        """
        check job status.
        Returns: RUNNING | PAUSED | FINISHED | UNBUILT | UNKNOWN
        """
        try:
            pass
        except Exception as e:
            log.debug(f"An exception occurred {e}")
            return "UNKNOWN"
