from __future__ import annotations
import os, time, re
from pathlib import Path
from typing import List, Dict, Optional
import logging
import subprocess
import csv

log = logging.getLogger(__name__)

def write_csv_file(csv_file, data, fieldnames=None):
    """write to csv file"""
    if len(data) == 0:
        return
    is_empty = os.stat(csv_file).st_size == 0 if \
        os.path.isfile(csv_file) else True
    try:
        with open(csv_file, 'a', newline='', encoding='utf-8') as file:
            if fieldnames is None:
                fieldnames = data[0].keys()
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if is_empty:
                writer.writeheader()
            writer.writerows(data)
    except Exception as e:
        log.error(f"Error writing to CSV file {csv_file}: {e}")
        raise

def read_csv_file(csv_file, fieldnames=None):
    """read data from csv file"""
    result = []
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file, fieldnames=fieldnames)
            for row in reader:
                result.append(row)
    except FileNotFoundError:
        log.warning("File not found.")
        return []
    except Exception as e:
        log.error(f"Error reading from CSV file {csv_file}: {e}")
        raise
    return result

class WBDownloader:
    """ Manages downloading the entire website from wayback machine
    using wayback machine downloader tool:
    https://github.com/StrawberryMaster/wayback-machine-downloader.git
    """
    def __init__(self, downloader: str, output_dir: str, concurrency: int):
        self.downloader = downloader
        self.output_dir = Path(output_dir)
        self._status_log = output_dir + os.sep + 'wb_download.csv'
        self.concurrency = concurrency
        self._jobs = {}
        self._finished_jobs = {}
        log.info("WBDownloader initialized with output_dir=%s, concurrency=%d",
                 self.output_dir, self.concurrency)

    def job_finished(self, job_name: str) -> bool:
        """ check if a wayback download job already exists (directory present) """
        if self._job_status is None:
            job_st = read_csv_file(self._status_log)
            self._finished_jobs = {r['job']:r['status'] for r in job_st}
        return job_name in self._finished_jobs

    def create_wayback_job(self, domain) -> str:
        """Create download job for domain, the job runs in a separate process.
        Returns the job name.
        """
        job_name = f"wb-{domain.replace('.', '-')}"
        if self.job_finished(job_name):
            return job_name
        job_dir = self.output_dir / job_name
        job_dir.mkdir(parents=True, exist_ok=True)

        cmd = [self.downloader, domain, str(job_dir), str(self.concurrency)]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
        self._jobs[job_name] = proc
        log.info("Started wayback download job %s for domain %s", job_name, domain)
        return job_name

    def get_job_status(self, job_name: str) -> str:
        """
        check job status.
        Returns: RUNNING | PAUSED | FINISHED | UNBUILT | UNKNOWN
        """
        if self.job_finished(job_name):
            return "FINISHED"

        if job_name not in self._jobs:
            return "UNBUILT"

        proc = self._jobs[job_name]
        retcode = proc.poll()
        if retcode is None:
            return "RUNNING"
        ret = proc.stdout.read().strip()
        self._finished_jobs['job_name'] = ret
        write_csv_file(self._status_log, [{"job":job_name,"status":ret}])
        return ret

    def destroy(self) -> None:
        """Terminate all running jobs."""
        for job_name, proc in self._jobs.items():
            if proc.poll() is None:
                proc.terminate()
                log.info("Terminated job %s", job_name)
        self._jobs.clear()
        log.info("All jobs terminated.")
