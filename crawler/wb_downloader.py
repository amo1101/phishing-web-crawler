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
        self._output_dir = output_dir
        self._status_log = output_dir + os.sep + 'wb_download.csv'
        self._concurrency = concurrency
        self._jobs = {}
        log.info("WBDownloader initialized with output_dir=%s, concurrency=%d",
                 self._output_dir, self._concurrency)

    def create_job(self, url) -> str:
        """Create download job, the job runs in a separate process.
        Returns the job name.
        """
        job_name = f"wb-{time.strftime('%Y%m%d-%H%M%S')}-{len(self._jobs) + 1}"
        cmd = [self.downloader, url, str(self._concurrency), self._output_dir, job_name]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
        self._jobs[job_name] = (url, proc)
        log.info("Started wayback download job for url %s", url)
        return job_name

    def get_job_status(self, job_name: str) -> str:
        """
        check job status.
        Returns: RUNNING | FINISHED | FAILED
        """
        if job_name not in self._jobs:
            log.error("Job not found: %s", job_name)
            return "FAILED"

        proc = self._jobs[job_name][1]
        retcode = proc.poll()
        if retcode is None:
            return "RUNNING"

        if retcode != 0:
            log.error("Job %s failed with return code %d", job_name, retcode)
            self._jobs.pop(job_name)
            return "FAILED"

        # find "job_name: STATUS,files_downloaded" in stdout
        output = proc.stdout.read().strip()
        match = re.search(rf"{re.escape(job_name)}:\s*(\w+),(\d+)", output)
        if not match:
            log.error("Job %s finished but status line not found in output", job_name)
            self._jobs.pop(job_name)
            return "FAILED"
        status = match.group(1)
        files_downloaded = match.group(2)
        log.info("Job %s finished with status %s, files downloaded: %s",
                    job_name, status, files_downloaded)
        write_csv_file(self._status_log, [{"job_name":job_name,
                                            "url":self._jobs[job_name][0],
                                            "status":status,
                                            "files_downloaded":files_downloaded,
                                            "finished_at":time.strftime('%Y-%m-%d %H:%M:%S')}])
        self._jobs.pop(job_name)
        return status

    def rebuild_job_info(self) -> List[Dict]:
        """
        Get all download jobs info from status log.
        """
        return read_csv_file(self._status_log)

    def destroy(self) -> None:
        """Terminate all running jobs."""
        for _, proc in self._jobs.values():
            if proc.poll() is None:
                proc.terminate()
        self._jobs.clear()
        log.info("All jobs terminated.")
