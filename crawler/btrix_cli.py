from __future__ import annotations
import os, time, re
from pathlib import Path
from typing import List, Dict, Optional
import logging, requests

log = logging.getLogger(__name__)

class BrowsertrixClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base = base_url.rstrip("/")
        self.auth = (username, password)
        log.info("BrowsertrixClient initialized: base=%s", self.base)

    # --- LIVE job: provided URLs + domain SURT scope ---
    def create_job(self, url: str,  crawler_setting: Dict) -> str:
        pass

    def get_job_status(self, job_name: str) -> str:
        """
        Best-effort: query job page and regex out status label.
        Returns: RUNNING | PAUSED | FINISHED | UNBUILT | UNKNOWN
        """
        pass
