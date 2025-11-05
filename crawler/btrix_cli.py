from __future__ import annotations
import os, time, re
from pathlib import Path
from typing import List, Dict, Optional
import logging, requests

log = logging.getLogger(__name__)

class BrowsertrixClient:
    """refer to https://docs.browsertrix.com/api/"""
    def __init__(self, base_url: str, username: str, password: str):
        self.base = base_url.rstrip("/")
        self.auth = (username, password)
        self.org_id: str = ""
        self.token: Optional[str] = None
        self.headers: Dict[str, str] = {}
        log.info("BrowsertrixClient initialized: base=%s", self.base)

    def _login(self) -> None:
        """Authenticate and store bearer token."""
        login_url = f"{self.base}/api/auth/jwt/login"
        payload = {"username": self.auth[0], "password": self.auth[1]}
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        log.debug("Logging in to Browsertrix: %s", login_url)
        resp = requests.post(login_url, data=payload, headers=headers, timeout=15)
        if not resp.ok:
            log.error("Login failed: %s %s", resp.status_code, resp.text)
            resp.raise_for_status()
        data = resp.json()
        token = data.get("access_token")
        if not token:
            raise RuntimeError("Login succeeded but no access token found in response")
        self.token = token
        self.org_id = data.get("user_info").get("orgs")[0].get("id") # use only one organization
        self.headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        log.info("Authenticated, token acquired")

    def _request(self, method: str, path: str, retry: bool = True, **kwargs) -> requests.Response:
        """Make request to API, auto-login on 401 and retry once."""
        url = f"{self.base}{path}"
        hdrs = kwargs.pop("headers", {})
        # merge headers with auth headers (self.headers has precedence)
        merged = {**hdrs, **self.headers}
        resp = requests.request(method, url, headers=merged, timeout=30, **kwargs)
        if resp.status_code == 401 and retry:
            # try to re-authenticate and retry once
            log.debug("401 received, attempting re-login")
            self._login()
            return self._request(method, path, retry=False, headers=hdrs, **kwargs)
        # raise on other http errors
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            log.error("API request failed %s %s: %s", method.upper(), url, resp.text)
            raise
        return resp

    def create_job(self, url: str,  job_desc: str, job_setting: Dict) -> str:
        """Create and start a crawl job for the given URL. """
        crawl_setting = {
            "schedule": "", # TBD
            "runNow": True,
            "name": f"crawl-{url}",
            "description": job_desc,
            "config": {
                "seeds": [{"url": url}],
                "scopeType": "page" # TBD
            }
        }
        job_name = ""
        try:
            resp = self.add_crawlconfig(crawl_setting)
            added = resp.get("added")
            if added:
                job_name = resp.get("id")
                log.debug("Crawl job created: %s for URL: %s", job_name, url)
            else:
                log.error("Failed to add crawl config: %s", resp)
        except Exception as e:
            log.error("Failed to create crawl job for %s: %s", url, str(e))
        return job_name

    def _convert_status(self, state: str) -> str:
        """Convert Browsertrix crawl state to job status."""
        mapping = {
            "RUNNING": "RUNNING",
            "COMPLETE": "FINISHED",
            "WAITING": "RUNNING",
            "STARTING": "RUNNING"
        }
        return mapping.get(state.upper(), "UNKNOWN")

    def get_job_status(self, job_name: str) -> str:
        """
        Get the status of a crawl job by name.
        """
        job = self.list_crawlconfigs(cid=job_name)
        if not job:
            log.error("Crawl job not found: %s", job_name)
            return {"status":"FAILED"}
        state = job[0].get("lastCrawlState", "UNKNOWN")
        crawl_count = job[0].get("crawlSuccessfulCount", 0)
        crawl_pages = job[0].get("lastCrawlStats").get("done")
        return {"status":self._convert_status(state),
                "crawl_count": crawl_count,
                "file_count":crawl_pages}

    def rebuild_job_info(self) -> List[Dict]:
        """
        Get all crawl jobs for the organization.
        """
        jobs = []
        try:
            configs = self.list_crawlconfigs()
            for config in configs:
                jobs.append({"job_name": config.get("id"),
                             "desc": config.get("description"),
                             "url": config.get("firstSeed"),
                             "status": self._convert_status(config.get("lastCrawlState")),
                             "crawl_count": config.get("crawlSuccessfulCount", 0),
                             "file_count": config.get("lastCrawlStats").get("done")})
            log.debug("Retrieved %d crawl configs", len(jobs))
        except Exception as e:
            log.error("Failed to retrieve crawl jobs: %s", str(e))
        return jobs

    def add_crawlconfig(self, crawl_config: Dict) -> Dict:
        """
        Add a new crawl config for the given organization.
        """
        # ensure authenticated
        if not self.token:
            self._login()
        path = f"/api/orgs/{self.org_id}/crawlconfigs/"
        resp = self._request("post", path, json=crawl_config)
        return resp.json()

    def del_crawlconfig(self, cid: str) -> Dict:
        """
        Delete a crawl config
        """
        # ensure authenticated
        if not self.token:
            self._login()
        path = f"/api/orgs/{self.org_id}/crawlconfigs/{cid}"
        resp = self._request("delete", path)
        return resp.json()

    def list_crawlconfigs(self, cid: str = "") -> List[Dict]:
        """
        List crawl configs for an organization
        """
        if not self.token:
            self._login()

        path = f"/api/orgs/{self.org_id}/crawlconfigs"
        if cid != "":
            path = f"/api/orgs/{self.org_id}/crawlconfigs/{cid}"
        try:
            resp = self._request("get", path)
            body = resp.json()
        except Exception as e:
            log.error("Failed to list crawl configs: %s", str(e))
            return []

        if cid != "":
            return [body]
        items = body.get("items")
        if items is None:
            return []
        return items

    def list_crawls(self, crawl_id: str = "") -> List[Dict]:
        """
        List crawls for an organization
        """
        if not self.token:
            self._login()

        path = f"/api/orgs/{self.org_id}/crawls"
        if crawl_id != "":
            path = f"/api/orgs/{self.org_id}/crawls/{crawl_id}"
        try:
            resp = self._request("get", path)
            body = resp.json()
        except Exception as e:
            log.error("Failed to list crawls: %s", str(e))
            return []

        if crawl_id != "":
            return [body]
        items = body.get("items")
        if items is None:
            return []
        return items

    def purge_all_crawls(self):
        """
        Remove all crawls, only for test
        """
        crawls = self.list_crawls()
        crawl_ids = [c["id"] for c in crawls]
        if len(crawl_ids) > 0:
            path = f"/api/orgs/{self.org_id}/all-crawls/delete"
            self._request("post", path, json={"crawl_ids": crawl_ids})

    def purge_all_crawlconfigs(self):
        """
        Remove all crawlconfigs, only for test
        """
        configs = self.list_crawlconfigs()
        for config in configs:
            self.del_crawlconfig(config["id"])
