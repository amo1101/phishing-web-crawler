from __future__ import annotations
import os, time, re
from pathlib import Path
from typing import List, Dict, Optional
import logging, requests
import traceback

log = logging.getLogger(__name__)

class BrowsertrixClient:
    """refer to https://docs.browsertrix.com/api/"""
    def __init__(self, base_url: str, username: str, password: str, org: str = "", collection: str = ""):
        self.base = base_url.rstrip("/")
        self.auth = (username, password)
        self.org = org
        self.org_id: str = ""
        self.org_slug: str = ""
        self.collection = collection
        self.collection_id: str = ""
        self.token: Optional[str] = None
        self.headers: Dict[str, str] = {}
        log.info("BrowsertrixClient initialized: base=%s", self.base)
        self._login()

    def _get_collection_id(self):
        """
        get collection id for the given collection name.
        """
        path = f"{self.base}/api/orgs/{self.org_id}/collections?name={requests.utils.quote(self.collection)}"
        try:
            resp = requests.get(path, headers=self.headers, timeout=15)
            body = resp.json()
        except Exception as e:
            log.error("Failed to get collections: %s", str(e))
            return

        items = body.get("items")
        if items is None:
            return
        self.collection_id = items[0].get("id", "")

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
        log.info("Authenticated, token acquired")
        log.info("self.org %s, self.collection:%s", self.org, self.collection)
        org_info = next((org for org in data.get("user_info").get("orgs") if org.get("name") == self.org), None)
        self.org_id = org_info.get("id") if org_info else None
        self.org_slug = org_info.get("slug") if org_info else None
        self.headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        self._get_collection_id()
        log.info("org_id %s, org_slug %s, collection_id %s", self.org_id, self.org_slug, self.collection_id)

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
        """Create and start a crawl job for the given URL.
        Crawl scope default to prefix, but for the following URLs, use page scope:
        1) facebook.com, twitter.com, instagram.com, linkedin.com,
           pinterest.com, play.google.com, tiktok.com, youtube.com.
        2) URLs with parameters.
        TODO: for pages that requires login, we basically cannot crawl, not sure whether Browser Profile could work around it or not.
        """
        scope = "prefix"
        if re.search(r"https?://(www\.)?(facebook|twitter|discord|x|instagram|linkedin|pinterest|tiktok|youtube|play.google)(\.com)?", url):
            scope = "page"
            log.debug("Special handling for social media site: %s", url)

        # if it is a page with params, set scope to "page"
        if re.search(r"\?.+=.+", url):
            scope = "page"
            log.debug("Setting scope to 'page' for URL with params: %s", url)

        crawl_setting = {
            "runNow": True,
            "name": f"crawl-{url}",
            "description": job_desc,
            "schedule": job_setting['frequency'],
            "crawlTimeout": job_setting['max_time'],
            "maxCrawlSize": job_setting['max_size'],
            "autoAddCollections": [self.collection_id],
            "config": {
            "seeds": [{"url": url}],
            "scopeType": scope,
            "exclude": [item.strip() for item in job_setting['exclude'].split(",")] if job_setting.get("exclude") else [],
            "blockAds": True
            }
        }
        log.debug(f'crawlsetting: {crawl_setting}')
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

    def resume_job(self, job_name: str) -> Dict:
        """
        Resume a job with given job_name
        """
        if not self.token:
            self._login()
        path = f"/api/orgs/{self.org_id}/crawlconfigs/{job_name}/run"
        resp = self._request("post", path)
        return resp.json()

    def _convert_status(self, state: str) -> str:
        """Convert Browsertrix crawl state to job status."""
        mapping = {
            "PAUSED": "STOPPED",
            "PAUSED_STORAGE_QUOTA_REACHED": "STOPPED",
            "PAUSED_TIME_QUOTA_REACHED": "STOPPED",
            "PAUSED_ORG_READONLY": "STOPPED",
            "STARTING": "RUNNING",
            "WAITING_CAPACITY": "RUNNING",
            "WAITING_ORG_LIMIT": "RUNNING",
            "WAITING_DEDUPE_INDEX": "RUNNING",
            "RUNNING": "RUNNING",
            "PENDING-WAIT": "RUNNING",
            "GENERATE-WACZ": "RUNNING",
            "UPLOADING-WACZ": "RUNNING",
            "CANCELED": "CANCELED",
            "FAILED": "FAILED",
            "FAILED_NOT_LOGGED_IN": "FAILED",
            "SKIPPED_STORAGE_QUOTA_REACHED": "STOPPED",
            "SKIPPED_TIME_QUOTA_REACHED": "STOPPED",
            "COMPLETE": "FINISHED",
            "STOPPED_BY_USER": "STOPPED",
            "STOPPED_PAUSE_EXPIRED": "STOPPED",
            "STOPPED_STORAGE_QUOTA_REACHED": "STOPPED",
            "STOPPED_TIME_QUOTA_REACHED": "STOPPED",
            "STOPPED_ORG_READONLY": "STOPPED"
        }
        return mapping.get(state.upper(), "UNKNOWN")

    def get_job_status(self, job_name: str) -> str:
        """
        Get the status of the lastest crawl with crawlconfig specified by job_name
        """
        job = self.list_crawlconfig(cid=job_name)
        #log.debug("Job config for %s: %s", job_name, job)
        if not job:
            log.error("Crawl job not found: %s", job_name)
            return {"status":"FAILED"}
        state = job.get("lastCrawlState") if job.get("lastCrawlState") else "UNKNOWN"
        crawl_count = job.get("crawlSuccessfulCount", 0)
        crawl_pages = job.get("lastCrawlStats").get("done", 0) if job.get("lastCrawlStats") else 0
        return {"status":self._convert_status(state),
                "crawl_count": crawl_count,
                "file_count":crawl_pages}

    def rebuild_job_info(self) -> List[Dict]:
        """
        Get all crawl jobs for the organization.
        """
        jobs = []
        try:
            log.info("Rebuilding browsertrix job info...")
            configs = self.list_crawlconfigs()
            log.info("Retrieved %d crawl configs", len(configs))
            for config in configs:
                jobs.append({"job_name": config.get("id"),
                             "desc": config.get("description"),
                             "url": config.get("firstSeed"),
                             "status": self._convert_status(config.get("lastCrawlState") \
                                                            if config.get("lastCrawlState") else "UNKNOWN"),
                             "crawl_count": config.get("crawlSuccessfulCount", 0),
                             "file_count": config.get("lastCrawlStats").get("done") \
                                if config.get("lastCrawlStats") else 0})
            log.debug("Rebuilding browsertrix job info done")
        except Exception as e:
            log.error("Failed to retrieve crawl jobs: %s, config: %s", str(e), config)
            log.error("Traceback: %s", traceback.format_exc())
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

    def update_crawlconfig(self, cid: str, crawl_config: Dict) -> Dict:
        """
        Update an existing crawl config for the given organization.
        """
        # ensure authenticated
        if not self.token:
            self._login()
        path = f"/api/orgs/{self.org_id}/crawlconfigs/{cid}"
        resp = self._request("patch", path, json=crawl_config)
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

    def list_crawlconfig(self, cid: str = ""):
        """
        List a crawl config
        """
        if not self.token:
            self._login()

        path = f"/api/orgs/{self.org_id}/crawlconfigs/{cid}"
        try:
            resp = self._request("get", path)
            return resp.json()
        except Exception as e:
            log.error("Failed to list crawl configs: %s", str(e))
            return None

    def list_crawl(self, crawl_id: str = ""):
        """
        List a crawl
        """
        if not self.token:
            self._login()

        path = f"/api/orgs/{self.org_id}/crawls/{crawl_id}"
        try:
            resp = self._request("get", path)
            return resp.json()
        except Exception as e:
            log.error("Failed to list crawl: %s", str(e))
            return None

    def list_crawlconfigs(self) -> List[Dict]:
        """
        List crawl configs for an organization
        """
        if not self.token:
            self._login()

        configs = []
        try:
            page = 1
            while True:
                path = f"/api/orgs/{self.org_id}/crawlconfigs?page={page}"
                resp = self._request("get", path)
                body = resp.json()
                if not body.get("items"):
                    break
                configs.extend(body["items"])
                page += 1
        except Exception as e:
            log.error("Failed to list crawl configs: %s", str(e))
            return []

        return configs

    def list_crawls(self) -> List[Dict]:
        """
        List crawls for an organization
        """
        if not self.token:
            self._login()

        crawls = []
        try:
            page = 1
            while True:
                path = f"/api/orgs/{self.org_id}/crawls?page={page}"
                resp = self._request("get", path)
                body = resp.json()
                if not body.get("items"):
                    break
                crawls.extend(body["items"])
                page += 1
        except Exception as e:
            log.error("Failed to list crawls: %s", str(e))
            return []

        return crawls

    def purge_all_crawls(self, crawl_ids: list[str] = []):
        """
        Remove all crawls
        """
        if not crawl_ids:
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

    def add_crawl_to_collection(self, crawl_ids: List[str]) -> Dict:
        """
        Add a crawls to a collection
        """
        if not self.token:
            self._login()
        path = f"/api/orgs/{self.org_id}/collections/{self.collection_id}/add"
        resp = self._request("post", path, json={"crawlIds": crawl_ids})
        return resp.json()
