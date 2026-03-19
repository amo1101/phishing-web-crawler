from __future__ import annotations
import argparse
from datetime import datetime
import os
from pathlib import Path
import logging, threading
from typing import Dict
from .logging_setup import setup_logging
from .btrix_cli import BrowsertrixClient
from .jobqueue import JobQueueWorker
from .config import Config
from .state import State
from .iosco import fetch_iosco_csv, parse_csv_url_info
from .liveness import classify_urls


def update_crawl_configs(cfg: Config):
    btrix = BrowsertrixClient(
        base_url=cfg["browsertrix"]["base_url"],
        username=cfg["browsertrix"]["username"],
        password=cfg["browsertrix"]["password"],
        org=cfg["browsertrix"]["org"],
        collection=cfg["browsertrix"]["collection"]
    )
    # update for all crawl configs for now, can be optimized to update only the ones with changes
    crawl_configs = btrix.list_crawlconfigs()
    cids = [c["id"] for c in crawl_configs]
    # any other config updates can be added to the config_update dict
    config_update = {"autoAddCollections": [btrix.collection_id]}
    for cid in cids:
        btrix.update_crawlconfig(cid, config_update)

def add_crawl_to_collection(cfg: Config):
    btrix = BrowsertrixClient(
        base_url=cfg["browsertrix"]["base_url"],
        username=cfg["browsertrix"]["username"],
        password=cfg["browsertrix"]["password"],
        org=cfg["browsertrix"]["org"],
        collection=cfg["browsertrix"]["collection"]
    )

    crawls = btrix.list_crawls()
    crawlIds = [c["id"] for c in crawls]
    btrix.add_crawl_to_collection(crawlIds)

# retry all cancelled or failed, canceled or stopped crawls
# you may want to resume crawls after adjust resources for k8s crawler pods
def retry_crawl_jobs(cfg: Config):
    state = State(cfg["state_db"])
    state.retry_live_crawl_jobs()

# Rebuild jobs info from existing jobs in Browsertrix and WBDownloader in case of state db loss
def rebuild_jobs_info(cfg: Config):
    state = State(cfg["state_db"])
    jq = JobQueueWorker(cfg, state)
    jq.rebuild_job_info()

# Permanently purge all crawls and crawl configs in Browsertrix, use with caution
def purge_all_crawl_jobs(cfg: Config):
    btrix = BrowsertrixClient(
        base_url=cfg["browsertrix"]["base_url"],
        username=cfg["browsertrix"]["username"],
        password=cfg["browsertrix"]["password"],
        org=cfg["browsertrix"]["org"],
        collection=cfg["browsertrix"]["collection"]
    )
    btrix.purge_all_crawls()
    btrix.purge_all_crawlconfigs()

# check livenss for all URLs daily,create a cron job for this
def check_liveness(cfg: Config):
    output_today = Path(cfg["tool"]["liveness_check_output"] + f"/{datetime.now().strftime('%Y%m%d')}")
    output_today.mkdir(parents=True, exist_ok=True)
    csv_file = fetch_iosco_csv(csv_root= output_today,
        start_date=None,
        end_date=None,
        nca_id=cfg["iosco"]["nca_id"],
        subsection=cfg["iosco"]["subsection"],
        timeout=int(cfg["iosco"]["request_timeout_seconds"]))
    url_info = parse_csv_url_info(csv_file)
    urls = list(url_info)
    url_status = classify_urls(
        urls=urls,
        timeout=cfg["liveness"]["timeout_seconds"],
        treat_4xx_as_live=cfg["liveness"]["treat_http_4xx_as_live"],
        max_workers=cfg["liveness"]["max_parallel_probes"],
    )
    # save the liveness result to a csv file
    output_file = output_today / "liveness_result.csv"
    with open(output_file, "w") as f:
        f.write("url,is_live,nca_id,nca_jurisdiction,nca_name,validation_date\n")
        for url, is_live in url_status.items():
            nca_id, nca_jurisdiction, nca_name, validation_date = url_info[url]
            f.write(f"{url},{is_live},{nca_id},{nca_jurisdiction},{nca_name},{validation_date}\n")
    print("Liveness check completed, results saved to %s", output_file)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", "-c", required=True, help="Path to config.yaml")
    ap.add_argument("--command", "-e", required=True, help="Command to execute")
    args = ap.parse_args()

    cfg = Config.load(args.config)
    setup_logging(cfg.data)
    log = logging.getLogger(__name__)
    log.info("Starting tool with config: %s", args.config)

    if args.command == "update_crawl_configs":
        update_crawl_configs(cfg)
    elif args.command == "add_crawl_to_collection":
        add_crawl_to_collection(cfg)
    elif args.command == "retry_crawl_jobs":
        retry_crawl_jobs(cfg)
    elif args.command == "rebuild_jobs_info":
        rebuild_jobs_info(cfg)
    elif args.command == "purge_all_crawl_jobs":
        purge_all_crawl_jobs(cfg)
    elif args.command == "check_liveness":
        check_liveness(cfg)
    else:
        print(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
