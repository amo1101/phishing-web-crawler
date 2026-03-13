from __future__ import annotations
import argparse
import logging, threading
from typing import Dict
import btrix_cli
from .config import Config
from .state import State


def update_crawl_configs(cfg: Config):
    btrix = btrix_cli.BrowsertrixClient(
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
    btrix = btrix_cli.BrowsertrixClient(
        base_url=cfg["browsertrix"]["base_url"],
        username=cfg["browsertrix"]["username"],
        password=cfg["browsertrix"]["password"],
        org=cfg["browsertrix"]["org"],
        collection=cfg["browsertrix"]["collection"]
    )

    crawls = btrix.list_crawls()
    crawlIds = [c["id"] for c in crawls]
    btrix.add_crawl_to_collection(crawlIds)

# resume all cancelled or failed or stopped crawls
# you may want to resume crawls after adjust resources for crawler pods
def resume_all_crawls(cfg: Config):
    btrix = btrix_cli.BrowsertrixClient(
        base_url=cfg["browsertrix"]["base_url"],
        username=cfg["browsertrix"]["username"],
        password=cfg["browsertrix"]["password"],
        org=cfg["browsertrix"]["org"],
        collection=cfg["browsertrix"]["collection"]
    )

    state = State(cfg["state_db"])
    crawls = btrix.list_crawls()
    for crawl in crawls:
        if crawl["state"] in ["canceled","failed","stopped_by_user"]:
            btrix.resume_crawl(crawl["id"])
            # update state in db to running so that it can be tracked in jobqueue worker
            state.mark_running_by_job_name(crawl["cid"])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.yaml")
    ap.add_argument("--command", "-c", required=True, help="Command to execute")
    args = ap.parse_args()

    cfg = Config.load(args.config)
    if args.command == "update_crawl_configs":
        update_crawl_configs(cfg)
    elif args.command == "add_crawl_to_collection":
        add_crawl_to_collection(cfg)
    elif args.command == "resume_all_crawls":
        resume_all_crawls(cfg)
    else:
        print(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
