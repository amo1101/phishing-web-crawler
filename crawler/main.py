from __future__ import annotations
import argparse
import logging, threading
from .config import Config
from .state import State
from .logging_setup import setup_logging
from .scheduler import run_loop
from .jobqueue import JobQueueWorker

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.yaml")
    args = ap.parse_args()

    cfg = Config.load(args.config)
    setup_logging(cfg.data)
    log = logging.getLogger(__name__)
    log.info("Starting Phishing web crawler with config: %s", args.config)

    st = State(cfg["state_db"])

    # Start the job-queue worker in background
    worker = JobQueueWorker(cfg, st)
    t = threading.Thread(target=worker.run_forever, name="jobqueue-worker", daemon=True)
    t.start()

    # Run the scheduler loop (blocks)
    try:
        run_loop(cfg, st)
    except Exception as e:
        log.info('Exception %s occured', str(e))
    finally:
        log.info('Quiting the crawler...')
        worker.stop()
        t.join()

if __name__ == "__main__":
    main()
