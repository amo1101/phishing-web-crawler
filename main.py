from __future__ import annotations
import argparse
from .config import Config
from .state import State
from .scheduler import run_loop

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.yaml")
    args = ap.parse_args()

    cfg = Config.load(args.config)
    st = State(cfg["state_db"])

    run_loop(cfg, st)

if __name__ == "__main__":
    main()
