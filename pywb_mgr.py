from __future__ import annotations
import subprocess
from pathlib import Path
from typing import List

def ensure_collection(collection: str, wb_manager_bin: str = "wb-manager"):
    # if collection exists, this returns non-zero? We'll try init and ignore if exists.
    try:
        subprocess.run([wb_manager_bin, "init", collection], check=True)
    except subprocess.CalledProcessError:
        # assume exists
        pass

def add_warcs(collection: str, warc_paths: List[Path], wb_manager_bin: str = "wb-manager"):
    if not warc_paths:
        return
    args = [wb_manager_bin, "add", collection] + [str(p) for p in warc_paths]
    subprocess.run(args, check=True)
