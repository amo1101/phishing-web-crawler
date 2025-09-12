from __future__ import annotations
from pathlib import Path
from datetime import date
from typing import Optional

def fetch_iosco_csv(start_date: Optional[date], end_date: Optional[date]) -> Path:
    """
    YOU implement:
      - Apply NCA filter = 'New Zealand - Financial Markets Authority'
      - First run: export ALL results (start_date == end_date == None)
      - Next runs: export rows with report date in [start_date, end_date] inclusive
    Return: absolute Path to the downloaded CSV.
    """
    raise NotImplementedError("Implement I-SCAN CSV export and return local CSV path")
