from __future__ import annotations
import logging, logging.config, os, json
from pathlib import Path

def _ensure_parent(path: str) -> None:
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

def build_dict_config(cfg: dict) -> dict:
    level = (cfg.get("level") or "INFO").upper()
    log_file = cfg.get("file") or "logs/app.log"
    rotate = cfg.get("rotate") or {}
    when = rotate.get("when", "midnight")
    backup = int(rotate.get("backupCount", 14))
    to_console = bool(cfg.get("console", True))
    as_json = bool(cfg.get("json", False))

    _ensure_parent(log_file)

    # Formatters
    if as_json:
        # Lightweight JSON formatter
        class JsonFormatter(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:
                data = {
                    "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
                    "level": record.levelname,
                    "logger": record.name,
                    "msg": record.getMessage(),
                }
                if record.exc_info:
                    data["exc_info"] = self.formatException(record.exc_info)
                return json.dumps(data, ensure_ascii=False)
        json_fmt_path = "crawler.logging_setup.JsonFormatter"
        fmt_name = "json"
        fmt_config = {"()": json_fmt_path}
    else:
        fmt_name = "standard"
        fmt_config = {"format": "%(asctime)s %(levelname)s [%(name)s] %(message)s"}

    handlers = {
        "file": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "level": level,
            "filename": log_file,
            "when": when,
            "backupCount": backup,
            "encoding": "utf-8",
            "formatter": fmt_name,
        }
    }
    if to_console:
        handlers["console"] = {
            "class": "logging.StreamHandler",
            "level": level,
            "formatter": fmt_name,
        }

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            fmt_name: fmt_config
        },
        "handlers": handlers,
        "loggers": {
            # Root logger: file (+console)
            "": {
                "handlers": list(handlers.keys()),
                "level": level,
            },
            # Quiet very chatty third-party libs if desired:
            "urllib3": {"level": "WARNING"},
            "requests": {"level": "WARNING"},
            "werkzeug": {"level": "WARNING"},
        },
    }

def setup_logging(app_cfg: dict) -> None:
    cfg = app_cfg.get("logging", {}) if app_cfg else {}
    logging.config.dictConfig(build_dict_config(cfg))
