# tests/test_config.py
from crawler.config import Config
import yaml


def test_config_load_and_access(tmp_path):
    cfg_dict = {"a": 1, "b": {"c": 2}}
    p = tmp_path / "cfg.yaml"
    p.write_text(yaml.safe_dump(cfg_dict), encoding="utf-8")

    cfg = Config.load(str(p))
    assert cfg["a"] == 1
    assert cfg["b"]["c"] == 2
    assert cfg.get("missing", "x") == "x"
