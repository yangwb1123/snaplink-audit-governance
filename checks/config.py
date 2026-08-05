from dataclasses import dataclass
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Config:
    max_go_lines: int = 1500
    max_function_lines: int = 260
    max_decisions: int = 45
    max_subdirs: int = 15
    build_dir: str = "bin"


def load(path: str | Path = ROOT / "engineering.yaml") -> Config:
    text = Path(path).read_text(encoding="utf-8")
    values = {}
    for name, default in (("max_go_lines", 1500), ("max_function_lines", 260),
                          ("max_decisions", 45), ("max_subdirs", 15)):
        match = re.search(rf"^\s*{name}:\s*(\d+)\s*$", text, re.MULTILINE)
        values[name] = int(match.group(1)) if match else default
    return Config(**values)


def get_config() -> Config:
    return load()

