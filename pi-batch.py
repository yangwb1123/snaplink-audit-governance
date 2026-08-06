#!/usr/bin/env python3
"""pi-batch -- serial/parallel batch executor for the pi agent.

Entry-point shim over the pbatch package: `python pi-batch.py ...`
and `python -c "import pi_batch_under_test"`-style loads both work; the
implementation lives in pbatch/ (config, models, runner, pipeline,
cli). Copy the pbatch/ directory next to this script to move the tool to
another project.
"""
from __future__ import annotations

import os
import sys
import time  # noqa: F401  (tests monkeypatch mod.time.sleep for retry backoff)

os.environ.setdefault("PBATCH_SCRIPT_DIR", os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pbatch import *  # noqa: F401,F403  (re-export the public API)
from pbatch import config  # noqa: F401  (tests override config.AGENT_BIN)
from pbatch.cli import main  # noqa: F401

if __name__ == "__main__":
    main()
