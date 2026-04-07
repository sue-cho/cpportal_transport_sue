"""
Posit Connect / Git deployment entrypoint.

Loads the Congestion Pricing Research Dashboard from `reference/app.py` (db_client, map_utils,
llm_cloud live next to that module). Do not use `import app` from within this file — it would
recurse; we load the submodule by file path.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_REF = _ROOT / "reference"
sys.path.insert(0, str(_REF))

_spec = importlib.util.spec_from_file_location("reference_shiny_app", _REF / "app.py")
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["reference_shiny_app"] = _mod
_spec.loader.exec_module(_mod)
app = _mod.app
