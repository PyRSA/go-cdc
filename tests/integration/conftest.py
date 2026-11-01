"""pytest bootstrap for the split integration tree.

The harness modules import each other with package-relative imports
(``from ..harness import pg_client``), so the parent of ``sinks/`` and
``common/`` must be importable *before* pytest collects a test module:

* ``python3 -m pytest`` inserts the rootdir, but not ``tests/integration``;
* ``python3 -m sinks.mysql.harness.run_it`` needs the same path entry.

Putting it here covers both, and keeps ``pytest tests/integration`` working from
the repository root without exporting PYTHONPATH.
"""
from __future__ import annotations

import sys
from pathlib import Path

IT_ROOT = Path(__file__).resolve().parent

if str(IT_ROOT) not in sys.path:
    sys.path.insert(0, str(IT_ROOT))
