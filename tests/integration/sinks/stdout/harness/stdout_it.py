"""stdout sink suite: the TP-12 case of the functional harness, run on its own.

The stdout sink shares the whole source-side setup (seeded tables, a free
server-id, the checkpoint layout) with the MySQL sink, so this module is a thin
wrapper: it renders the stdout fixture and delegates the case body to
``sinks.mysql.harness.run_it.run_stdout_case`` instead of copying it.

    CDC_SINK=stdout python3 -m sinks.stdout.harness.stdout_it

Report: docs/test/reports/mysql-mysql-func.md (same file the functional suite
writes, because it is the same test point).
"""
from __future__ import annotations

from ...mysql.harness import run_it


def main() -> int:
    """Run only TP-12 (stdout), then write the report like the full suite does."""
    return run_it.main(only="stdout")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        run_it.stop_job()
