"""PostgreSQL 14-18 compatibility matrix (PG-209, tasks T3.17-T3.20).

One core suite - ``sinks.postgresql.harness.pg_it`` (TP-P01..TP-P20) - runs once per server
major version. Nothing is copied per version: the only per-version inputs are
the container image, the ``PG_EXPECT_MAJOR`` guard (the suite refuses to seed a
server of another major version) and the report/checkpoint paths, so two
versions can never read each other's artefacts.

Per version the matrix:

1. exports ``PG_IMAGE`` / ``PG_EXPECT_MAJOR`` / a per-version E2E report path,
2. brings the compose service up with ``scripts/run.sh postgresql`` (that script drops
   the volume when the image changes, so a major switch always starts clean),
3. runs ``python -m sinks.postgresql.harness.pg_it`` and records its exit status,
4. writes one row into ``docs/test/reports/mysql-postgresql-compat.md``.

Status vocabulary (spec section 2/9): ``PASS`` / ``FAIL`` / ``ENV BLOCKED`` /
``NOT RUN``. A version that did not run is never a pass, and the report states
that PG-209 stays incomplete until all five have really run.

No version-specific branch is added here. ``RECORDED_DIFFERENCES`` stays empty
until a difference is proven by a real run; guessing one is out of scope.
"""
from __future__ import annotations

import argparse
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence

from common import config as cfg

VERSIONS: tuple[str, ...] = ("14", "15", "16", "17", "18")

# The shared core suite. The compatibility matrix must never grow its own copy
# of the test points: it reuses this module for every version.
SUITE_MODULE = "sinks.postgresql.harness.pg_it"

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_BLOCKED = "ENV BLOCKED"
STATUS_NOT_RUN = "NOT RUN"

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_BLOCKED = 2
# ``harness.pg_it.main`` returns 2 when its preflight cannot use the environment
# (no binary, no server of the expected major version, no MySQL).
SUITE_ENV_BLOCKED = 2

# T3.20: only a difference proven by a real run may be recorded, with evidence.
# Nothing is recorded yet, so no version-specific compatibility branch exists.
RECORDED_DIFFERENCES: tuple[dict[str, str], ...] = ()


@dataclass
class Result:
    """Outcome of one version of the matrix."""

    version: str
    image: str
    status: str
    exit_code: int | None = None
    seconds: float = 0.0
    report: str = ""
    log: str = ""
    reason: str = ""
    failed: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status == STATUS_PASS


# ------------------------------------------------------------------ version plan
def normalize_versions(raw: Sequence[str]) -> list[str]:
    """Accept 14 / PG14 / pg14 / 14.18 and return distinct supported majors."""
    out: list[str] = []
    for value in raw:
        text = str(value).strip().lower()
        if text.startswith("pg"):
            text = text[2:]
        major = text.split(".")[0]
        if major not in VERSIONS:
            raise ValueError(
                f"unsupported PostgreSQL version {value!r}: expected one of {', '.join(VERSIONS)}"
            )
        if major not in out:
            out.append(major)
    return out


def image_for(version: str) -> str:
    """Image for a major version; ``PG_COMPAT_IMAGE_<major>`` pins a minor tag.

    The default is the major-only tag so the matrix never guesses which minor
    tags exist on the registry; pin one explicitly for a reproducible run.
    """
    return cfg.env(f"PG_COMPAT_IMAGE_{version}", f"postgres:{version}")


def report_path(version: str) -> Path:
    """Per-version E2E report, so five runs do not overwrite one another."""
    return cfg.REPORT_DIR / f"mysql-postgresql-func-pg{version}.md"


def checkpoint_dir(version: str) -> Path:
    return Path(cfg.WORKDIR) / f"ckpts-pg-compat-{version}"


def checkpoint_restart_dir(version: str) -> Path:
    return Path(cfg.WORKDIR) / f"ckpts-pg-compat-{version}-restart"


def compat_env(version: str, base: Mapping[str, str] | None = None) -> dict[str, str]:
    """Environment of one matrix row: same suite, pinned to a single server."""
    env = dict(os.environ if base is None else base)
    env.update(
        {
            "PG_IMAGE": image_for(version),
            # The suite refuses to seed a server of another major version, so a
            # stale container can never be reported as this version's result.
            "PG_EXPECT_MAJOR": version,
            "REPORT_PG_FUNC": str(report_path(version)),
            "CHECKPOINT_PG_DIR": str(checkpoint_dir(version)),
            "CHECKPOINT_PG_RESTART_DIR": str(checkpoint_restart_dir(version)),
        }
    )
    return env


def case_args(cases: Sequence[str]) -> list[str]:
    """Test points handed to the shared suite (empty means every test point)."""
    return [str(case).upper() for case in cases]


def up_argv(version: str = "") -> list[str]:
    """Start the source MySQL plus the PostgreSQL server pinned to ``version``.

    One entry script owns container start-up for every sink, so the matrix only
    passes the variant (``pg14``..``pg18``) instead of duplicating compose flags.
    """
    argv = ["bash", str(cfg.IT_ROOT / "scripts" / "setup.sh"), "postgresql"]
    return argv + [f"pg{version}"] if version else argv


def suite_argv(cases: Sequence[str] = ()) -> list[str]:
    return [sys.executable, "-m", SUITE_MODULE, *case_args(cases)]


def docker_available() -> bool:
    """True only when a docker CLI can actually talk to a daemon."""
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.call(
            ["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        ) == 0
    except OSError:
        return False


# ------------------------------------------------------------------- dependencies
def go_mod_versions(root: Path | None = None) -> dict[str, str]:
    """Go toolchain and pgx/v5 versions the matrix rows actually ran (T3.18)."""
    found = {"go": "unknown", "pgx": "unknown"}
    path = Path(root or cfg.ROOT) / "go.mod"
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return found
    match = re.search(r"^go\s+(\S+)$", text, re.MULTILINE)
    if match:
        found["go"] = match.group(1)
    match = re.search(r"github\.com/jackc/pgx/v5\s+(\S+)", text)
    if match:
        found["pgx"] = match.group(1)
    return found


def ci_pg_image(compose: Path | None = None) -> str:
    """Default image the PostgreSQL sink compose service pins, recorded for comparison."""
    path = Path(compose or cfg.SINKS_DIR / "postgresql" / "docker-compose.yml")
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return "unknown"
    match = re.search(r"\$\{PG_IMAGE:-\s*(postgres:\S+?)\s*\}", text)
    return match.group(1) if match else "unknown"


# ------------------------------------------------------------------------ runner
def _default_runner(argv: Sequence[str], env: Mapping[str, str], log_path: Path,
                    append: bool) -> int:
    """Run one step with its output appended to the version's log file."""
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a" if append else "w", encoding="utf-8") as handle:
        handle.write(f"$ {' '.join(argv)}\n")
        handle.flush()
        return subprocess.call(
            list(argv), env=dict(env), stdout=handle, stderr=subprocess.STDOUT,
            cwd=str(cfg.IT_ROOT),
        )


def _log_tail(log_path: Path, limit: int = 400) -> str:
    path = Path(log_path)
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace").strip()
    return text[-limit:] if text else ""


def _blocked_reason(log_path: Path) -> str:
    """The suite prints ``ENV BLOCKED: <why>`` from preflight; keep that line."""
    for line in reversed(_log_tail(log_path, 4000).splitlines()):
        if "ENV BLOCKED:" in line:
            return line.split("ENV BLOCKED:", 1)[1].strip()
    return ""


def failed_cases(report: Path) -> list[str]:
    """Test points marked FAIL in the E2E report the suite just wrote."""
    path = Path(report)
    if not path.exists():
        return []
    failing: list[str] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = re.match(r"^\|\s*(TP-P\d+)\s*\|.*❌ FAIL\s*\|\s*$", line)
        if match:
            failing.append(match.group(1))
    return failing


def run_version(
    version: str,
    cases: Sequence[str] = (),
    *,
    log_dir: Path | None = None,
    runner=None,
    docker: bool | None = None,
) -> Result:
    """Bring up one server version and run the shared suite against it."""
    version = normalize_versions([version])[0]
    log_path = Path(log_dir or cfg.WORKDIR) / f"pg-compat-{version}.log"
    result = Result(
        version=version,
        image=image_for(version),
        status=STATUS_NOT_RUN,
        report=cfg.project_display_path(report_path(version)),
        log=cfg.project_display_path(log_path),
    )
    available = docker_available() if docker is None else docker
    if not available:
        result.status = STATUS_BLOCKED
        result.reason = "docker daemon is not available: no version can be exercised here"
        return result

    run = runner or _default_runner
    env = compat_env(version)
    started = time.time()
    up_code = run(up_argv(version), env, log_path, False)
    if up_code != 0:
        result.status = STATUS_BLOCKED
        result.exit_code = up_code
        result.seconds = round(time.time() - started, 1)
        result.reason = f"setup.sh postgresql pg{version} exited {up_code}: {_log_tail(log_path)}"
        return result

    code = run(suite_argv(cases), env, log_path, True)
    result.exit_code = code
    result.seconds = round(time.time() - started, 1)
    if code == 0:
        result.status = STATUS_PASS
    elif code == SUITE_ENV_BLOCKED:
        result.status = STATUS_BLOCKED
        result.reason = _blocked_reason(log_path) or "the suite reported an unusable environment"
    elif code == EXIT_FAIL:
        result.status = STATUS_FAIL
        result.failed = failed_cases(report_path(version))
        result.reason = "failing test point(s): " + (", ".join(result.failed) or "see log")
    else:
        result.status = STATUS_FAIL
        result.reason = f"the suite exited {code} (unexpected): {_log_tail(log_path)}"
    return result


# ------------------------------------------------------------------------ report
def _cell(value, *, code: bool = False) -> str:
    # An empty artefact (a row that never produced a report or a log) is written
    # as "-" instead of an empty code span, so the table stays readable.
    if isinstance(value, str) and not value.strip():
        return "-"
    return cfg.md_cell(value, code=code)


def write_report(
    results: Sequence[Result],
    *,
    meta: Mapping[str, object] | None = None,
    path: Path | None = None,
) -> Path:
    """Write the matrix report; PASS is only ever printed with a real run."""
    out = Path(path or cfg.REPORT_PG_COMPAT)
    out.parent.mkdir(parents=True, exist_ok=True)
    meta = dict(meta or {})
    deps = go_mod_versions()

    passed = [r for r in results if r.status == STATUS_PASS]
    failed = [r for r in results if r.status == STATUS_FAIL]
    blocked = [r for r in results if r.status == STATUS_BLOCKED]
    not_run = [r for r in results if r.status == STATUS_NOT_RUN]
    covered = {r.version for r in results}
    complete = covered == set(VERSIONS) and len(passed) == len(VERSIONS)
    docker_text = (
        "available" if (meta["docker"] if "docker" in meta else docker_available())
        else "unavailable"
    )
    modes = meta.get("modes") or ["live"]

    lines = [
        "# go-cdc mysql2postgresql compatibility matrix (PostgreSQL 14-18)\n",
        f"- Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Core suite: `python -m {SUITE_MODULE}` (TP-P01..TP-P20), shared by every version",
        "- Per-version inputs: container image, `PG_EXPECT_MAJOR`, report and checkpoint "
        "paths. No test implementation is copied per version.",
        "- Modes: " + ", ".join(f"`{m}`" for m in modes),
        f"- **PG-209: {'COMPLETE' if complete else 'INCOMPLETE'}** - "
        f"PASS {len(passed)} / FAIL {len(failed)} / ENV BLOCKED {len(blocked)} / "
        f"NOT RUN {len(not_run)} of {len(VERSIONS)}",
        "",
        "## 0. Status vocabulary\n",
        "| Status | Meaning |",
        "| --- | --- |",
        "| `PASS` | that version really ran and every test point passed in this environment |",
        "| `FAIL` | that version really ran and at least one test point failed |",
        "| `ENV BLOCKED` | could not run here (no docker daemon, image unavailable, no server); "
        "never a pass |",
        "| `NOT RUN` | not executed in this matrix, e.g. a dry run or an unselected version |",
        "",
        "## 1. Dependencies and environment (T3.18)\n",
        "| Item | Value |",
        "| --- | --- |",
        f"| Go toolchain (`go.mod`) | `{_cell(deps['go'])}` |",
        f"| pgx/v5 (`go.mod`) | `{_cell(deps['pgx'])}` |",
        f"| Python | `{_cell(platform.python_version())}` |",
        f"| Host | `{_cell(platform.platform())}` |",
        f"| Docker daemon | `{docker_text}` |",
        f"| Existing CI PostgreSQL job image | `{_cell(ci_pg_image())}` |",
        f"| MySQL source | `{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}` / `{cfg.SRC_USER}` |",
        f"| PostgreSQL target | `{cfg.PG_HOST}:{cfg.PG_PORT}` / `{cfg.PG_DB}` |",
        f"| Entry point | `{_cell(cfg.project_display_path(cfg.BINARY))} <pipeline.yaml>` |",
        "",
        "## 2. Matrix (T3.19)\n",
        "| PostgreSQL | Image | Status | Exit | Seconds | E2E report | Log | Note |",
        "| --- | --- | --- | ---: | ---: | --- | --- | --- |",
    ]
    for result in results:
        lines.append(
            f"| {result.version} | {_cell(result.image, code=True)} | "
            f"`{result.status}` | {result.exit_code if result.exit_code is not None else '-'} | "
            f"{result.seconds or '-'} | {_cell(result.report, code=True)} | "
            f"{_cell(result.log, code=True)} | {_cell(result.reason) or '-'} |"
        )
    lines.append("")

    lines.append("## 3. Per-version detail\n")
    for result in results:
        lines.append(f"### PostgreSQL {result.version}\n")
        lines.append(f"- Status: `{result.status}`")
        lines.append(f"- Image: {_cell(result.image, code=True)}")
        if result.exit_code is not None:
            lines.append(f"- Suite exit code: `{result.exit_code}`")
        if result.seconds:
            lines.append(f"- Wall clock (up + suite): `{result.seconds}` s")
        lines.append(f"- Command: `PG_IMAGE={result.image} PG_EXPECT_MAJOR={result.version} "
                     f"python -m {SUITE_MODULE}`")
        if result.failed:
            lines.append("- Failing test point(s): " + ", ".join(f"`{c}`" for c in result.failed))
        if result.reason:
            lines.append(f"- Note: {_cell(result.reason)}")
        lines.append(f"- E2E report: {_cell(result.report, code=True)}")
        lines.append(f"- Log: {_cell(result.log, code=True)}")
        lines.append("")

    lines.append("## 4. Version differences (T3.20)\n")
    if RECORDED_DIFFERENCES:
        lines.extend(["| Version | Behaviour | Evidence | Handling |", "| --- | --- | --- | --- |"])
        for item in RECORDED_DIFFERENCES:
            lines.append(
                f"| {_cell(item['version'])} | {_cell(item['behaviour'])} | "
                f"{_cell(item['evidence'])} | {_cell(item['handling'])} |"
            )
    else:
        lines.append(
            "No version-specific behaviour difference has been proven by a real run, so no "
            "compatibility branch is added. A difference is only recorded here with the "
            "version, the test point and the log/report that shows it."
        )
    lines.append("")

    lines.append("## 5. Limits of this matrix\n")
    lines.append(
        f"- The core suite is `{SUITE_MODULE}`: multi-DB snapshot, snapshot-to-binlog "
        "switchover, binlog DML, DDL, routing, refused types, restart/replay and "
        "`PG_RESTART_ENABLED` opt-in. E2E Load and E2E Scale are separate suites and are "
        "not re-run per version."
    )
    lines.append(
        "- Two test points are recorded as `SKIPPED`, never as passed: TP-P17 (container "
        "restart) unless `PG_RESTART_ENABLED=1`, and TP-P18 (network interruption), which has "
        "no injector in this repository. A `PASS` row therefore covers TP-P01..TP-P16, TP-P19 "
        "and TP-P20 with those two recorded as SKIPPED."
    )
    lines.append(
        "- A single version passing, or a mock/offline check passing, is not the matrix: "
        "PG-209 needs all five rows at `PASS`."
    )
    lines.append("")
    out.write_text(cfg.sanitize_report_text("\n".join(lines)), encoding="utf-8")
    return out


# -------------------------------------------------------------------------- cli
def _print_plan(versions: Sequence[str], cases: Sequence[str]) -> None:
    for version in versions:
        print(
            f"PG{version}: image={image_for(version)} expected_major={version} "
            f"report={cfg.project_display_path(report_path(version))} "
            f"suite=python -m {SUITE_MODULE}"
            + (f" {' '.join(case_args(cases))}" if cases else "")
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m sinks.postgresql.harness.pg_compat",
        description="Run the shared Postgres E2E suite against PostgreSQL 14-18.",
    )
    parser.add_argument("versions", nargs="*", help="majors to run (default: 14 15 16 17 18)")
    parser.add_argument("--list", action="store_true",
                        help="print the plan and exit (no docker, no database)")
    parser.add_argument("--dry-run", action="store_true",
                        help="write the matrix with every version NOT RUN")
    parser.add_argument("--cases", nargs="*", default=[],
                        help="test points reused for every version (default: all)")
    parser.add_argument("--report", default=None, help="matrix report path override")
    parser.add_argument("--log-dir", default=None, help="per-version log directory")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))

    try:
        versions = normalize_versions(args.versions) or list(VERSIONS)
    except ValueError as exc:
        parser.error(str(exc))
        return EXIT_BLOCKED
    cases = case_args(args.cases)

    if args.list:
        _print_plan(versions, cases)
        return EXIT_OK

    meta = {
        "modes": ["dry-run" if args.dry_run else "live"],
        "docker": False if args.dry_run else docker_available(),
    }
    if args.dry_run:
        results = [
            Result(
                version=version,
                image=image_for(version),
                status=STATUS_NOT_RUN,
                report=cfg.project_display_path(report_path(version)),
                log=cfg.project_display_path(Path(args.log_dir or cfg.WORKDIR)
                                             / f"pg-compat-{version}.log"),
                reason="dry run: no server was started and no test point ran",
            )
            for version in versions
        ]
    else:
        results = [
            run_version(version, cases, log_dir=Path(args.log_dir) if args.log_dir else None)
            for version in versions
        ]

    path = write_report(results, meta=meta, path=Path(args.report) if args.report else None)
    print(f"REPORT {cfg.project_display_path(path)}")
    for result in results:
        print(f"PG{result.version}: {result.status}"
              + (f" ({result.reason})" if result.reason else ""))
    if args.dry_run:
        return EXIT_OK
    if any(result.status == STATUS_FAIL for result in results):
        return EXIT_FAIL
    if any(result.status != STATUS_PASS for result in results):
        return EXIT_BLOCKED
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
