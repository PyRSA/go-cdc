#!/usr/bin/env python3
"""Turn a git diff into the CI job matrix: which sink, which variant.

Contract (what ``.github/workflows/ci.yml`` consumes):

* stdout is a *standard* matrix JSON - ``{"include": [{"sink": ..., "variant":
  ...}]}`` - so it can be fed straight into ``strategy.matrix``. It is always an
  object, never a bare array: a docs-only change prints ``{"include": []}``, so
  the caller can hand the value to ``fromJSON()`` without a type-dependent
  special case (and the ``result`` job treats "no rows" as success);
* every sink name also gets its own matrix (``mysql={"include": [...]}``), so a
  reusable ``ci-<sink>.yml`` receives only the rows it owns and never has to
  filter an aggregate.

Nothing here guesses: the mapping from a path to a sink is one table, and an
unknown path is treated as shared (run everything). A missed run is a silent
regression; an extra run only costs minutes.

Usage:
    python3 tools/ci/plan.py --base <sha> --head <sha> --profile heavy
    python3 tools/ci/plan.py --base <sha> --profile normal --github-output "$GITHUB_OUTPUT"
    python3 tools/ci/plan.py --paths connector/sink/kafka/sink.go    # offline
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

SINKS: tuple[str, ...] = (
    "mysql",
    "stdout",
    "kafka",
    "doris",
    "elasticsearch",
    "postgresql",
)

# Owned by exactly one sink: the connector, the sink-local harness / fixtures /
# unit tests / compose file, that sink's demo config and its own reusable
# workflow. A change here runs that sink only.
def sink_dirs(sink: str) -> tuple[str, ...]:
    return (f"tests/integration/sinks/{sink}/", f"connector/sink/{sink}/")


def sink_files(sink: str) -> tuple[str, ...]:
    return (f"configs/demo-{sink}.yaml", f".github/workflows/ci-{sink}.yml")


# The single table behind `owner_of`: every path that routes to one sink.
SINK_PATHS: dict[str, tuple[str, ...]] = {
    sink: sink_dirs(sink) + sink_files(sink) for sink in SINKS
}


# Shared by every sink: the CDC core, the MySQL source side, the IT plumbing,
# the CI entry point and the build description. "connector/sink/" is common
# *after* the per-sink table above has been tested, so sink.go (the interface)
# lands here while connector/sink/kafka/ already matched kafka.
COMMON_PATHS: tuple[str, ...] = (
    ".github/workflows/ci.yml",
    "cmd/",
    "common/",
    "composer/",
    "connector/sink/",
    "connector/source/",
    "runtime/",
    "tests/integration/common/",
    "tests/integration/scripts/",
    "tests/integration/sources/",
    "tests/integration/conftest.py",
    "tests/integration/README",
    "tools/",
)

COMMON_FILES: frozenset[str] = frozenset(
    {"go.mod", "go.sum", "Makefile", ".golangci.yml", ".gitignore"}
)

# Never a reason to run anything: prose, editor state, the generated
# report/checkpoint workdir, configs that are not a sink's demo, and the two
# standalone workflows (release / repo hygiene) that are not part of the
# routing matrix.
IGNORE_PREFIXES: tuple[str, ...] = (
    "docs/",
    "configs/",
    ".idea/",
    ".cursor/",
    ".claude/",
    ".qoder/",
    ".vscode/",
    "tests/integration/workdir/",
)
IGNORE_FILES: frozenset[str] = frozenset(
    {".github/workflows/release.yml", ".github/workflows/repo-checks.yml"}
)
IGNORE_SUFFIXES: tuple[str, ...] = (".md",)

# Which variants each sink is exercised on, per profile. The tier still never
# decides which suites run - that is static per sink in run.sh's default_suites;
# the tier only resizes writers and timeouts. The one deliberate asymmetry is the
# PostgreSQL VERSION matrix: `normal` pins the single default major (pg16) to keep
# the PR gate cheap, while `heavy` fans the compat majors (pg14..pg18) out into
# separate matrix rows so they run in parallel CI jobs instead of as one slow
# pg14..pg18 loop inside a single process (run.sh's compat suite then exercises
# just the version each row brings up).
VARIANTS: dict[str, dict[str, tuple[str, ...]]] = {
    "normal": {
        "mysql": ("57", "80"),
        "stdout": ("",),
        "kafka": ("",),
        "doris": ("",),
        "elasticsearch": ("es8", "es7", "es9"),
        "postgresql": ("pg16",),
    },
    "heavy": {
        "mysql": ("57", "80"),
        "stdout": ("",),
        "kafka": ("",),
        "doris": ("",),
        "elasticsearch": ("es8", "es7", "es9"),
        # Compat majors become parallel rows; each runs its own server, so the
        # in-process five-version loop is gone and the wall clock no longer pays
        # for five container swaps.
        "postgresql": ("pg14", "pg15", "pg16", "pg17", "pg18"),
    },
}


def _owns(path: str, owned: str) -> bool:
    """A directory entry matches by prefix; a single file matches exactly."""
    return path.startswith(owned) if owned.endswith("/") else path == owned


def owner_of(path: str) -> str:
    """``sink:<name>``, ``shared`` or ``ignore`` for one changed path."""
    path = path.replace("\\", "/")
    # Only an explicit "./" is dropped: lstrip("./") would eat the dot of
    # ".github/workflows/..." and silently fall through to "shared".
    while path.startswith("./"):
        path = path[2:]
    # Prose is never a reason to run anything, not even for a sink.
    if path.endswith(IGNORE_SUFFIXES):
        return "ignore"
    # Sink-local first: `configs/demo-<sink>.yaml` and `ci-<sink>.yml` must win
    # over the blanket `configs/` ignore and the shared connector/sink/ prefix.
    for sink, owned in SINK_PATHS.items():
        if any(_owns(path, entry) for entry in owned):
            return f"sink:{sink}"
    if path.startswith(IGNORE_PREFIXES) or path in IGNORE_FILES:
        return "ignore"
    if path in COMMON_FILES or path.startswith(COMMON_PATHS):
        return "shared"
    # Unknown path: be conservative and run everything.
    return "shared"


def affected_sinks(paths: list[str]) -> list[str]:
    """Sinks whose suites must run for this diff, in a stable order."""
    owners = {owner_of(p) for p in paths}
    if not owners or "shared" in owners:
        return list(SINKS)
    hit = {o.split(":", 1)[1] for o in owners if o.startswith("sink:")}
    return [s for s in SINKS if s in hit]


def rows_for(sinks: list[str], profile: str) -> list[dict[str, str]]:
    """Matrix rows for the given sinks; ``variant`` may be the empty string."""
    variants = VARIANTS[profile]
    return [
        {"sink": sink, "variant": variant}
        for sink in sinks
        for variant in variants[sink]
    ]


def matrix(sinks: list[str], profile: str) -> dict[str, list[dict[str, str]]]:
    """The published matrix: always an ``{"include": [...]}`` object.

    ``strategy.matrix`` requires an object, so "nothing to run" is
    ``{"include": []}`` rather than ``[]``: the same shape feeds
    ``fromJSON()`` in every case.
    """
    return {"include": rows_for(sinks, profile)}


def per_sink(sinks: list[str], profile: str) -> dict[str, dict]:
    """One ``{"include": [...]}`` per affected sink (used by ci-<sink>.yml)."""
    out: dict[str, dict] = {}
    for sink in sinks:
        out[sink] = {"include": rows_for([sink], profile)}
    return out


# The trunk a fallback merge-base is measured against, in order. One place to
# answer "which remote ref is the default branch": a repo that lives on `main`
# fails the `origin/master` probe silently and lands on the next candidate.
DEFAULT_BRANCH_CANDIDATES: tuple[str, ...] = ("origin/master", "origin/main")

# git quotes "unusual" path bytes by default (core.quotePath), so a Chinese file
# name comes back as ``"docs/\351\234\200.md"``: the ``.md`` ignore rule then misses
# the trailing quote and every sink runs. This is the only command that asks git
# for paths, so the switch lives here. (``-z`` would also survive a newline in a
# path; not worth the split while no such path exists.)
QUOTE_PATH_OFF: tuple[str, ...] = ("-c", "core.quotePath=false")


def _git(repo: Path | str, *args: str, quiet: bool = False) -> str | None:
    """Run one git command; its stdout, or ``None`` when it fails. Never raises.

    ``quiet`` exists for probing commands (a merge-base candidate that does not
    exist is expected, not news); a real failure stays loud, because it is the
    difference between a precise plan and a full run.
    """
    try:
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:  # bad sha, shallow clone
        if not quiet:
            detail = (getattr(exc, "stderr", "") or str(exc)).strip()
            # The plumbing in front of the subcommand (``-C <repo>``, ``-c ...``)
            # is noise in a CI log: say which git command failed, not how it was
            # invoked.
            display_args = [
                a for a in args if not a.startswith("-") and a != "core.quotePath=false"
            ]
            print(f"plan: git {' '.join(display_args)} failed: {detail}", file=sys.stderr)
        return None


def _diff_names(base: str, head: str, repo: Path | str) -> list[str] | None:
    """Changed paths between two commits, or ``None`` when git cannot tell.

    An empty *successful* diff is ``[]``, not ``None``: both end up running
    everything, but only ``None`` means "this question was unanswerable".
    """
    out = _git(
        repo, *QUOTE_PATH_OFF, "diff", "--name-only", "--no-renames", base, head
    )
    if out is None:
        return None
    return [line for line in out.splitlines() if line]


def _fork_point(head: str, repo: Path | str) -> str | None:
    """Where ``head`` branched off the default branch, or ``None`` if unknown."""
    for candidate in DEFAULT_BRANCH_CANDIDATES:
        sha = _git(repo, "merge-base", candidate, head, quiet=True)
        if sha and sha.strip():
            return sha.strip()
    return None


def changed_files(base: str, head: str = "HEAD", repo: Path | str = ".") -> list[str]:
    """Paths changed between ``base`` and ``head``, with a merge-base fallback.

    Zero: a ``workflow_dispatch`` short-circuits to ``[]`` before git runs. A
    dispatch is a person asking for a verdict, and it arrives with an empty
    ``BASE`` while still sitting on a real branch - without this guard the
    merge-base fallback below would answer it with a *selective* plan, i.e. a
    manual promotion could silently skip sinks. Outside GitHub Actions the
    variable is unset, so a local run is unaffected.

    Three attempts otherwise, most precise first:

    1. ``git diff <base> <head>`` - the normal path, where ``base`` is the PR
       base sha or the previous push tip (``github.event.before``). Because both
       ends are commit tips, the diff covers *every* commit the push carried.
       This is why the fallback below is a fork point and never ``HEAD^``: the
       parent only describes the last commit, so a multi-commit push would plan
       one commit's paths and silently miss the rest.
    2. ``git merge-base origin/<trunk> <head>``, then that diff - reached when
       ``base`` is empty or all-zero (a brand-new branch: the push payload's
       null SHA) or when the diff itself fails (a force push left the old tip
       dangling, so a fresh ``fetch-depth: 0`` clone cannot resolve it). The
       fork point against the default branch is still the branch's own change
       set, however long the branch is.
    3. ``[]`` - nothing usable (no base, no ``origin/master`` or ``origin/main``,
       unrelated histories). Callers read ``[]`` as "unknown history" and run
       everything, so this is the fail-open last resort, unchanged.

    ``[]`` therefore never means "nothing changed": that case arrives as a
    non-empty path list whose owners are all ``ignore``.
    """
    # GITHUB_EVENT_NAME is one of the default environment variables Actions
    # exports, so no workflow change is needed to read the trigger here.
    if os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch":
        print(
            "plan: workflow_dispatch; no diff to trust, running every sink",
            file=sys.stderr,
        )
        return []

    # A usable base is any non-empty value that is not exactly the null SHA. The
    # comparison is equality, not ``set(base) > {"0"}``: that would be a *proper
    # superset* test and reject every real sha as well.
    if base and set(base) != {"0"}:
        paths = _diff_names(base, head, repo)
        if paths is not None:
            return paths

    fork_point = _fork_point(head, repo)
    if fork_point is not None:
        paths = _diff_names(fork_point, head, repo)
        if paths is not None:
            return paths

    print(
        "plan: no usable base and no merge-base with "
        f"{'/'.join(DEFAULT_BRANCH_CANDIDATES)}; treating the change set as "
        "unknown and running every sink",
        file=sys.stderr,
    )
    return []


def plan(paths: list[str], profile: str = "normal") -> dict:
    """Everything the workflow needs, derived from one diff."""
    sinks = affected_sinks(paths)
    rows = rows_for(sinks, profile)
    return {
        "sinks": sinks,
        "matrix": matrix(sinks, profile),
        "count": len(rows),
        "per_sink": per_sink(sinks, profile) if rows else {},
    }


def _write_github_output(path: Path, result: dict) -> None:
    lines = [
        f"matrix={json.dumps(result['matrix'], separators=(',', ':'))}",
        f"count={result['count']}",
        f"sinks={','.join(result['sinks'])}",
    ]
    for sink, matrix_json in result["per_sink"].items():
        lines.append(f"{sink}={json.dumps(matrix_json, separators=(',', ':'))}")
    with path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="git diff -> CI matrix")
    ap.add_argument("--base", default="", help="base commit (PR base sha / push before)")
    ap.add_argument("--head", default="HEAD", help="head commit")
    ap.add_argument("--repo", default=".", help="repository root")
    ap.add_argument("--profile", default="normal", choices=sorted(VARIANTS))
    ap.add_argument(
        "--paths",
        nargs="*",
        default=None,
        help="use these paths instead of asking git (offline/debug)",
    )
    ap.add_argument("--github-output", default="", help="append step outputs here")
    ap.add_argument(
        "--print",
        dest="print_json",
        action="store_true",
        help="print the matrix (the default; kept for explicitness)",
    )
    args = ap.parse_args(argv)

    paths = args.paths if args.paths is not None else changed_files(
        args.base, args.head, args.repo
    )
    result = plan(paths, args.profile)

    # The matrix always goes to stdout (one JSON document, ``{"include": [...]}``
    # even when it has no rows) so a local run and the workflow log show the same
    # contract.
    print(json.dumps(result["matrix"], separators=(",", ":")))
    print(
        f"plan: {len(paths)} changed path(s) -> {result['count']} job(s) "
        f"[{','.join(result['sinks']) or 'none'}] profile={args.profile}",
        file=sys.stderr,
    )
    if args.github_output:
        _write_github_output(Path(args.github_output), result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
