#!/usr/bin/env python3
"""Fail if tracked project text contains non-MIT license *declarations*.

Scope (mode A): declaration-shaped text only — SPDX identifiers (≠ MIT),
common license headers / “Licensed under …” lines — not casual product-name
mentions (e.g. “Apache Kafka”).

Root LICENSE / README MIT wording is allowed.

Run from repo root: ``python3 test/check-license-decls.py``
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Declaration-shaped patterns; keep product-name mentions out of these.
FORBIDDEN: list[re.Pattern[str]] = [
    re.compile(r"(?i)SPDX-License-Identifier:\s*(?!MIT\b)\S+"),
    re.compile(r"(?i)Licensed under the Apache License"),
    re.compile(r"(?i)Apache License,?\s+Version\s+\d"),
    re.compile(r"(?i)Licensed to the Apache Software Foundation"),
    re.compile(r"(?i)GNU (?:Affero )?General Public License"),
    re.compile(r"(?i)GNU Lesser General Public License"),
    re.compile(r"(?i)Mozilla Public License"),
    re.compile(r"(?i)Eclipse Public License"),
    re.compile(r"(?i)European Union Public Licen[cs]e"),
    re.compile(r"(?i)Common Development and Distribution License"),
    re.compile(r"(?i)Server Side Public License"),
    re.compile(r"(?i)Business Source License"),
    re.compile(
        r"(?i)Licensed under (?:the )?(?:BSD|ISC|BSL-1\.|Zlib|Artistic|WTFPL)\b"
    ),
]

SKIP_SUFFIXES = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".ico",
    ".pdf",
    ".zip",
    ".gz",
    ".tgz",
    ".jar",
    ".wasm",
    ".exe",
    ".so",
    ".dylib",
    ".a",
    ".o",
    ".sum",
}

SKIP_NAMES = {"go.sum"}


def tracked_files() -> list[Path]:
    out = subprocess.check_output(["git", "ls-files", "-z"], cwd=REPO_ROOT)
    paths: list[Path] = []
    for raw in out.split(b"\0"):
        if not raw:
            continue
        paths.append(REPO_ROOT / Path(raw.decode()))
    return paths


def should_scan(path: Path) -> bool:
    if path.name in SKIP_NAMES:
        return False
    if path.suffix.lower() in SKIP_SUFFIXES:
        return False
    return path.is_file() and not path.is_symlink()


def main() -> int:
    hits: list[str] = []
    for path in tracked_files():
        if not should_scan(path):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            hits.append(f"{path.relative_to(REPO_ROOT)}: cannot read: {exc}")
            continue
        rel = path.relative_to(REPO_ROOT)
        for lineno, line in enumerate(text.splitlines(), 1):
            for pat in FORBIDDEN:
                if pat.search(line):
                    hits.append(f"{rel}:{lineno}: {line.strip()}")
                    break
    if hits:
        print("Non-MIT license declaration(s) found:", file=sys.stderr)
        for h in hits:
            print(f"  {h}", file=sys.stderr)
        print(
            "\nProject license is MIT. Remove non-MIT headers/SPDX "
            "or use SPDX-License-Identifier: MIT.",
            file=sys.stderr,
        )
        return 1
    print("OK: no non-MIT license declarations in tracked text files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
