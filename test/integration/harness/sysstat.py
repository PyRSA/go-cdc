"""Host resource snippets for integration harness progress logs (Linux CI)."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

_GiB = 1024**3
_MiB = 1024**2

# (idle, total) jiffies from previous /proc/stat sample for delta CPU %.
_last_cpu: tuple[int, int] | None = None


def _read_meminfo() -> dict[str, int]:
    out: dict[str, int] = {}
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and parts[0].endswith(":"):
                    out[parts[0][:-1]] = int(parts[1])  # kB
    except OSError:
        pass
    return out


def _read_cpu_jiffies() -> tuple[int, int] | None:
    """Return (idle_jiffies, total_jiffies) from the aggregate cpu line."""
    try:
        with open("/proc/stat", encoding="utf-8") as f:
            line = f.readline()
    except OSError:
        return None
    if not line.startswith("cpu "):
        return None
    fields = [int(x) for x in line.split()[1:]]
    if len(fields) < 4:
        return None
    idle = fields[3] + (fields[4] if len(fields) > 4 else 0)  # idle + iowait
    # guest/guest_nice (fields[8]/[9]) are already included in user/nice.
    total = sum(fields[:8])
    return idle, total


def format_disk(path: str | Path | None = None) -> str:
    """Free / total disk for ``path`` (default: cwd)."""
    target = Path(path) if path is not None else Path.cwd()
    try:
        usage = shutil.disk_usage(target)
    except OSError as exc:
        return f"disk=? ({target}: {exc})"
    free_g = usage.free / _GiB
    total_g = usage.total / _GiB
    used_pct = 100.0 * (1.0 - usage.free / usage.total) if usage.total else 0.0
    return f"disk free={free_g:.1f}GiB/{total_g:.1f}GiB used={used_pct:.0f}% path={target}"


def format_cpu_mem() -> str:
    """Host loadavg + /proc CPU% since last call + MemAvailable/MemTotal."""
    global _last_cpu

    parts: list[str] = []
    try:
        load1, load5, load15 = os.getloadavg()
        parts.append(f"load={load1:.2f}/{load5:.2f}/{load15:.2f}")
    except OSError:
        parts.append("load=?")

    cur = _read_cpu_jiffies()
    if cur is not None and _last_cpu is not None:
        d_idle = cur[0] - _last_cpu[0]
        d_total = cur[1] - _last_cpu[1]
        if d_total > 0:
            busy_pct = 100.0 * (1.0 - d_idle / d_total)
            parts.append(f"cpu={busy_pct:.0f}%")
        else:
            parts.append("cpu=?")
    else:
        parts.append("cpu=?")
    if cur is not None:
        _last_cpu = cur

    mem = _read_meminfo()
    total_kb = mem.get("MemTotal", 0)
    avail_kb = mem.get("MemAvailable", mem.get("MemFree", 0))
    if total_kb > 0:
        avail_g = avail_kb * 1024 / _GiB
        total_g = total_kb * 1024 / _GiB
        used_pct = 100.0 * (1.0 - avail_kb / total_kb)
        parts.append(f"mem avail={avail_g:.1f}GiB/{total_g:.1f}GiB used={used_pct:.0f}%")
    else:
        parts.append("mem=?")

    return " | ".join(parts)


def format_proc_rss(pid: int | None) -> str:
    """Optional process RSS for go-cdc (or any pid)."""
    if pid is None or pid <= 0:
        return ""
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    kb = int(line.split()[1])
                    return f"cdc_rss={kb * 1024 / _MiB:.0f}MiB"
    except (OSError, ValueError, IndexError):
        return f"cdc_rss=? pid={pid}"
    return f"cdc_rss=? pid={pid}"


def format_host_res(pid: int | None = None) -> str:
    """CPU/mem line plus optional go-cdc RSS for catch-up progress logs."""
    base = format_cpu_mem()
    rss = format_proc_rss(pid)
    return f"{base} | {rss}" if rss else base
