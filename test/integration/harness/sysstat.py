"""Host resource snippets for integration harness progress logs (Linux CI)."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

_GiB = 1024**3

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


def cpu_busy_pct() -> float | None:
    """Host CPU busy % since the previous call (None on the first call, non-Linux)."""
    global _last_cpu

    cur = _read_cpu_jiffies()
    if cur is None:
        return None
    busy: float | None = None
    if _last_cpu is not None:
        d_idle = cur[0] - _last_cpu[0]
        d_total = cur[1] - _last_cpu[1]
        if d_total > 0:
            busy = 100.0 * (1.0 - d_idle / d_total)
    _last_cpu = cur
    return busy


def mem_mib() -> tuple[float, float] | None:
    """(available MiB, total MiB) from /proc/meminfo; None when unavailable."""
    mem = _read_meminfo()
    total_kb = mem.get("MemTotal", 0)
    if total_kb <= 0:
        return None
    avail_kb = mem.get("MemAvailable", mem.get("MemFree", 0))
    return avail_kb / 1024.0, total_kb / 1024.0


def proc_rss_mib(pid: int | None) -> float | None:
    """Resident set size of ``pid`` in MiB; None when it cannot be read (non-Linux)."""
    if pid is None or pid <= 0:
        return None
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except (OSError, ValueError, IndexError):
        return None
    return None


def format_cpu_mem() -> str:
    """Host loadavg + /proc CPU% since last call + MemAvailable/MemTotal."""
    parts: list[str] = []
    try:
        load1, load5, load15 = os.getloadavg()
        parts.append(f"load={load1:.2f}/{load5:.2f}/{load15:.2f}")
    except OSError:
        parts.append("load=?")

    busy = cpu_busy_pct()
    parts.append(f"cpu={busy:.0f}%" if busy is not None else "cpu=?")

    usage = mem_mib()
    if usage is not None:
        avail_mib, total_mib = usage
        used_pct = 100.0 * (1.0 - avail_mib / total_mib) if total_mib else 0.0
        parts.append(
            f"mem avail={avail_mib / 1024:.1f}GiB/{total_mib / 1024:.1f}GiB used={used_pct:.0f}%"
        )
    else:
        parts.append("mem=?")

    return " | ".join(parts)


def format_proc_rss(pid: int | None) -> str:
    """Optional process RSS for go-cdc (or any pid)."""
    if pid is None or pid <= 0:
        return ""
    rss = proc_rss_mib(pid)
    if rss is None:
        return f"cdc_rss=? pid={pid}"
    return f"cdc_rss={rss:.0f}MiB"


def format_host_res(pid: int | None = None) -> str:
    """CPU/mem line plus optional go-cdc RSS for catch-up progress logs."""
    base = format_cpu_mem()
    rss = format_proc_rss(pid)
    return f"{base} | {rss}" if rss else base
