"""
monitor.py  -  Server metrics collector
========================================
Reads CPU %, VmRSS, and PSS from /proc for a given server PID.
Runs in a background thread; logs one CSV row every `interval` seconds.
Call summary() after stop() to get a final dict.

Metrics:
  cpu_pct   - CPU usage in percent  (sampled over interval window)
  rss_kb    - VmRSS from /proc/<pid>/status  (physical RAM pages)
  pss_kb    - PSS  from /proc/<pid>/smaps_rollup (proportional set size)
"""

import os, time, threading, csv
from pathlib import Path


def _read_proc_stat(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().split()
        return int(fields[13]), int(fields[14])
    except Exception:
        return 0, 0


def _cpu_percent(pid, interval):
    """CPU % consumed by pid over `interval` seconds."""
    hz = os.sysconf("SC_CLK_TCK")
    u1, s1 = _read_proc_stat(pid)
    t1 = time.perf_counter()
    time.sleep(interval)
    u2, s2 = _read_proc_stat(pid)
    t2 = time.perf_counter()
    ticks = (u2 + s2) - (u1 + s1)
    wall  = t2 - t1
    return (ticks / hz / wall) * 100.0 if wall > 0 else 0.0


def _read_rss_kb(pid):
    """VmRSS (kB) from /proc/<pid>/status."""
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except Exception:
        pass
    return 0


def _read_pss_kb(pid):
    """PSS (kB) - tries smaps_rollup first, falls back to smaps."""
    try:
        with open(f"/proc/{pid}/smaps_rollup") as f:
            for line in f:
                if line.startswith("Pss:"):
                    return int(line.split()[1])
    except FileNotFoundError:
        pass
    except Exception:
        return 0
    total = 0
    try:
        with open(f"/proc/{pid}/smaps") as f:
            for line in f:
                if line.startswith("Pss:"):
                    total += int(line.split()[1])
    except Exception:
        pass
    return total


class ServerMonitor:
    """
    Samples CPU / RSS / PSS for *pid* every *interval* seconds in a
    background thread and streams rows to a CSV file.

    Usage:
        mon = ServerMonitor(pid=1234, label="thread_10c", log_dir="logs")
        mon.start()
        # run test...
        mon.stop()
        print(mon.summary())
    """

    def __init__(self, pid, label, log_dir="logs", interval=5.0):
        self.pid      = pid
        self.label    = label
        self.interval = interval
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        self._csv_path = Path(log_dir) / f"{label}_metrics.csv"
        self._samples  = []
        self._stop_evt = threading.Event()
        self._thread   = threading.Thread(
            target=self._loop, name=f"monitor-{label}", daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_evt.set()
        self._thread.join(timeout=self.interval + 3)

    def summary(self):
        if not self._samples:
            return {"label": self.label, "samples": 0}
        cpus = [s[1] for s in self._samples]
        rsss = [s[2] for s in self._samples]
        psss = [s[3] for s in self._samples]
        return {
            "label":        self.label,
            "samples":      len(self._samples),
            "cpu_mean_pct": round(sum(cpus) / len(cpus), 2),
            "cpu_max_pct":  round(max(cpus), 2),
            "rss_mean_kb":  int(sum(rsss) / len(rsss)),
            "rss_max_kb":   max(rsss),
            "pss_mean_kb":  int(sum(psss) / len(psss)),
            "pss_max_kb":   max(psss),
            "csv_path":     str(self._csv_path),
        }

    def get_samples(self):
        return list(self._samples)

    def _loop(self):
        with open(self._csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "cpu_pct", "rss_kb", "pss_kb"])
            while not self._stop_evt.is_set():
                try:
                    cpu = _cpu_percent(self.pid, self.interval)
                    rss = _read_rss_kb(self.pid)
                    pss = _read_pss_kb(self.pid)
                    ts  = time.time()
                    row = (ts, round(cpu, 2), rss, pss)
                    self._samples.append(row)
                    writer.writerow(row)
                    f.flush()
                except Exception as exc:
                    if not self._stop_evt.is_set():
                        print(f"[Monitor:{self.label}] stopped: {exc}")
                    break
