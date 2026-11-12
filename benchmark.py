"""
benchmark.py  –  Performance benchmark harness
================================================
Runs Load Test and Stress Test against all three C chat servers
(fork / thread / select). Saves results to results/ and logs to logs/.

Usage:
    python3 benchmark.py                   # full run, all 3 servers
    python3 benchmark.py --server thread   # single server
    python3 benchmark.py --quick           # shorter durations (CI / smoke)

Test scenarios
--------------
Load Test:
    10 concurrent clients each send BROADCAST messages as fast as
    possible for LOAD_DURATION seconds. Measures latency percentiles
    and throughput.

Stress Test:
    Client count ramps from STRESS_STEP up to STRESS_MAX in steps.
    At each step, clients hammer the server for STRESS_STEP_DUR seconds.
    Stops early when:
      - error rate > 10 %, OR
      - p99 latency  > 5 000 ms, OR
      - fewer than 50 % of clients could connect

Output files
------------
  logs/<server>_<label>_metrics.csv   – per-sample CPU/RSS/PSS (5-s intervals)
  results/<server>_load.json          – load-test summary
  results/<server>_stress.json        – stress-test summary
  results/all_results.json            – combined file used by visualise.py
"""

import os
import sys
import json
import time
import signal
import socket
import subprocess
import threading
import statistics
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(__file__))
from monitor     import ServerMonitor
from bench_client import BenchClient
from protocol    import DISCOVERY_IP, DISCOVERY_PORT

# ── directories ───────────────────────────────────────────────────────────────
BASE     = Path(__file__).parent
LOGS     = BASE / "logs";    LOGS.mkdir(exist_ok=True)
RESULTS  = BASE / "results"; RESULTS.mkdir(exist_ok=True)

# ── server config ─────────────────────────────────────────────────────────────
SERVERS = {
    "fork":   {"bin": str(BASE / "chat_server_fork"),   "port": 8080},
    "thread": {"bin": str(BASE / "chat_server_thread"), "port": 8081},
    "select": {"bin": str(BASE / "chat_server_select"), "port": 8082},
}

# ── test parameters ───────────────────────────────────────────────────────────
LOAD_CLIENTS      = 10
LOAD_DURATION     = 20      # seconds
MSG_INTERVAL      = 0.05    # seconds between sends per client  (20 msg/s)

STRESS_STEP       = 5       # add this many clients per ramp step
STRESS_MAX        = 30      # upper bound (MAX_CLIENTS in common.h is 10; keep ≤ 10 per server)
STRESS_STEP_DUR   = 12      # seconds per step

MONITOR_INTERVAL  = 5.0     # seconds between metric samples


# ════════════════════════════════════════════════════════════════════════════════
# Server lifecycle helpers
# ════════════════════════════════════════════════════════════════════════════════

_disc_proc = None

def start_discovery() -> subprocess.Popen:
    global _disc_proc
    if _disc_proc and _disc_proc.poll() is None:
        return _disc_proc
    _disc_proc = subprocess.Popen(
        [str(BASE / "discovery_server")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.0)
    print(f"[bench] Discovery server started  PID={_disc_proc.pid}")
    return _disc_proc


def stop_discovery():
    global _disc_proc
    if _disc_proc:
        _disc_proc.terminate()
        try: _disc_proc.wait(timeout=3)
        except: _disc_proc.kill()
        _disc_proc = None


def start_chat_server(server_type: str) -> subprocess.Popen:
    cfg  = SERVERS[server_type]
    proc = subprocess.Popen(
        [cfg["bin"]],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _wait_for_port(DISCOVERY_IP, cfg["port"], timeout=5)
    print(f"[bench] {server_type} server started  PID={proc.pid}  port={cfg['port']}")
    return proc


def stop_chat_server(proc: subprocess.Popen):
    if proc and proc.poll() is None:
        proc.terminate()
        try: proc.wait(timeout=4)
        except: proc.kill()


def _wait_for_port(host: str, port: int, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=0.3)
            s.close(); return True
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"Port {port} not ready after {timeout}s")


# ════════════════════════════════════════════════════════════════════════════════
# Client pool helpers
# ════════════════════════════════════════════════════════════════════════════════

def _connect_clients(server_type: str, n: int, base_idx: int = 0) -> list:
    """
    Connect *n* BenchClients concurrently.
    Returns list of successfully connected clients.
    """
    def _make(i):
        username = f"u{base_idx + i:04d}"
        password = f"p{base_idx + i:04d}"
        port     = 6000 + base_idx + i
        c = BenchClient(server_type, username, password, port)
        return c if c.setup() else None

    clients = []
    with ThreadPoolExecutor(max_workers=min(n, 20)) as ex:
        futs = {ex.submit(_make, i): i for i in range(n)}
        for f in as_completed(futs):
            c = f.result()
            if c:
                clients.append(c)
    return clients


def _run_send_phase(clients: list, duration: float, interval: float):
    """All clients send broadcasts concurrently for *duration* seconds."""
    stop = threading.Event()

    def sender(c: BenchClient):
        while not stop.is_set():
            c.send_broadcast()
            stop.wait(interval)

    threads = [threading.Thread(target=sender, args=(c,), daemon=True)
               for c in clients]
    for t in threads: t.start()
    time.sleep(duration)
    stop.set()
    for t in threads: t.join(timeout=3)


def _disconnect_clients(clients: list):
    with ThreadPoolExecutor(max_workers=20) as ex:
        list(ex.map(lambda c: c.stop(), clients))


# ════════════════════════════════════════════════════════════════════════════════
# Result aggregation
# ════════════════════════════════════════════════════════════════════════════════

def _aggregate(clients: list, num_clients: int) -> dict:
    all_dt      = []
    total_sent  = 0
    total_recv  = 0
    total_err   = 0

    for c in clients:
        all_dt.extend(c.delivery_times)
        total_sent += c.messages_sent
        total_recv += c.messages_recv
        total_err  += c.errors

    if not all_dt:
        return {
            "num_clients":    num_clients,
            "messages_sent":  total_sent,
            "messages_recv":  total_recv,
            "errors":         total_err,
            "delivery_times": [],
            "latency_mean_ms": None,
            "latency_p50_ms":  None,
            "latency_p95_ms":  None,
            "latency_p99_ms":  None,
            "latency_max_ms":  None,
            "throughput_mps":  0,
        }

    s = sorted(all_dt)
    def pct(p): return s[int(len(s) * p / 100)]

    return {
        "num_clients":     num_clients,
        "messages_sent":   total_sent,
        "messages_recv":   total_recv,
        "errors":          total_err,
        "delivery_times":  all_dt,
        "latency_mean_ms": round(statistics.mean(all_dt), 2),
        "latency_p50_ms":  round(pct(50), 2),
        "latency_p95_ms":  round(pct(95), 2),
        "latency_p99_ms":  round(pct(99), 2),
        "latency_max_ms":  round(max(all_dt), 2),
        "throughput_mps":  round(total_sent / max(sum(
            [len(c.delivery_times) for c in clients]), 1), 2),
    }


# ════════════════════════════════════════════════════════════════════════════════
# Load Test
# ════════════════════════════════════════════════════════════════════════════════

def run_load_test(server_type: str, n_clients: int = LOAD_CLIENTS,
                  duration: float = LOAD_DURATION) -> dict:
    print(f"\n{'='*60}")
    print(f"  LOAD TEST  |  server={server_type}  |  clients={n_clients}  |  duration={duration}s")
    print(f"{'='*60}")

    srv_proc = start_chat_server(server_type)
    monitor  = ServerMonitor(srv_proc.pid,
                             label    = f"{server_type}_load_{n_clients}c",
                             log_dir  = str(LOGS),
                             interval = MONITOR_INTERVAL)
    monitor.start()

    try:
        clients = _connect_clients(server_type, n_clients)
        print(f"  Connected: {len(clients)}/{n_clients}")
        if not clients:
            return {}

        _run_send_phase(clients, duration, MSG_INTERVAL)
        result = _aggregate(clients, len(clients))
        _disconnect_clients(clients)

    finally:
        monitor.stop()
        stop_chat_server(srv_proc)
        time.sleep(1)

    srv_metrics = monitor.summary()
    result["server_metrics"] = srv_metrics

    print(f"  Latency  mean={result['latency_mean_ms']}ms  "
          f"p50={result['latency_p50_ms']}ms  "
          f"p95={result['latency_p95_ms']}ms  "
          f"p99={result['latency_p99_ms']}ms")
    print(f"  CPU mean={srv_metrics.get('cpu_mean_pct')}%  "
          f"RSS mean={srv_metrics.get('rss_mean_kb')}kB  "
          f"PSS mean={srv_metrics.get('pss_mean_kb')}kB")

    # Save per-server result
    out = {
        "test":   "load",
        "server": server_type,
        **{k: v for k, v in result.items() if k != "delivery_times"},
        "delivery_times_sample": result["delivery_times"][:1000],
    }
    (RESULTS / f"{server_type}_load.json").write_text(json.dumps(out, indent=2))
    return result


# ════════════════════════════════════════════════════════════════════════════════
# Stress Test
# ════════════════════════════════════════════════════════════════════════════════

def run_stress_test(server_type: str,
                    max_clients: int = STRESS_MAX,
                    step: int = STRESS_STEP,
                    step_dur: float = STRESS_STEP_DUR) -> dict:
    print(f"\n{'='*60}")
    print(f"  STRESS TEST  |  server={server_type}  |  max_clients={max_clients}  |  step={step}")
    print(f"{'='*60}")

    srv_proc = start_chat_server(server_type)
    monitor  = ServerMonitor(srv_proc.pid,
                             label    = f"{server_type}_stress",
                             log_dir  = str(LOGS),
                             interval = MONITOR_INTERVAL)
    monitor.start()

    steps = []
    try:
        base_idx   = 0
        step_sizes = range(step, max_clients + 1, step)

        for n in step_sizes:
            print(f"\n  -- step: {n} clients --")
            clients = _connect_clients(server_type, n, base_idx)
            base_idx += n
            connected = len(clients)
            print(f"     Connected: {connected}/{n}")

            if connected == 0:
                print(f"     No clients connected – stopping stress test")
                break

            _run_send_phase(clients, step_dur, MSG_INTERVAL)
            res = _aggregate(clients, connected)
            _disconnect_clients(clients)
            time.sleep(0.5)

            print(f"     mean={res['latency_mean_ms']}ms  "
                  f"p99={res['latency_p99_ms']}ms  "
                  f"err={res['errors']}")

            steps.append({k: v for k, v in res.items() if k != "delivery_times"})

            # Degradation check
            if res["errors"] > 0 and res["messages_sent"] > 0:
                err_rate = res["errors"] / res["messages_sent"]
                if err_rate > 0.10:
                    print(f"  !! Error rate {err_rate:.1%} > 10% – stopping")
                    break

            if res["latency_p99_ms"] and res["latency_p99_ms"] > 5000:
                print(f"  !! p99 latency {res['latency_p99_ms']}ms > 5s – stopping")
                break

            if connected / n < 0.5:
                print(f"  !! Only {connected}/{n} connected – stopping")
                break

    finally:
        monitor.stop()
        stop_chat_server(srv_proc)
        time.sleep(1)

    srv_metrics = monitor.summary()
    raw_samples = [
        {"timestamp": s[0], "cpu_pct": s[1], "rss_kb": s[2], "pss_kb": s[3]}
        for s in monitor.get_samples()
    ]

    out = {
        "test":            "stress",
        "server":          server_type,
        "steps":           steps,
        "server_metrics":  srv_metrics,
        "monitor_samples": raw_samples,
    }
    (RESULTS / f"{server_type}_stress.json").write_text(json.dumps(out, indent=2))
    return out


# ════════════════════════════════════════════════════════════════════════════════
# Summary report
# ════════════════════════════════════════════════════════════════════════════════

def _print_summary_table(all_results: dict):
    print("\n" + "="*70)
    print("  SUMMARY REPORT")
    print("="*70)
    print(f"  {'Server':<10} {'Test':<8} {'Clients':>7} {'Mean ms':>8} "
          f"{'p95 ms':>8} {'p99 ms':>8} {'CPU%':>6} {'RSS MB':>7}")
    print("-"*70)

    for srv, tests in all_results.items():
        for test_name, r in tests.items():
            if test_name == "load":
                sm = r.get("server_metrics", {})
                print(f"  {srv:<10} {test_name:<8} "
                      f"{r.get('num_clients','-'):>7} "
                      f"{r.get('latency_mean_ms') or '-':>8} "
                      f"{r.get('latency_p95_ms')  or '-':>8} "
                      f"{r.get('latency_p99_ms')  or '-':>8} "
                      f"{sm.get('cpu_mean_pct')   or '-':>6} "
                      f"{int((sm.get('rss_mean_kb') or 0)/1024):>7}")
            elif test_name == "stress" and r.get("steps"):
                last = r["steps"][-1]
                sm   = r.get("server_metrics", {})
                print(f"  {srv:<10} {test_name:<8} "
                      f"{last.get('num_clients','-'):>7} "
                      f"{last.get('latency_mean_ms') or '-':>8} "
                      f"{last.get('latency_p95_ms')  or '-':>8} "
                      f"{last.get('latency_p99_ms')  or '-':>8} "
                      f"{sm.get('cpu_mean_pct')       or '-':>6} "
                      f"{int((sm.get('rss_mean_kb') or 0)/1024):>7}")
    print("="*70)


# ════════════════════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Chat server benchmark")
    parser.add_argument("--server",   choices=["fork","thread","select","all"],
                        default="all")
    parser.add_argument("--test",     choices=["load","stress","all"],
                        default="all")
    parser.add_argument("--clients",  type=int,   default=LOAD_CLIENTS)
    parser.add_argument("--duration", type=float, default=LOAD_DURATION)
    parser.add_argument("--max-clients", type=int, default=STRESS_MAX)
    parser.add_argument("--step",     type=int,   default=STRESS_STEP)
    parser.add_argument("--quick",    action="store_true",
                        help="Short durations for a quick smoke run")
    args = parser.parse_args()

    if args.quick:
        args.duration    = 8
        args.max_clients = 10
        args.step        = 5
        global STRESS_STEP_DUR
        STRESS_STEP_DUR  = 6

    servers = ["fork", "thread", "select"] if args.server == "all" \
              else [args.server]

    start_discovery()
    time.sleep(0.5)

    all_results = {}
    try:
        for srv in servers:
            all_results[srv] = {}
            if args.test in ("load", "all"):
                r = run_load_test(srv, args.clients, args.duration)
                all_results[srv]["load"] = r
                time.sleep(2)

            if args.test in ("stress", "all"):
                r = run_stress_test(srv, args.max_clients, args.step)
                all_results[srv]["stress"] = r
                time.sleep(2)
    finally:
        stop_discovery()

    # Write combined JSON for visualise.py
    combined = {}
    for srv, tests in all_results.items():
        combined[srv] = {}
        for t, r in tests.items():
            combined[srv][t] = {k: v for k, v in r.items()
                                if k != "delivery_times"}
            if "delivery_times" in r:
                combined[srv][t]["delivery_times_sample"] = \
                    r["delivery_times"][:2000]
    (RESULTS / "all_results.json").write_text(json.dumps(combined, indent=2))

    _print_summary_table(all_results)
    print(f"\n[bench] Results saved to {RESULTS}/")
    print(f"[bench] Metric logs saved to {LOGS}/")
    print(f"[bench] Run  python3 visualise.py  to generate plots")


if __name__ == "__main__":
    main()
