"""
visualise.py  -  Benchmark result plots
=========================================
Reads results/all_results.json (written by benchmark.py) and generates
three publication-quality comparison figures:

  Figure 1 - Delivery time distribution (box plot)
             fork vs thread vs select, load-test data

  Figure 2 - CPU usage vs number of clients
             fork vs thread vs select, stress-test data

  Figure 3 - Memory (RSS + PSS) vs number of clients
             fork vs thread vs select, stress-test data

Output: plots/delivery_time_dist.png
        plots/cpu_vs_clients.png
        plots/memory_vs_clients.png
        plots/combined_dashboard.png   (all 3 in one figure)
"""

import json
import sys
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")          # headless / WSL safe
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
except ImportError:
    print("Install matplotlib and numpy:  pip install matplotlib numpy")
    sys.exit(1)

# ── paths ──────────────────────────────────────────────────────────────────────
BASE    = Path(__file__).parent
RESULTS = BASE / "results"
PLOTS   = BASE / "plots"
PLOTS.mkdir(exist_ok=True)

# ── colour scheme ──────────────────────────────────────────────────────────────
COLORS = {
    "fork":   "#E07B54",   # orange-red
    "thread": "#4C9BE8",   # blue
    "select": "#5DBB63",   # green
}
SERVER_LABELS = {
    "fork":   "Fork",
    "thread": "Thread",
    "select": "Select (I/O mux)",
}

plt.rcParams.update({
    "font.family":     "DejaVu Sans",
    "font.size":       11,
    "axes.titlesize":  13,
    "axes.labelsize":  11,
    "axes.grid":       True,
    "grid.alpha":      0.35,
    "figure.dpi":      120,
})


# ════════════════════════════════════════════════════════════════════════════════
# Data loading helpers
# ════════════════════════════════════════════════════════════════════════════════

def load_results() -> dict:
    path = RESULTS / "all_results.json"
    if not path.exists():
        print(f"[!] {path} not found. Run benchmark.py first.")
        sys.exit(1)
    with open(path) as f:
        return json.load(f)


def _get_delivery_times(data: dict, server: str) -> list:
    """Return delivery times list from load-test results."""
    load = data.get(server, {}).get("load", {})
    return load.get("delivery_times_sample", [])


def _get_stress_steps(data: dict, server: str) -> list:
    """Return list of step dicts from stress-test results."""
    return data.get(server, {}).get("stress", {}).get("steps", [])


def _get_monitor_samples(server: str) -> list:
    """Load per-sample CPU/memory data from the server's stress CSV."""
    path = BASE / "results" / f"{server}_stress.json"
    if not path.exists():
        return []
    with open(path) as f:
        d = json.load(f)
    return d.get("monitor_samples", [])


# ════════════════════════════════════════════════════════════════════════════════
# Figure 1 - Delivery time distribution  (box plot)
# ════════════════════════════════════════════════════════════════════════════════

def plot_delivery_distribution(data: dict, ax=None, save=True):
    standalone = ax is None
    if standalone:
        fig, ax = plt.subplots(figsize=(9, 5))

    servers  = [s for s in ["fork", "thread", "select"] if s in data]
    all_data = [_get_delivery_times(data, s) for s in servers]
    labels   = [SERVER_LABELS[s] for s in servers]
    colors   = [COLORS[s] for s in servers]

    # Filter out empty
    valid = [(l, d, c) for l, d, c in zip(labels, all_data, colors) if d]
    if not valid:
        ax.text(0.5, 0.5, "No load-test data available",
                ha="center", va="center", transform=ax.transAxes)
        if standalone and save:
            fig.savefig(PLOTS / "delivery_time_dist.png", bbox_inches="tight")
        return

    labels, all_data, colors = zip(*valid)

    bp = ax.boxplot(all_data, patch_artist=True, notch=False,
                    widths=0.5,
                    medianprops=dict(color="white", linewidth=2.5),
                    whiskerprops=dict(linewidth=1.4),
                    capprops=dict(linewidth=1.4),
                    flierprops=dict(marker=".", markersize=3, alpha=0.4))

    for patch, color in zip(bp["boxes"], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.82)

    ax.set_xticks(range(1, len(labels) + 1))
    ax.set_xticklabels(labels)
    ax.set_ylabel("Delivery Time (ms)")
    ax.set_title("Message Delivery Time Distribution\n(Load Test - 10 Concurrent Clients)")
    ax.set_ylim(bottom=0)

    # Annotate medians
    for i, d in enumerate(all_data):
        if d:
            med = float(np.median(d))
            ax.text(i + 1, med + 0.5, f"{med:.1f}ms",
                    ha="center", va="bottom", fontsize=9,
                    fontweight="bold", color="black")

    if standalone and save:
        fig.tight_layout()
        path = PLOTS / "delivery_time_dist.png"
        fig.savefig(path, bbox_inches="tight")
        print(f"[plot] Saved {path}")
        plt.close(fig)


# ════════════════════════════════════════════════════════════════════════════════
# Figure 2 - CPU usage vs number of clients
# ════════════════════════════════════════════════════════════════════════════════

def plot_cpu_vs_clients(data: dict, ax=None, save=True):
    standalone = ax is None
    if standalone:
        fig, ax = plt.subplots(figsize=(9, 5))

    servers = [s for s in ["fork", "thread", "select"] if s in data]

    for srv in servers:
        steps = _get_stress_steps(data, srv)
        if not steps:
            continue
        xs = [s["num_clients"] for s in steps]
        ys = [s.get("latency_mean_ms") or 0 for s in steps]

        # Use monitor samples if available for CPU per step
        # Otherwise fall back to step latency as a proxy indicator
        mon = _get_monitor_samples(srv)
        if mon:
            # Bin samples by step index (approximately)
            n_steps = len(steps)
            n_samp  = len(mon)
            chunk   = max(1, n_samp // n_steps)
            cpu_per_step = []
            for i in range(n_steps):
                chunk_data = mon[i * chunk: (i + 1) * chunk]
                if chunk_data:
                    avg = sum(s["cpu_pct"] for s in chunk_data) / len(chunk_data)
                    cpu_per_step.append(avg)
                else:
                    cpu_per_step.append(0)
            ys = cpu_per_step

        ax.plot(xs, ys,
                marker="o", linewidth=2.2, markersize=6,
                color=COLORS[srv], label=SERVER_LABELS[srv])
        ax.fill_between(xs, ys, alpha=0.10, color=COLORS[srv])

    ax.set_xlabel("Number of Concurrent Clients")
    ax.set_ylabel("CPU Usage (%)")
    ax.set_title("Server CPU Usage vs Number of Clients\n(Stress Test - Incremental Load)")
    ax.legend(loc="upper left")
    ax.set_ylim(bottom=0)

    if standalone and save:
        fig.tight_layout()
        path = PLOTS / "cpu_vs_clients.png"
        fig.savefig(path, bbox_inches="tight")
        print(f"[plot] Saved {path}")
        plt.close(fig)


# ════════════════════════════════════════════════════════════════════════════════
# Figure 3 - Memory (RSS + PSS) vs number of clients
# ════════════════════════════════════════════════════════════════════════════════

def plot_memory_vs_clients(data: dict, ax=None, save=True):
    standalone = ax is None
    if standalone:
        fig, ax = plt.subplots(figsize=(9, 5))

    servers = [s for s in ["fork", "thread", "select"] if s in data]
    has_data = False

    for srv in servers:
        steps = _get_stress_steps(data, srv)
        if not steps:
            continue
        mon = _get_monitor_samples(srv)
        if not mon:
            continue
        has_data = True

        xs     = [s["num_clients"] for s in steps]
        n_steps = len(steps)
        n_samp  = len(mon)
        chunk   = max(1, n_samp // n_steps)

        rss_vals, pss_vals = [], []
        for i in range(n_steps):
            chunk_data = mon[i * chunk: (i + 1) * chunk]
            if chunk_data:
                rss = sum(s["rss_kb"] for s in chunk_data) / len(chunk_data) / 1024
                pss = sum(s["pss_kb"] for s in chunk_data) / len(chunk_data) / 1024
            else:
                rss = pss = 0
            rss_vals.append(rss)
            pss_vals.append(pss)

        color = COLORS[srv]
        label = SERVER_LABELS[srv]
        ax.plot(xs, rss_vals, marker="o", linewidth=2.2, markersize=6,
                color=color, label=f"{label} - RSS")
        ax.plot(xs, pss_vals, marker="s", linewidth=1.8, markersize=5,
                color=color, linestyle="--", label=f"{label} - PSS", alpha=0.8)

    if not has_data:
        ax.text(0.5, 0.5, "No stress-test monitor data available",
                ha="center", va="center", transform=ax.transAxes)

    ax.set_xlabel("Number of Concurrent Clients")
    ax.set_ylabel("Memory (MB)")
    ax.set_title("Server Memory Usage vs Number of Clients\n(VmRSS = solid, PSS = dashed)")
    ax.legend(loc="upper left", fontsize=9, ncol=2)
    ax.set_ylim(bottom=0)

    if standalone and save:
        fig.tight_layout()
        path = PLOTS / "memory_vs_clients.png"
        fig.savefig(path, bbox_inches="tight")
        print(f"[plot] Saved {path}")
        plt.close(fig)


# ════════════════════════════════════════════════════════════════════════════════
# Figure 4 - Latency percentiles vs clients (bonus)
# ════════════════════════════════════════════════════════════════════════════════

def plot_latency_vs_clients(data: dict, ax=None, save=True):
    """p50 / p95 / p99 latency vs clients for each server."""
    standalone = ax is None
    if standalone:
        fig, ax = plt.subplots(figsize=(9, 5))

    servers = [s for s in ["fork", "thread", "select"] if s in data]
    pct_styles = {"p50": "-", "p95": "--", "p99": ":"}

    for srv in servers:
        steps = _get_stress_steps(data, srv)
        if not steps:
            continue
        xs   = [s["num_clients"] for s in steps]
        color = COLORS[srv]
        for key, ls in pct_styles.items():
            field = f"latency_{key}_ms"
            ys = [s.get(field) or 0 for s in steps]
            label = f"{SERVER_LABELS[srv]} {key.upper()}"
            ax.plot(xs, ys, marker="o", markersize=4, linewidth=1.8,
                    color=color, linestyle=ls, label=label, alpha=0.9)

    ax.set_xlabel("Number of Concurrent Clients")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Latency Percentiles vs Number of Clients\n(p50 solid, p95 dashed, p99 dotted)")
    ax.legend(fontsize=8, ncol=3, loc="upper left")
    ax.set_ylim(bottom=0)

    if standalone and save:
        fig.tight_layout()
        path = PLOTS / "latency_vs_clients.png"
        fig.savefig(path, bbox_inches="tight")
        print(f"[plot] Saved {path}")
        plt.close(fig)


# ════════════════════════════════════════════════════════════════════════════════
# Combined dashboard  (all 4 in one figure)
# ════════════════════════════════════════════════════════════════════════════════

def plot_dashboard(data: dict):
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle("Chat Server Performance Benchmark — Fork vs Thread vs Select",
                 fontsize=15, fontweight="bold", y=1.01)

    plot_delivery_distribution(data, ax=axes[0, 0], save=False)
    plot_latency_vs_clients(   data, ax=axes[0, 1], save=False)
    plot_cpu_vs_clients(       data, ax=axes[1, 0], save=False)
    plot_memory_vs_clients(    data, ax=axes[1, 1], save=False)

    # Shared legend
    handles = [mpatches.Patch(color=COLORS[s], label=SERVER_LABELS[s])
               for s in ["fork", "thread", "select"]]
    fig.legend(handles=handles, loc="lower center", ncol=3,
               fontsize=11, bbox_to_anchor=(0.5, -0.03))

    fig.tight_layout()
    path = PLOTS / "combined_dashboard.png"
    fig.savefig(path, bbox_inches="tight", dpi=130)
    print(f"[plot] Saved {path}")
    plt.close(fig)


# ════════════════════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════════════════════

def main():
    data = load_results()
    servers_found = list(data.keys())
    print(f"[vis] Loaded results for servers: {servers_found}")

    plot_delivery_distribution(data)
    plot_cpu_vs_clients(data)
    plot_memory_vs_clients(data)
    plot_latency_vs_clients(data)
    plot_dashboard(data)

    print(f"\n[vis] All plots saved to {PLOTS}/")
    print("      combined_dashboard.png  ← best for reports")


if __name__ == "__main__":
    main()
