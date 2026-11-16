"""Plot fusion/replication metrics against k, one figure per workload.

Install the plotting dependency with: pip install matplotlib
Run from the repo root:
    python -m chord_replication.visualize /tmp/all_k_comparison.csv
    python -m chord_replication.visualize results.csv --n 8 --output comparison.png

Multiple workloads produce separate files named comparison_01.png, etc.
Backup percentages use logical UTF-8 key/value bytes as their denominator.
"""

import argparse
from collections import defaultdict
import csv
import math
from pathlib import Path


# CSV column or derived value, subplot title, y-axis label, display scale.
METRICS = (
    ("total_storage_bytes", "Total storage (primary + backup + metadata)", "KiB", 1 / 1024),
    ("backup_storage_bytes", "Backup storage", "KiB", 1 / 1024),
    ("backup_to_data_ratio", "Backup / logical key-value data", "%", 100),
    ("write_latency", "Mean write latency", "Simulated time units", 1),
    ("read_latency", "Mean read latency", "Simulated time units", 1),
    ("delete_latency", "Mean delete latency", "Simulated time units", 1),
    ("recovery_time", "Recovery duration", "Simulated time units", 1),
    ("recovery_messages", "Recovery messages", "Messages", 1),
    ("recovery_bytes", "Recovery data transferred", "KiB", 1 / 1024),
    ("message_count", "Total messages", "Messages", 1),
    ("bytes_transferred", "Total data transferred", "KiB", 1 / 1024),
    ("recovery_cpu_seconds", "Recovery CPU time", "Actual milliseconds", 1000),
)

SCENARIO_FIELDS = (
    "n", "records_per_node_target", "value_bytes", "seed", "network_delay",
    "bandwidth_bytes_per_unit",
)
INTEGER_FIELDS = {"n", "records_per_node_target", "value_bytes", "seed", "k"}
STYLES = {
    "fusion": ("Fusion", "#2563eb", "o", "-"),
    "chord_replication": ("Chord replication", "#d97706", "^", "--"),
    "replication": ("Indexed replication", "#059669", "s", ":"),
}


def load_results(path):
    """Validate CSV values and group matching workload/network configurations."""
    groups = defaultdict(list)
    seen = set()
    required = {"scheme", "k", "primary_payload_bytes", *SCENARIO_FIELDS,
                *(metric[0] for metric in METRICS if metric[0] != "backup_to_data_ratio")}
    with Path(path).open(newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV is missing columns: {', '.join(sorted(missing))}")
        for line, source in enumerate(reader, start=2):
            row = {"scheme": (source.get("scheme") or "").strip()}
            if not row["scheme"]:
                raise ValueError(f"Line {line}: scheme is empty")
            for column in sorted(required - {"scheme"}):
                try:
                    number = float(source[column])
                    if not math.isfinite(number):
                        raise ValueError("not finite")
                    if column in INTEGER_FIELDS:
                        if not number.is_integer():
                            raise ValueError("not an integer")
                        number = int(number)
                    if column != "seed" and number < 0:
                        raise ValueError("negative value")
                    row[column] = number
                except (TypeError, ValueError) as error:
                    raise ValueError(f"Line {line}: invalid {column}") from error
            if not 1 <= row["k"] < row["n"]:
                raise ValueError(f"Line {line}: expected 1 <= k < n")
            if row["records_per_node_target"] < 1 or row["bandwidth_bytes_per_unit"] <= 0:
                raise ValueError(f"Line {line}: record count and bandwidth must be positive")
            # Physical primary memory includes scheme-specific metadata. Using
            # logical data prevents a larger primary representation from making
            # a scheme's backup percentage appear artificially smaller.
            payload = row["primary_payload_bytes"]
            row["backup_to_data_ratio"] = row["backup_storage_bytes"] / payload if payload else math.nan
            scenario = tuple(row[column] for column in SCENARIO_FIELDS)
            identity = (scenario, row["scheme"], row["k"])
            if identity in seen:
                raise ValueError(f"Line {line}: duplicate scheme/k for the same workload")
            seen.add(identity)
            groups[scenario].append(row)
    if not groups:
        raise ValueError("CSV contains no benchmark rows")
    return dict(groups)


def plot_results(groups, output, show=False):
    """Save figures and return their paths. Matplotlib is imported only here."""
    import matplotlib

    if not show:
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator, ScalarFormatter

    output = Path(output)
    if output.suffix.lower() not in {".png", ".pdf", ".svg"}:
        raise ValueError("Output extension must be .png, .pdf, or .svg")
    output.parent.mkdir(parents=True, exist_ok=True)
    paths = []
    for number, (scenario, rows) in enumerate(sorted(groups.items()), start=1):
        n, records, value_bytes, seed, delay, bandwidth = scenario
        fig, axes = plt.subplots(4, 3, figsize=(15, 15), constrained_layout=True)
        fig.suptitle(
            f"Fusion and Chord replication across k · n={n}\n"
            f"{records} target records/node · {value_bytes}-byte values · seed {seed}\n"
            f"Delay {delay:g} · bandwidth {bandwidth:g} bytes/unit",
            fontsize=15,
        )
        schemes = sorted({row["scheme"] for row in rows})
        ticks = sorted({row["k"] for row in rows})
        for axis, (column, title, units, scale) in zip(axes.flat, METRICS):
            for scheme in schemes:
                series = sorted((row for row in rows if row["scheme"] == scheme),
                                key=lambda row: row["k"])
                label, color, marker, linestyle = STYLES.get(scheme, (scheme, None, "o", "-"))
                axis.plot([row["k"] for row in series],
                          [row[column] * scale for row in series],
                          label=label, color=color, marker=marker,
                          linestyle=linestyle, linewidth=1.8, markersize=5,
                          markerfacecolor="white")
            axis.set_title(title, fontsize=11, loc="left")
            axis.set_xlabel("k (tolerated node failures)")
            axis.set_ylabel(units)
            if len(ticks) <= 12:
                axis.set_xticks(ticks)
            else:
                axis.xaxis.set_major_locator(MaxNLocator(integer=True))
            maximum = max((row[column] * scale for row in rows
                           if math.isfinite(row[column])), default=0)
            if column == "backup_to_data_ratio" and not maximum:
                axis.text(0.5, 0.5, "Undefined for empty logical data",
                          ha="center", va="center", transform=axis.transAxes)
            axis.set_ylim(0, maximum * 1.12 if maximum else 1)
            formatter = ScalarFormatter(useOffset=False)
            formatter.set_scientific(False)
            axis.yaxis.set_major_formatter(formatter)
            axis.grid(alpha=0.25)
            axis.spines[["top", "right"]].set_visible(False)
        axes[0, 0].legend(frameon=False, fontsize=9)
        fig.supxlabel(
            "Storage includes Python metadata. Percentage denominator is logical UTF-8 key/value data.\n"
            "Network latency uses simulated time and excludes coding CPU and shared-link contention.",
            fontsize=9,
        )
        path = output if len(groups) == 1 else output.with_name(
            f"{output.stem}_{number:02d}{output.suffix}"
        )
        fig.savefig(path, dpi=170)
        paths.append(path)
        if not show:
            plt.close(fig)
    if show:
        plt.show()
        plt.close("all")
    return paths


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", nargs="?", type=Path, default=Path("/tmp/all_k_comparison.csv"))
    parser.add_argument("--output", type=Path, help="PNG, PDF, or SVG path; defaults to the CSV name with .png")
    parser.add_argument("--n", type=int, help="Select a ring size")
    parser.add_argument("--records", type=int, help="Select target records per node")
    parser.add_argument("--value-bytes", type=int, help="Select value size")
    parser.add_argument("--show", action="store_true", help="Also open the Matplotlib window")
    args = parser.parse_args()
    try:
        groups = load_results(args.csv)
        groups = {scenario: rows for scenario, rows in groups.items()
                  if (args.n is None or scenario[0] == args.n)
                  and (args.records is None or scenario[1] == args.records)
                  and (args.value_bytes is None or scenario[2] == args.value_bytes)}
        if not groups:
            raise ValueError("No benchmark rows match the selected filters")
        paths = plot_results(groups, args.output or args.csv.with_suffix(".png"), args.show)
    except ImportError:
        parser.error("Install the plotting dependency with: pip install matplotlib")
    except (OSError, ValueError) as error:
        parser.error(str(error))
    for path in paths:
        print(f"Saved {path}")


if __name__ == "__main__":
    main()
