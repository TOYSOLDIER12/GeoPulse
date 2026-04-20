import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt


def load_result(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def get_sync(result: dict) -> dict:
    return result["sync"]


def get_kafka(result: dict) -> dict:
    return result["kafka"]


def value(dct: dict, key: str, default=0.0):
    return dct.get(key, default)


def add_value_labels(ax, bars):
    for bar in bars:
        height = bar.get_height()
        ax.annotate(
            f"{height:.1f}",
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 3),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=9,
        )


def build_plot(result: dict, output_png: Path):
    sync = get_sync(result)
    kafka = get_kafka(result)
    labels = ["Sync", "Kafka"]
    x = [0, 1]
    width = 0.35

    sync_time = float(value(sync, "write_seconds"))
    kafka_time = float(value(kafka, "end_to_end_seconds"))
    sync_eps = float(value(sync, "events_per_second"))
    kafka_eps = float(value(kafka, "events_per_second"))
    sync_loss_pct = float(value(sync, "not_reaching_consumer_pct"))
    kafka_loss_pct = float(value(kafka, "not_reaching_consumer_pct"))
    sync_loss_count = int(value(sync, "not_reaching_consumer", 0))
    kafka_loss_count = int(value(kafka, "not_reaching_consumer", 0))

    produced = int(value(kafka, "produced", value(result.get("dataset", {}), "total_events", 0)))
    consumed = int(value(kafka, "consumed", value(kafka, "consumed_by_consumer", 0)))
    delivery_gap = int(value(kafka, "delivery_gap", max(produced - consumed, 0)))

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    axes = axes.flatten()
    fig.suptitle("Combined Faults Result: Sync vs Kafka", fontsize=14, fontweight="bold")

    ax0 = axes[0]
    bars = ax0.bar(labels, [sync_time, kafka_time], color=["#2a9d8f", "#e76f51"])
    ax0.set_ylabel("Seconds")
    ax0.set_title("Execution Time")
    add_value_labels(ax0, bars)

    ax1 = axes[1]
    bars = ax1.bar(labels, [sync_eps, kafka_eps], color=["#264653", "#f4a261"])
    ax1.set_ylabel("Events / second")
    ax1.set_title("Throughput")
    add_value_labels(ax1, bars)

    ax2 = axes[2]
    bars_sync = ax2.bar([x[0] - width / 2], [sync_loss_pct], width, label="Sync", color="#8ecae6")
    bars_kafka = ax2.bar([x[1] + width / 2], [kafka_loss_pct], width, label="Kafka", color="#ffb703")
    ax2.set_xticks(x, labels)
    ax2.set_ylabel("Not reaching consumer (%)")
    ax2.set_title("Reliability Loss")
    ax2.legend()
    add_value_labels(ax2, bars_sync)
    add_value_labels(ax2, bars_kafka)
    ax2.annotate(
        f"missed={sync_loss_count}",
        xy=(x[0] - width / 2, sync_loss_pct),
        xytext=(0, 18),
        textcoords="offset points",
        ha="center",
        fontsize=8,
    )
    ax2.annotate(
        f"missed={kafka_loss_count}",
        xy=(x[1] + width / 2, kafka_loss_pct),
        xytext=(0, 18),
        textcoords="offset points",
        ha="center",
        fontsize=8,
    )

    ax3 = axes[3]
    bars = ax3.bar(["Produced", "Consumed", "Gap"], [produced, consumed, delivery_gap], color=["#577590", "#90be6d", "#e63946"])
    ax3.set_ylabel("Events")
    ax3.set_title("Kafka Delivery Check")
    add_value_labels(ax3, bars)

    plt.tight_layout()
    fig.savefig(output_png, dpi=150)
    print(f"[INFO] Plot saved to {output_png}")


def parse_args():
    parser = argparse.ArgumentParser(description="Plot a single combined sync-vs-Kafka result JSON.")
    parser.add_argument("--json", required=True, help="Combined result JSON file")
    parser.add_argument("--output-png", default="combined_faults_result.png")
    return parser.parse_args()


def main():
    args = parse_args()
    result = load_result(Path(args.json))
    build_plot(result, Path(args.output_png))


if __name__ == "__main__":
    main()
