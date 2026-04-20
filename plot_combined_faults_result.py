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

    sync_loss_pct = float(value(sync, "not_reaching_consumer_pct"))
    kafka_loss_pct = float(value(kafka, "not_reaching_consumer_pct"))
    sync_loss_count = int(value(sync, "not_reaching_consumer", 0))
    kafka_loss_count = int(value(kafka, "not_reaching_consumer", 0))

    fig, ax = plt.subplots(1, 1, figsize=(8, 5))
    ax.set_title("Combined Faults Result: Reliability Loss", fontsize=14, fontweight="bold")

    bars_sync = ax.bar([x[0] - width / 2], [sync_loss_pct], width, label="Sync", color="#8ecae6")
    bars_kafka = ax.bar([x[1] + width / 2], [kafka_loss_pct], width, label="Kafka", color="#ffb703")
    ax.set_xticks(x, labels)
    ax.set_ylabel("Not reaching consumer (%)")
    ax.legend()

    max_loss = max(sync_loss_pct, kafka_loss_pct, 1.0)
    ax.set_ylim(0, max_loss * 1.4)

    add_value_labels(ax, bars_sync)
    add_value_labels(ax, bars_kafka)

    def add_missed_label(bar, missed_count):
        bar_center = bar.get_x() + bar.get_width() / 2
        bar_height = bar.get_height()
        ax.text(
            bar_center,
            bar_height + 0.6, #max(2.0, max_loss * 0.05),
            f"missed={missed_count}",
            ha="center",
            va="bottom",
            fontsize=8,
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.85, "pad": 1.5},
        )

    add_missed_label(bars_sync[0], sync_loss_count)
    add_missed_label(bars_kafka[0], kafka_loss_count)

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
