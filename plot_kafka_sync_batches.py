import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt


def load_result(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def pick_sync_time(result: dict) -> float:
    return float(result["sync"]["write_seconds"])


def pick_kafka_time(result: dict) -> float:
    return float(result["kafka"]["end_to_end_seconds"])


def pick_sync_eps(result: dict) -> float:
    return float(result["sync"]["events_per_second"])


def pick_kafka_eps(result: dict) -> float:
    return float(result["kafka"]["events_per_second"])


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


def build_plot(small: dict, big: dict, output_png: Path):
    labels = ["Small batch", "Big batch"]

    sync_time = [pick_sync_time(small), pick_sync_time(big)]
    kafka_time = [pick_kafka_time(small), pick_kafka_time(big)]

    sync_eps = [pick_sync_eps(small), pick_sync_eps(big)]
    kafka_eps = [pick_kafka_eps(small), pick_kafka_eps(big)]

    kafka_produce = [float(small["kafka"]["produce_seconds"]), float(big["kafka"]["produce_seconds"])]
    kafka_consume_write = [
        float(small["kafka"]["consume_write_seconds"]),
        float(big["kafka"]["consume_write_seconds"]),
    ]

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle("Sync vs Kafka: Small and Big Batch Comparison", fontsize=14, fontweight="bold")

    width = 0.35
    x = [0, 1]

    ax0 = axes[0]
    bars_sync_time = ax0.bar([i - width / 2 for i in x], sync_time, width, label="Sync", color="#2a9d8f")
    bars_kafka_time = ax0.bar([i + width / 2 for i in x], kafka_time, width, label="Kafka", color="#e76f51")
    ax0.set_xticks(x, labels)
    ax0.set_ylabel("Seconds")
    ax0.set_title("End-to-End Time")
    ax0.legend()
    add_value_labels(ax0, bars_sync_time)
    add_value_labels(ax0, bars_kafka_time)

    ax1 = axes[1]
    bars_sync_eps = ax1.bar([i - width / 2 for i in x], sync_eps, width, label="Sync", color="#264653")
    bars_kafka_eps = ax1.bar([i + width / 2 for i in x], kafka_eps, width, label="Kafka", color="#f4a261")
    ax1.set_xticks(x, labels)
    ax1.set_ylabel("Events / second")
    ax1.set_title("Throughput")
    ax1.legend()
    add_value_labels(ax1, bars_sync_eps)
    add_value_labels(ax1, bars_kafka_eps)

    ax2 = axes[2]
    bars_prod = ax2.bar(x, kafka_produce, width=0.5, label="Kafka produce", color="#577590")
    bars_cons = ax2.bar(
        x,
        kafka_consume_write,
        width=0.5,
        bottom=kafka_produce,
        label="Kafka consume+write",
        color="#90be6d",
    )
    ax2.set_xticks(x, labels)
    ax2.set_ylabel("Seconds")
    ax2.set_title("Kafka Time Breakdown")
    ax2.legend()
    add_value_labels(ax2, bars_prod)
    add_value_labels(ax2, bars_cons)

    plt.tight_layout()
    fig.savefig(output_png, dpi=150)
    print(f"[INFO] Plot saved to {output_png}")


def validate_result(name: str, result: dict):
    required_paths = [
        ("sync", "write_seconds"),
        ("sync", "events_per_second"),
        ("kafka", "end_to_end_seconds"),
        ("kafka", "events_per_second"),
        ("kafka", "produce_seconds"),
        ("kafka", "consume_write_seconds"),
    ]

    for section, key in required_paths:
        if section not in result or key not in result[section]:
            raise ValueError(f"{name} is missing key: {section}.{key}")


def parse_args():
    parser = argparse.ArgumentParser(description="Plot sync vs Kafka for small and big batch runs.")
    parser.add_argument("--small-json", required=True, help="JSON result file for small batch")
    parser.add_argument("--big-json", required=True, help="JSON result file for big batch")
    parser.add_argument("--output-png", default="kafka_vs_sync_small_big.png")
    return parser.parse_args()


def main():
    args = parse_args()
    small = load_result(Path(args.small_json))
    big = load_result(Path(args.big_json))
    validate_result("small-json", small)
    validate_result("big-json", big)

    build_plot(small, big, Path(args.output_png))


if __name__ == "__main__":
    main()
