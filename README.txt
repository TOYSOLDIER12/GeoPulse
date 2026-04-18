GeoPulse
========

Overview
--------
GeoPulse ingests GDELT event data and writes relationship graphs into Neo4j.
It supports two processing modes:
1) Synchronous mode (direct fetch -> parse -> write to Neo4j)
2) Kafka mode (fetch/parse -> publish to Kafka -> consume -> write to Neo4j)

The repository also includes benchmarking scripts to compare latency and throughput between modes.

Architecture
------------
Data flow (sync):
GDELT API -> fetcher/gdelt_fetcher.py -> fetcher/gdelt_parser.py -> pipeline_common.py -> Neo4j

Data flow (kafka):
GDELT API -> fetcher/gdelt_fetcher.py -> fetcher/gdelt_parser.py -> fetcher/kafka_producer.py -> Kafka topic -> consumer/gdelt_to_neo4j.py -> pipeline_common.py -> Neo4j

Prerequisites
-------------
- Python 3.10+
- Docker + Docker Compose
- Network access to GDELT endpoint

Python dependencies are listed in requirements.txt:
- kafka-python
- neo4j
- requests
- pycountry
- matplotlib
- plus supporting libraries

Project Files and Roles
-----------------------
Root files:
- benchmark_experiment.py
  - Small-window benchmark harness for one timestamp.
  - Runs either sync mode or kafka mode and prints timing summaries.
- massive_batch_experiment.py
  - Large-window benchmark harness.
  - Builds a JSONL dataset across many 15-minute slices, runs sync and/or kafka, and writes structured metrics JSON.
- pipeline_common.py
  - Shared normalization and Neo4j write logic.
  - Contains Neo4jWriter and write_event_to_neo4j used by both sync and kafka flows.
- plot_kafka_sync_batches.py
  - Reads benchmark JSON outputs and creates comparison charts (time, throughput, kafka breakdown).
- docker-compose.yml
  - Defines zookeeper, kafka, neo4j, and kafka-init topic bootstrap service.
- requirements.txt
  - Pinned Python dependencies.
- README.tx
  - Existing quick notes (legacy/rough run instructions).
- README.txt
  - This detailed project guide.
- output.txt
  - Captured sample benchmark command output/log.
- small_batch_results.json
  - Example benchmark output for a small dataset window.
- big_batch_results.json
  - Example benchmark output for a larger dataset window.
- massive_batch_dataset.jsonl
  - Generated dataset file used by massive_batch_experiment.py.
- kafka_vs_sync_small_big.png
  - Example generated visualization from plot_kafka_sync_batches.py.

Folder consumer/:
- consumer/gdelt_to_neo4j.py
  - Kafka consumer that reads event messages and writes them to Neo4j.
  - Supports --max-events for controlled benchmark runs.

Folder fetcher/:
- fetcher/gdelt_fetcher.py
  - Builds GDELT URLs, downloads zipped export files, and exposes CSV extraction helper.
- fetcher/gdelt_parser.py
  - Parses GDELT TSV rows into GDELTEvent objects with basic filtering.
- fetcher/models.py
  - Dataclass schema for parsed GDELT events.
- fetcher/kafka_producer.py
  - Kafka producer wrapper that serializes event objects to JSON and publishes to topic gdelt-events.
- fetcher/main.py
  - Continuous polling producer loop for Kafka mode (every 15 minutes).
- fetcher/sync_main.py
  - Single-run synchronous pipeline (fetch one timestamp and write directly to Neo4j).
- fetcher/main_sync.py
  - Alternate sync runner with the same purpose as sync_main.py.
- fetcher/normalizer.py
  - Legacy normalization helpers and canonical actor mapping loader.
- fetcher/consumer_test.py
  - Simple local Kafka consumer script for manual topic inspection/testing.
- fetcher/canonical_actors.json
  - Lookup dictionary for canonical actor/region/org name normalization.

Setup
-----
1) Create and activate a virtual environment
   Linux/macOS:
   python3 -m venv venv
   source venv/bin/activate

2) Install Python packages
   pip install -r requirements.txt

3) Start infrastructure with Docker
   Sync-only (Neo4j only):
   docker compose up -d neo4j

   Kafka + Neo4j:
   docker compose up -d zookeeper kafka neo4j kafka-init

4) Verify services
   - Neo4j Browser: http://localhost:7474
   - Neo4j Bolt: bolt://localhost:7687
   - Neo4j credentials from compose: neo4j / password
   - Kafka broker: localhost:9092

How to Run
----------
A) Run synchronous pipeline (single timestamp)
- Command:
  python fetcher/sync_main.py --timestamp 2026-04-14T12:15

- Optional Neo4j args:
  --neo4j-uri bolt://localhost:7687
  --neo4j-user neo4j
  --neo4j-password password

B) Run benchmark harness in sync mode
- Command:
  python benchmark_experiment.py --mode sync --timestamp 2026-04-14T12:15

C) Run benchmark harness in kafka mode
- Recommended reset to avoid old messages:
  docker compose down -v
  docker compose up -d zookeeper kafka neo4j kafka-init

- Command:
  python benchmark_experiment.py --mode kafka --timestamp 2026-04-14T12:15

D) Run continuous Kafka producer loop
- Command:
  python fetcher/main.py

E) Run Kafka consumer manually
- Command:
  python consumer/gdelt_to_neo4j.py

Benchmarking and Plotting
-------------------------
1) Small batch example:
   python massive_batch_experiment.py --mode both --slices 16 --end-timestamp 2026-04-14T12:15 --output-json small_batch_results.json

2) Big batch example:
   python massive_batch_experiment.py --mode both --slices 192 --end-timestamp 2026-04-14T12:15 --output-json big_batch_results.json

3) Plot comparison:
   python plot_kafka_sync_batches.py --small-json small_batch_results.json --big-json big_batch_results.json --output-png kafka_vs_sync_small_big.png

Notes and Troubleshooting
-------------------------
- Timestamps should be UTC and effectively aligned to 15-minute slices.
- If Kafka benchmark appears to reprocess old data, reset Docker volumes before rerun.
- If Neo4j auth fails, confirm docker-compose credentials and that old containers were removed.
- If no GDELT file exists for a timestamp, scripts print warnings and exit/skip that slice.

Quick Start
-----------
For a minimal end-to-end comparison from a clean state:
1) python3 -m venv venv && source venv/bin/activate
2) pip install -r requirements.txt
3) docker compose down -v
4) docker compose up -d zookeeper kafka neo4j kafka-init
5) python benchmark_experiment.py --mode sync --timestamp 2026-04-14T12:15
6) python benchmark_experiment.py --mode kafka --timestamp 2026-04-14T12:15
