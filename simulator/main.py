#!/usr/bin/env python3
"""Entry point for the transaction simulator.

Runs continuously, generating transactions at a configurable rate:
1. Publishes raw transactions to Kafka
2. Uploads ground-truth labels to BigQuery with a 7-day delay
3. Persists state to allow restarts from checkpoint

Usage:
    python -m simulator.main
"""
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

from simulator.config import Config
from simulator.generator import TransactionGenerator
from simulator.producer import KafkaProducer
from simulator.label_uploader import LabelUploader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("simulator")

STATE_PATH = os.environ.get("SIM_STATE_PATH", "/tmp/simulator_state.json")
STATS_PATH = os.environ.get("SIM_STATS_PATH", "/tmp/simulator_stats.json")

_running = True


def _handle_signal(signum, frame):
    global _running
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    _running = False


def _load_stats() -> dict:
    if os.path.exists(STATS_PATH):
        with open(STATS_PATH, "r") as f:
            return json.load(f)
    return {
        "total_transactions": 0,
        "total_labels_uploaded": 0,
        "total_batches": 0,
        "start_time": None,
        "last_batch_time": None,
    }


def _save_stats(stats: dict):
    with open(STATS_PATH, "w") as f:
        json.dump(stats, f, indent=2)


def _calculate_tps(min_tps: int, max_tps: int, hour_of_day: int) -> int:
    """Simulate realistic TPS pattern: lower at night, higher during day."""
    import math
    # Sine wave peaking at 14:00, minimum at 02:00
    normalized = (math.sin((hour_of_day - 2) * math.pi / 12) + 1) / 2
    tps = min_tps + int(normalized * (max_tps - min_tps))
    return max(min_tps, min(max_tps, tps))


def run():
    global _running

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    config = Config()
    generator = TransactionGenerator(config)
    producer = KafkaProducer(config)
    uploader = LabelUploader(config)

    state = config.load_simulation_state(STATE_PATH)
    stats = _load_stats()
    stats["start_time"] = stats.get("start_time") or datetime.now(timezone.utc).isoformat()

    sim_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)
    current_day = state["current_day"]
    current_second = state["current_second"]
    tx_counter = state["transaction_counter"]

    logger.info(
        f"Starting simulator | day={current_day} | second={current_second} | "
        f"transactions={tx_counter}"
    )

    try:
        while _running:
            _batch_start = time.time()
            # Calculate current simulation time
            sim_time = sim_start + timedelta(days=current_day, seconds=current_second)

            # Determine TPS for this second
            hour_of_day = sim_time.hour
            tps = _calculate_tps(config.min_tps, config.max_tps, hour_of_day)

            # Generate batch for this second
            batch = generator.generate_batch(batch_size=tps, start_time=sim_time)

            # Publish to Kafka (strip fraud labels from payload)
            producer.produce_batch(batch)

            # Upload labels with delay
            try:
                labels_uploaded = uploader.upload_labels(batch, sim_time)
            except Exception as e:
                logger.error(f"Label upload failed: {e}")
                labels_uploaded = 0

            # Update counters
            tx_counter += len(batch)
            current_second += 1
            if current_second >= 86400:
                current_second = 0
                current_day += 1
                logger.info(f"Completed simulation day {current_day}")

            # Update stats
            stats["total_transactions"] += len(batch)
            stats["total_labels_uploaded"] += labels_uploaded
            stats["total_batches"] += 1
            stats["last_batch_time"] = datetime.now(timezone.utc).isoformat()

            # Persist state every 100 batches
            if stats["total_batches"] % 100 == 0:
                state["transaction_counter"] = tx_counter
                state["current_day"] = current_day
                state["current_second"] = current_second
                config.save_simulation_state(STATE_PATH, state)
                _save_stats(stats)

            # Sleep to maintain exactly 1 second of real time per simulated second
            # This ensures the configured TPS matches real throughput
            elapsed = time.time() - _batch_start
            sleep_time = max(0, 1.0 - elapsed)
            time.sleep(sleep_time)

            # Log progress every 1000 transactions
            if tx_counter % 10000 < len(batch):
                logger.info(
                    f"Progress: day={current_day} | tx={tx_counter} | "
                    f"tps={tps} | labels_uploaded={stats['total_labels_uploaded']}"
                )

    except Exception as e:
        logger.error(f"Simulator crashed: {e}", exc_info=True)
    finally:
        # Persist final state
        state["transaction_counter"] = tx_counter
        state["current_day"] = current_day
        state["current_second"] = current_second
        config.save_simulation_state(STATE_PATH, state)
        _save_stats(stats)
        producer.close()
        logger.info(
            f"Simulator stopped | total_transactions={stats['total_transactions']} | "
            f"total_labels={stats['total_labels_uploaded']}"
        )


if __name__ == "__main__":
    run()
