"""
Benchmark Pipeline Latency & Throughput:
Measures empirical end-to-end ingestion and commit latency:
- Transaction creation time (t0)
- Bronze append time (t1)
- Silver ACID MERGE commit time (t2)

Calculates p50, p95, p99, min, max, mean, and throughput (events/sec).
Provides empirical evidence rather than unsubstantiated claims.
"""

import os
import sys
import time
import json
import shutil
import numpy as np
import pandas as pd
from typing import List, Dict, Any

# Ensure project root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipeline.delta_engine import DeltaLakehouseEngine
from pipeline.consumer import CDCPipelineConsumer


def generate_benchmark_batch(n_events: int = 200) -> List[Dict[str, Any]]:
    """Generates synthetic high-fidelity CDC envelopes for benchmarking."""
    events = []
    base_ts = int(time.time() * 1000)

    for i in range(1, n_events + 1):
        order_id = f"BENCH-ORD-{i:05d}"
        ev = {
            "schema": {"type": "struct", "name": "erp_cdc.public.orders.Envelope"},
            "payload": {
                "op": "c",
                "ts_ms": base_ts + i,
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "postgresql",
                    "name": "erp_cdc",
                    "ts_ms": base_ts + i,
                    "db": "erp_database",
                    "schema": "public",
                    "table": "orders",
                    "txId": 5000 + i,
                    "lsn": 100000 + (i * 10)
                },
                "before": None,
                "after": {
                    "order_id": order_id,
                    "customer_id": f"CUST-BENCH-{(i % 10):02d}",
                    "product_id": "SKU-TIRE-001",
                    "quantity": 10 + (i % 50),
                    "total_price": round(100.0 + (i * 1.5), 2),
                    "status": "CONFIRMED",
                    "created_at": "2026-09-20T10:00:00Z",
                    "updated_at": "2026-09-20T10:00:00Z"
                }
            }
        }
        events.append(ev)
    return events


def run_latency_benchmark(n_events: int = 200, storage_dir: str = "./benchmark_storage") -> Dict[str, Any]:
    """Runs the benchmark and collects high-precision timing per event."""
    if os.path.exists(storage_dir):
        shutil.rmtree(storage_dir, ignore_errors=True)

    engine = DeltaLakehouseEngine(base_storage_dir=storage_dir)
    consumer = CDCPipelineConsumer(lakehouse_engine=engine)
    events = generate_benchmark_batch(n_events=n_events)

    latencies_ms = []

    print(f"======================================================================")
    print(f"       STARTING EMPIRICAL PIPELINE LATENCY BENCHMARK ({n_events} EVENTS)        ")
    print(f"======================================================================")

    total_start = time.perf_counter()

    for idx, ev in enumerate(events, 1):
        raw_data = json.dumps(ev)
        t_start = time.perf_counter()
        
        # Process event through contract validation -> Bronze Delta -> Silver Delta MERGE
        res = consumer.process_raw_message(
            raw_data=raw_data,
            topic_hint="erp_cdc.public.orders",
            partition=0,
            offset=idx
        )
        t_end = time.perf_counter()

        if res["status"] != "success":
            print(f"[!] Event {idx} failed or quarantined: {res}")
            continue

        latency_ms = (t_end - t_start) * 1000.0
        latencies_ms.append(latency_ms)

    total_elapsed_sec = time.perf_counter() - total_start

    # Clean up benchmark storage
    if os.path.exists(storage_dir):
        shutil.rmtree(storage_dir, ignore_errors=True)

    latencies_arr = np.array(latencies_ms)
    p50 = float(np.percentile(latencies_arr, 50))
    p95 = float(np.percentile(latencies_arr, 95))
    p99 = float(np.percentile(latencies_arr, 99))
    max_lat = float(np.max(latencies_arr))
    min_lat = float(np.min(latencies_arr))
    mean_lat = float(np.mean(latencies_arr))
    throughput = len(latencies_ms) / total_elapsed_sec if total_elapsed_sec > 0 else 0

    results = {
        "events_processed": len(latencies_ms),
        "total_time_sec": round(total_elapsed_sec, 3),
        "throughput_eps": round(throughput, 1),
        "min_ms": round(min_lat, 2),
        "mean_ms": round(mean_lat, 2),
        "p50_ms": round(p50, 2),
        "p95_ms": round(p95, 2),
        "p99_ms": round(p99, 2),
        "max_ms": round(max_lat, 2)
    }

    print("\n--- EMPIRICAL BENCHMARK RESULTS ---")
    print(f"  Events Processed:    {results['events_processed']}")
    print(f"  Total Ingestion Time:{results['total_time_sec']}s")
    print(f"  Throughput:          {results['throughput_eps']} events/sec")
    print(f"  Latency p50:         {results['p50_ms']} ms")
    print(f"  Latency p95:         {results['p95_ms']} ms")
    print(f"  Latency p99:         {results['p99_ms']} ms")
    print(f"  Latency Min:         {results['min_ms']} ms")
    print(f"  Latency Max:         {results['max_ms']} ms")
    print(f"  Latency Mean:        {results['mean_ms']} ms")
    print("======================================================================\n")

    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Benchmark CDC ingestion latency")
    parser.add_argument("count", nargs="?", type=int, default=100, help="Number of events to benchmark")
    parser.add_argument("--events", "-n", type=int, dest="events_flag", default=None, help="Number of events to benchmark")
    args = parser.parse_args()
    n_events = args.events_flag if args.events_flag is not None else args.count
    run_latency_benchmark(n_events=n_events)
