import os
import sys
import json
import time
import random
import argparse
import subprocess
import multiprocessing as mp

from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ConnectionFailure, NetworkTimeout, ServerSelectionTimeoutError, PyMongoError


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_NAME = "mysimbdp"
COLLECTION_NAME = "yellow_trips"


def connect(uri, per_query_timeout_ms):
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=per_query_timeout_ms,
    )
    client.admin.command("ping")
    return client


def percentile(values, p):
    if not values:
        return None
    values_sorted = sorted(values)
    idx = int((p / 100.0) * (len(values_sorted) - 1))
    return values_sorted[idx]


def get_days(client, match_sample_days):
    coll = client[DB_NAME][COLLECTION_NAME]
    try:
        days = coll.distinct("pickup_day")
        days = [d for d in days if d]
        if match_sample_days is not None and len(days) > match_sample_days:
            days = random.sample(days, match_sample_days)
        return days
    except PyMongoError:
        return []


def run_query(collection, query_type, day, find_limit):
    if query_type == "count":
        collection.count_documents({"pickup_day": day})
    elif query_type == "aggregate":
        pipeline = [
            {"$match": {"pickup_day": day}},
            {"$group": {"_id": "$payment_type", "avg_total": {"$avg": "$total_amount"}}},
            {"$limit": 10},
        ]
        list(collection.aggregate(pipeline))
    elif query_type == "find":
        list(collection.find({"pickup_day": day}).limit(find_limit))
    else:
        choice = random.choice(["count", "aggregate", "find"])
        run_query(collection, choice, day, find_limit)


def worker_main(worker_id, cfg, result_queue):
    latencies_ms = []
    errors = 0
    queries = 0
    queries_ok = 0
    error_types = {}

    try:
        client = connect(cfg["mongo_uri"], cfg["per_query_timeout_ms"])
        collection = client[DB_NAME][COLLECTION_NAME]
    except Exception as e:
        result_queue.put(
            {
                "worker_id": worker_id,
                "ok": False,
                "queries": 0,
                "queries_ok": 0,
                "errors": 1,
                "latencies_ms": [],
                "error_types": {type(e).__name__: 1},
                "error": str(e),
            }
        )
        return

    if not cfg["days"]:
        result_queue.put(
            {
                "worker_id": worker_id,
                "ok": False,
                "queries": 0,
                "queries_ok": 0,
                "errors": 1,
                "latencies_ms": [],
                "error_types": {"NoDays": 1},
                "error": "no pickup_day values",
            }
        )
        return

    end_time = time.perf_counter() + cfg["duration"]
    while time.perf_counter() < end_time:
        day = random.choice(cfg["days"])
        t0 = time.perf_counter()
        try:
            run_query(collection, cfg["query_type"], day, cfg["find_limit"])
            t1 = time.perf_counter()
            latencies_ms.append((t1 - t0) * 1000.0)
            queries += 1
            queries_ok += 1
        except (ServerSelectionTimeoutError, AutoReconnect, NetworkTimeout, ConnectionFailure, PyMongoError) as e:
            errors += 1
            queries += 1
            name = type(e).__name__
            error_types[name] = error_types.get(name, 0) + 1

    result_queue.put(
        {
            "worker_id": worker_id,
            "ok": True,
            "queries": queries,
            "queries_ok": queries_ok,
            "errors": errors,
            "latencies_ms": latencies_ms,
            "error_types": error_types,
        }
    )


def parse_concurrency(value):
    parts = [p.strip() for p in value.split(",") if p.strip()]
    if not parts:
        return []
    return [int(p) for p in parts]

# This is for the “read while writes are happening” scenario.
def start_producers(mongo_uri):
    producers_path = os.path.join(REPO_ROOT, "code", "bench_producers.py")
    cmd = [
        sys.executable,
        producers_path,
        "--mongo_uri",
        mongo_uri,
        "--concurrency",
        "5",
        "--write_concern",
        "w1",
        "--runs",
        "1",
        "--limit",
        "50000",
    ]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def stop_producers(proc):
    if not proc:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo_uri", default=None, help="Mongo URI (default: env MONGO_URI or localhost:27018)")
    parser.add_argument("--concurrency", default="1,5,10", help="comma list of worker counts")
    parser.add_argument("--duration", type=int, default=30, help="seconds per test (default: 30)")
    parser.add_argument("--per_query_timeout_ms", type=int, default=5000, help="socket timeout per query (default: 5000)")
    parser.add_argument("--max_extra_seconds", type=int, default=5, help="extra time allowance (default: 5)")
    parser.add_argument("--find_limit", type=int, default=50, help="limit for find() (default: 50)")
    parser.add_argument("--match_sample_days", type=int, default=50, help="sample pickup_day values (default: 50)")
    parser.add_argument(
        "--query_type",
        choices=["count", "aggregate", "find", "mixed"],
        default="mixed",
        help="query type (default: mixed)",
    )
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--with_producers", choices=["true", "false"], default="false")
    args = parser.parse_args()

    mongo_uri = args.mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27018")

    try:
        client = connect(mongo_uri, args.per_query_timeout_ms)
    except Exception as e:
        print(f"MongoDB not reachable: {e}")
        sys.exit(1)

    days = get_days(client, args.match_sample_days)
    if not days:
        print("No pickup_day values found. Ingest data first.")
        sys.exit(1)

    conc_list = parse_concurrency(args.concurrency)
    if not conc_list:
        print("No concurrency values provided.")
        sys.exit(1)

    logs_dir = os.path.join(REPO_ROOT, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(logs_dir, f"bench_consumers_{timestamp}.json")

    results = []

    for conc in conc_list:
        for run_idx in range(1, args.runs + 1):
            producers_proc = None
            if args.with_producers == "true":
                producers_proc = start_producers(mongo_uri)

            cfg = {
                "mongo_uri": mongo_uri,
                "duration": args.duration,
                "query_type": args.query_type,
                "days": days,
                "per_query_timeout_ms": args.per_query_timeout_ms,
                "find_limit": args.find_limit,
            }

            result_queue = mp.Queue()
            procs = []
            wall_start = time.perf_counter()
            for i in range(conc):
                p = mp.Process(target=worker_main, args=(i, cfg, result_queue))
                p.start()
                procs.append(p)

            worker_results = []
            timeout_s = args.duration + args.max_extra_seconds + 10
            for _ in range(conc):
                try:
                    worker_results.append(result_queue.get(timeout=timeout_s))
                except Exception:
                    worker_results.append(
                        {
                            "worker_id": None,
                            "ok": False,
                            "queries": 0,
                            "queries_ok": 0,
                            "errors": 1,
                            "latencies_ms": [],
                            "error_types": {"ResultTimeout": 1},
                        }
                    )

            for p in procs:
                p.join(timeout=5)
                if p.is_alive():
                    p.terminate()

            wall_seconds = time.perf_counter() - wall_start

            stop_producers(producers_proc)

            total_queries = sum(r["queries"] for r in worker_results)
            total_queries_ok = sum(r.get("queries_ok", 0) for r in worker_results)
            total_errors = sum(r["errors"] for r in worker_results)
            ok = all(r["ok"] for r in worker_results)
            all_latencies = []
            error_types = {}
            for r in worker_results:
                all_latencies.extend(r["latencies_ms"])
                for k, v in r.get("error_types", {}).items():
                    error_types[k] = error_types.get(k, 0) + v

            avg_latency = sum(all_latencies) / len(all_latencies) if all_latencies else None
            p95_latency = percentile(all_latencies, 95)
            qps = total_queries / wall_seconds if wall_seconds > 0 else 0.0

            result = {
                "concurrency": conc,
                "run": run_idx,
                "queries": total_queries,
                "queries_ok": total_queries_ok,
                "wall_seconds": round(wall_seconds, 3),
                "queries_per_sec": round(qps, 3),
                "avg_latency_ms": round(avg_latency, 3) if avg_latency is not None else None,
                "p95_latency_ms": round(p95_latency, 3) if p95_latency is not None else None,
                "errors": total_errors,
                "error_types": error_types,
                "worker_ok": ok,
            }
            results.append(result)

            print(
                f"run {run_idx} conc={conc} queries={result['queries']} wall_s={result['wall_seconds']} "
                f"qps={result['queries_per_sec']}"
            )

    payload = {
        "mongo_uri": mongo_uri,
        "query_type": args.query_type,
        "duration": args.duration,
        "per_query_timeout_ms": args.per_query_timeout_ms,
        "find_limit": args.find_limit,
        "match_sample_days": args.match_sample_days,
        "runs": args.runs,
        "with_producers": args.with_producers == "true",
        "results": results,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print("")
    print("Summary:")
    print("concurrency | queries | wall_s | qps | avg_ms | p95_ms | errors")
    for r in results:
        print(
            f"{r['concurrency']:>11} | {r['queries']:>7} | {r['wall_seconds']:>6} | "
            f"{r['queries_per_sec']:>5} | {str(r['avg_latency_ms']):>6} | "
            f"{str(r['p95_latency_ms']):>6} | {r['errors']:>6}"
        )

    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
