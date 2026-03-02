import os
import sys
import json
import time
import argparse
import multiprocessing as mp

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ConnectionFailure, NetworkTimeout, ServerSelectionTimeoutError
from pymongo.write_concern import WriteConcern


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_DATA_PATH = os.path.join(REPO_ROOT, "data", "yellow_tripdata_2025-05.parquet")
DB_NAME = "mysimbdp"
COLLECTION_NAME = "yellow_trips"


def detect_time_col(df, kind):
    for col in df.columns:
        name = col.lower()
        if kind in name and ("time" in name or "date" in name or "datetime" in name):
            return col
    return None


def to_datetime(series):
    if pd.api.types.is_datetime64_any_dtype(series):
        return series
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_datetime(series, unit="ms", errors="coerce", utc=True)
    return pd.to_datetime(series, errors="coerce", utc=True)


def to_ms(series):
    ms = (series.astype("int64") // 1_000_000)
    ms = ms.where(series.notna(), None)
    return ms


def clean_df(df):
    pickup_col = detect_time_col(df, "pickup")
    dropoff_col = detect_time_col(df, "dropoff")

    if pickup_col:
        pickup_dt = to_datetime(df[pickup_col])
        df["pickup_datetime"] = pickup_dt
        df["pickup_datetime_ms"] = to_ms(pickup_dt)
    if dropoff_col:
        dropoff_dt = to_datetime(df[dropoff_col])
        df["dropoff_datetime"] = dropoff_dt
        df["dropoff_datetime_ms"] = to_ms(dropoff_dt)

    if "pickup_datetime" in df.columns:
        df["pickup_day"] = df["pickup_datetime"].dt.strftime("%Y-%m-%d")
        df["pickup_month"] = df["pickup_datetime"].dt.strftime("%Y-%m")
        df["pickup_day"] = df["pickup_day"].where(df["pickup_datetime"].notna(), None)
        df["pickup_month"] = df["pickup_month"].where(df["pickup_datetime"].notna(), None)

    numeric_cols = [
        "VendorID",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "Airport_fee",
        "ehail_fee",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.where(pd.notnull(df), None)
    return df


def connect_collection(uri, write_concern=None):
    mongo_client = MongoClient(
        uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=30000,
        retryWrites=True,
    )
    mongo_client.server_info()
    db = mongo_client[DB_NAME]
    if write_concern is not None:
        collection = db.get_collection(COLLECTION_NAME, write_concern=write_concern)
    else:
        collection = db[COLLECTION_NAME]
    return mongo_client, collection


def clear_collection(uri, write_concern, max_retries=6):
    for attempt in range(max_retries):
        try:
            _, collection = connect_collection(uri, write_concern=write_concern)
            collection.delete_many({})
            return
        except (ServerSelectionTimeoutError, AutoReconnect, NetworkTimeout, ConnectionFailure) as e:
            print(f"Clear failed ({type(e).__name__}), retry {attempt + 1}/{max_retries} ...")
            time.sleep(2)
    raise RuntimeError("clear collection failed after retries")


def percentile(values, p):
    if not values:
        return None
    values_sorted = sorted(values)
    idx = int((p / 100.0) * (len(values_sorted) - 1))
    return values_sorted[idx]


def worker_main(worker_id, cfg, result_queue):
    docs_inserted = 0
    retry_count = 0
    error_count = 0
    batch_times_ms = []

    try:
        df = pd.read_parquet(cfg["data_path"])
        if cfg["limit"] is not None:
            df = df.head(cfg["limit"])
        df = clean_df(df)

        write_concern = WriteConcern(
            w=cfg["w_value"],
            j=cfg["journal"],
            wtimeout=cfg["wtimeout_ms"],
        )
        client, collection = connect_collection(cfg["mongo_uri"], write_concern=write_concern)

        start_time = time.perf_counter()
        for start in range(0, len(df), cfg["batch_size"]):
            end = start + cfg["batch_size"]
            batch = df.iloc[start:end].to_dict(orient="records")
            if not batch:
                continue
            max_retries = 6
            inserted_ok = False
            for attempt in range(max_retries):
                t0 = time.perf_counter()
                try:
                    collection.insert_many(batch, ordered=False)
                    t1 = time.perf_counter()
                    batch_times_ms.append((t1 - t0) * 1000.0)
                    inserted_ok = True
                    break
                except (ServerSelectionTimeoutError, AutoReconnect, NetworkTimeout, ConnectionFailure) as e:
                    retry_count += 1
                    error_count += 1
                    print(f"[worker {worker_id}] Batch insert failed ({type(e).__name__}), retry {attempt + 1}/{max_retries} ...")
                    time.sleep(2)
                    try:
                        client, collection = connect_collection(cfg["mongo_uri"], write_concern=write_concern)
                    except Exception as reconnect_error:
                        error_count += 1
                        print(f"[worker {worker_id}] Reconnect failed ({type(reconnect_error).__name__})")
            if not inserted_ok:
                raise RuntimeError(f"batch insert failed after retries (rows {start}-{end})")

            docs_inserted += len(batch)
            # rate limiting logic
            if cfg["rate_docs_per_sec"] is not None and cfg["rate_docs_per_sec"] > 0:
                expected = len(batch) / cfg["rate_docs_per_sec"]
                actual = batch_times_ms[-1] / 1000.0
                if actual < expected:
                    time.sleep(expected - actual)

        total_seconds = time.perf_counter() - start_time
        avg_batch_ms = sum(batch_times_ms) / len(batch_times_ms) if batch_times_ms else None
        p95_batch_ms = percentile(batch_times_ms, 95)

        result_queue.put(
            {
                "worker_id": worker_id,
                "ok": True,
                "docs": docs_inserted,
                "seconds": total_seconds,
                "avg_batch_ms": avg_batch_ms,
                "p95_batch_ms": p95_batch_ms,
                "retries": retry_count,
                "errors": error_count,
            }
        )
    except Exception as e:
        result_queue.put(
            {
                "worker_id": worker_id,
                "ok": False,
                "docs": docs_inserted,
                "seconds": None,
                "avg_batch_ms": None,
                "p95_batch_ms": None,
                "retries": retry_count,
                "errors": error_count + 1,
                "error": str(e),
            }
        )


def parse_concurrency(value):
    parts = [p.strip() for p in value.split(",") if p.strip()]
    if not parts:
        return []
    return [int(p) for p in parts]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo_uri", default=None, help="Mongo URI (default: env MONGO_URI or localhost:27018)")
    parser.add_argument("--data", default=DEFAULT_DATA_PATH, help="path to input parquet")
    parser.add_argument("--concurrency", default="1,5,10", help="comma list of worker counts")
    parser.add_argument("--limit", type=int, default=None, help="limit rows per worker")
    parser.add_argument("--batch_size", type=int, default=1000, help="batch size (default: 1000)")
    parser.add_argument("--rate_docs_per_sec", type=float, default=None, help="throttle per worker (docs/sec)")
    parser.add_argument("--write_concern", choices=["w1", "majority"], default="w1")
    parser.add_argument("--journal", choices=["true", "false"], default="true")
    parser.add_argument("--wtimeout_ms", type=int, default=10000)
    parser.add_argument("--drop_before", choices=["true", "false"], default="false")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--out", default=None, help="output JSON path (default: logs/bench_producers_<ts>.json)")
    args = parser.parse_args()

    mongo_uri = args.mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27018")
    data_path = os.path.abspath(args.data)
    if not os.path.exists(data_path):
        print(f"File not found: {data_path}")
        sys.exit(1)

    conc_list = parse_concurrency(args.concurrency)
    if not conc_list:
        print("No concurrency values provided.")
        sys.exit(1)

    journal = args.journal == "true"
    w_value = 1 if args.write_concern == "w1" else "majority"

    logs_dir = os.path.join(REPO_ROOT, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    if args.out:
        out_path = os.path.abspath(args.out)
    else:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(logs_dir, f"bench_producers_{timestamp}.json")

    results = []

    for conc in conc_list:
        for run_idx in range(1, args.runs + 1):
            if args.drop_before == "true":
                wc = WriteConcern(w=w_value, j=journal, wtimeout=args.wtimeout_ms)
                print(f"Clearing collection before run (concurrency={conc}, run={run_idx}) ...")
                clear_collection(mongo_uri, wc)

            cfg = {
                "mongo_uri": mongo_uri,
                "data_path": data_path,
                "limit": args.limit,
                "batch_size": args.batch_size,
                "rate_docs_per_sec": args.rate_docs_per_sec,
                "w_value": w_value,
                "journal": journal,
                "wtimeout_ms": args.wtimeout_ms,
            }

            result_queue = mp.Queue()
            procs = []
            wall_start = time.perf_counter()
            # spawn workers processes
            for i in range(conc):
                p = mp.Process(target=worker_main, args=(i, cfg, result_queue))
                p.start()
                procs.append(p)

            worker_results = []
            for _ in range(conc):
                worker_results.append(result_queue.get())

            for p in procs:
                p.join()

            wall_seconds = time.perf_counter() - wall_start
            total_docs = sum(r["docs"] for r in worker_results)
            total_retries = sum(r["retries"] for r in worker_results)
            total_errors = sum(r["errors"] for r in worker_results)
            ok = all(r["ok"] for r in worker_results)

            worker_throughputs = []
            avg_batches = []
            p95_batches = []
            for r in worker_results:
                if r["seconds"]:
                    worker_throughputs.append(r["docs"] / r["seconds"])
                if r["avg_batch_ms"] is not None:
                    avg_batches.append(r["avg_batch_ms"])
                if r["p95_batch_ms"] is not None:
                    p95_batches.append(r["p95_batch_ms"])

            avg_worker_tp = sum(worker_throughputs) / len(worker_throughputs) if worker_throughputs else None
            avg_batch_ms = sum(avg_batches) / len(avg_batches) if avg_batches else None
            p95_batch_ms = sum(p95_batches) / len(p95_batches) if p95_batches else None

            result = {
                "concurrency": conc,
                "run": run_idx,
                "docs": total_docs,
                "wall_seconds": round(wall_seconds, 3),
                "throughput_docs_per_sec": round(total_docs / wall_seconds, 3) if wall_seconds > 0 else 0.0,
                "avg_worker_throughput": round(avg_worker_tp, 3) if avg_worker_tp is not None else None,
                "avg_batch_ms": round(avg_batch_ms, 3) if avg_batch_ms is not None else None,
                "p95_batch_ms": round(p95_batch_ms, 3) if p95_batch_ms is not None else None,
                "retries": total_retries,
                "errors": total_errors,
                "worker_ok": ok,
            }
            results.append(result)

            print(
                f"run {run_idx} conc={conc} docs={result['docs']} wall_s={result['wall_seconds']} "
                f"docs/s={result['throughput_docs_per_sec']}"
            )

    payload = {
        "mongo_uri": mongo_uri,
        "data": data_path,
        "write_concern": args.write_concern,
        "journal": journal,
        "wtimeout_ms": args.wtimeout_ms,
        "rate_docs_per_sec": args.rate_docs_per_sec,
        "batch_size": args.batch_size,
        "limit": args.limit,
        "runs": args.runs,
        "results": results,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print("")
    print("Summary:")
    print("concurrency | docs | wall_s | docs/s | avg_batch_ms | p95_batch_ms | retries | errors")
    for r in results:
        print(
            f"{r['concurrency']:>11} | {r['docs']:>4} | {r['wall_seconds']:>6} | "
            f"{r['throughput_docs_per_sec']:>6} | {str(r['avg_batch_ms']):>12} | "
            f"{str(r['p95_batch_ms']):>12} | {r['retries']:>7} | {r['errors']:>6}"
        )

    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
