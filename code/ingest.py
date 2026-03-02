import os
import sys
import argparse
import time
import json

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ConnectionFailure, NetworkTimeout, ServerSelectionTimeoutError
from pymongo.write_concern import WriteConcern


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_DATA_PATH = os.path.join(REPO_ROOT, "data", "yellow_tripdata_2025-05.parquet")
DB_NAME = "mysimbdp"
COLLECTION_NAME = "yellow_trips"
BATCH_SIZE = 1000
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018")


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

# Create a numeric “milliseconds timestamp” column
def to_ms(series):
    ms = (series.astype("int64") // 1_000_000)
    ms = ms.where(series.notna(), None)
    return ms


def connect_collection(
    uri,
    write_concern=None,
    server_selection_timeout_ms=30000,
    connect_timeout_ms=30000,
    socket_timeout_ms=30000,
):
    mongo_client = MongoClient(
        uri,
        serverSelectionTimeoutMS=server_selection_timeout_ms,
        connectTimeoutMS=connect_timeout_ms,
        socketTimeoutMS=socket_timeout_ms,
        retryWrites=True,
    )
    mongo_client.server_info()
    db = mongo_client[DB_NAME]
    if write_concern is not None:
        collection = db.get_collection(COLLECTION_NAME, write_concern=write_concern)
    else:
        collection = db[COLLECTION_NAME]
    return mongo_client, collection


def safe_drop_or_clear(uri, mode):
    max_retries = 6
    for attempt in range(max_retries):
        try:
            if mode == "hard_drop":
                print("Hard dropping collection...")
                _, collection = connect_collection(
                    uri,
                    server_selection_timeout_ms=60000,
                    connect_timeout_ms=60000,
                    socket_timeout_ms=180000,
                )
                collection.drop()
            else:
                print("Clearing collection (delete_many) ...")
                _, collection = connect_collection(uri)
                collection.delete_many({})
            return
        except (ServerSelectionTimeoutError, AutoReconnect, NetworkTimeout, ConnectionFailure) as e:
            print(f"{mode} failed ({type(e).__name__}), retry {attempt + 1}/{max_retries} ...")
            time.sleep(2)
    raise RuntimeError(f"{mode} failed after {max_retries} retries")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data",
        default=DEFAULT_DATA_PATH,
        help="path to input file (default: data/yellow_tripdata_2025-05.parquet)",
    )
    parser.add_argument("--drop", action="store_true", help="clear collection before insert (delete_many)")
    parser.add_argument("--hard-drop", action="store_true", help="real drop collection before insert")
    parser.add_argument(
        "--write_concern",
        choices=["w1", "majority"],
        default="w1",
        help="write concern mode: w1 or majority (default: w1)",
    )
    parser.add_argument(
        "--journal",
        choices=["true", "false"],
        default="true",
        help="journal for write concern (default: true)",
    )
    parser.add_argument("--wtimeout_ms", type=int, default=10000, help="write concern timeout in ms (default: 10000)")
    args = parser.parse_args()

    data_path = os.path.abspath(args.data)
    if not os.path.exists(data_path):
        print(f"File not found: {data_path}")
        sys.exit(1)

    journal = args.journal == "true"
    w_value = 1 if args.write_concern == "w1" else "majority"
    write_concern = WriteConcern(w=w_value, j=journal, wtimeout=args.wtimeout_ms)
    print(f"Write concern: w={w_value}, j={journal}, wtimeout_ms={args.wtimeout_ms}")

    try:
        df = pd.read_parquet(data_path)
    except Exception as e:
        print(f"Failed to read parquet: {e}")
        sys.exit(1)

    print(f"Rows read: {len(df)}")

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

    try:
        client, collection = connect_collection(MONGO_URI, write_concern=write_concern)
    except Exception as e:
        print(f"MongoDB not running or not reachable: {e}")
        sys.exit(1)

    if args.hard_drop:
        safe_drop_or_clear(MONGO_URI, "hard_drop")
        client, collection = connect_collection(MONGO_URI, write_concern=write_concern)
    elif args.drop:
        safe_drop_or_clear(MONGO_URI, "clear")
        client, collection = connect_collection(MONGO_URI, write_concern=write_concern)

    total_inserted = 0
    retry_count = 0
    error_count = 0
    ingest_start = time.perf_counter()
    for start in range(0, len(df), BATCH_SIZE):
        end = start + BATCH_SIZE
        batch = df.iloc[start:end].to_dict(orient="records")
        if batch:
            max_retries = 6
            inserted_ok = False
            for attempt in range(max_retries):
                try:
                    collection.insert_many(batch, ordered=False)
                    inserted_ok = True
                    break
                except (ServerSelectionTimeoutError, AutoReconnect, NetworkTimeout, ConnectionFailure) as e:
                    retry_count += 1
                    error_count += 1
                    print(f"Batch insert failed ({type(e).__name__}), retry {attempt + 1}/{max_retries} ...")
                    time.sleep(2)
                    try:
                        client, collection = connect_collection(MONGO_URI, write_concern=write_concern)
                    except Exception as reconnect_error:
                        error_count += 1
                        print(f"Reconnect failed ({type(reconnect_error).__name__})")
            if not inserted_ok:
                raise RuntimeError(f"Batch insert failed after {max_retries} retries (rows {start}-{end})")
            total_inserted += len(batch)
            print(f"Inserted: {total_inserted}")

    ingest_seconds = time.perf_counter() - ingest_start
    docs_per_sec = total_inserted / ingest_seconds if ingest_seconds > 0 else 0.0

    print("Creating indexes...")
    collection.create_index("pickup_day")
    collection.create_index([("PULocationID", 1), ("pickup_day", 1)])
    collection.create_index("total_amount")

    print(f"Ingest done. Docs: {total_inserted}")
    print(f"Time (s): {ingest_seconds:.2f}")
    print(f"Throughput (docs/s): {docs_per_sec:.2f}")

    logs_dir = os.path.join(REPO_ROOT, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(logs_dir, f"ingest_{timestamp}_{args.write_concern}.json")
    log_payload = {
        "dataset_path": data_path,
        "limit": None,
        "batch_size": BATCH_SIZE,
        "write_concern": args.write_concern,
        "journal": journal,
        "wtimeout_ms": args.wtimeout_ms,
        "docs_inserted": total_inserted,
        "seconds": round(ingest_seconds, 3),
        "docs_per_sec": round(docs_per_sec, 3),
        "retries_count": retry_count,
        "error_count": error_count,
    }
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log_payload, f, indent=2)

    print(f"Log saved: {log_path}")
    print("Done.")


if __name__ == "__main__":
    main()
