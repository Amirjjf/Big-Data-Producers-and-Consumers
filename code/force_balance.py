import os
import time
import argparse

from pymongo import MongoClient
from pymongo.errors import PyMongoError


DB_NAME = "mysimbdp"
COLLECTION_NAME = "yellow_trips"


def connect(uri):
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=30000,
    )
    client.admin.command("ping")
    return client


def chunk_count(config_db, ns):
    coll_doc = config_db.collections.find_one({"_id": ns})
    if not coll_doc or "uuid" not in coll_doc:
        return 0
    return config_db.chunks.count_documents({"uuid": coll_doc["uuid"]})


def chunk_distribution(config_db, ns):
    coll_doc = config_db.collections.find_one({"_id": ns})
    if not coll_doc or "uuid" not in coll_doc:
        return None
    uuid = coll_doc["uuid"]
    pipeline = [
        {"$match": {"uuid": uuid}},
        {"$group": {"_id": "$shard", "count": {"$sum": 1}}},
    ]
    dist = {}
    for doc in config_db.chunks.aggregate(pipeline):
        dist[doc["_id"]] = doc["count"]
    return dist


def shard_dist(db):
    try:
        stats = db.command("collStats", COLLECTION_NAME)
        return stats.get("shards")
    except PyMongoError:
        return None


def print_summary(label, client):
    ns = f"{DB_NAME}.{COLLECTION_NAME}"
    config_db = client["config"]
    app_db = client[DB_NAME]

    print(f"\n{label}")
    print(f"namespace: {ns}")
    try:
        chunks = chunk_count(config_db, ns)
        print(f"chunks: {chunks}")
    except PyMongoError as e:
        print(f"chunks: error ({e})")

    try:
        dist = chunk_distribution(config_db, ns)
        if dist:
            print("chunk distribution:")
            for shard_name, count in dist.items():
                print(f"  {shard_name}: {count} chunks")
        else:
            print("chunk distribution: unavailable (no uuid or no chunks)")
    except PyMongoError as e:
        print(f"chunk distribution: error ({e})")

    shards = shard_dist(app_db)
    if shards:
        print("collStats shard counts (docs):")
        for shard_name, shard_info in shards.items():
            print(f"  {shard_name}: {shard_info.get('count', 0)} docs")
    else:
        print("collStats shard counts: unavailable (collStats failed)")


def enable_balancer(client):
    try:
        client.admin.command("balancerStart")
        return True
    except PyMongoError as e:
        print(f"balancerStart: {e}")
        return False


def set_chunk_size(client, chunk_mb):
    config_db = client["config"]
    config_db.settings.update_one(
        {"_id": "chunksize"},
        {"$set": {"value": int(chunk_mb)}},
        upsert=True,
    )


def has_distribution(client):
    ns = f"{DB_NAME}.{COLLECTION_NAME}"
    config_db = client["config"]
    app_db = client[DB_NAME]

    chunks = chunk_count(config_db, ns)
    shards = shard_dist(app_db)
    if not shards:
        return False, chunks, 0
    shards_with_docs = sum(1 for s in shards.values() if s.get("count", 0) > 0)
    return chunks > 1 and shards_with_docs > 1, chunks, shards_with_docs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk_mb", type=int, default=64, help="chunk size in MB (default: 64)")
    parser.add_argument("--wait_seconds", type=int, default=120, help="max wait time (default: 120)")
    parser.add_argument("--poll_every", type=int, default=5, help="poll interval seconds (default: 5)")
    parser.add_argument(
        "--recreate_with_initial_chunks",
        choices=["true", "false"],
        default="false",
        help="drop + reshard collection with numInitialChunks (default: false)",
    )
    parser.add_argument("--num_initial_chunks", type=int, default=8, help="numInitialChunks for reshard (default: 8)")
    args = parser.parse_args()

    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27018")

    try:
        client = connect(mongo_uri)
    except Exception as e:
        print(f"MongoDB not reachable: {e}")
        return

    print_summary("Before", client)

    if args.recreate_with_initial_chunks == "true":
        print("\nWARNING: This will DELETE data in mysimbdp.yellow_trips")
        try:
            app_db = client[DB_NAME]
            app_db[COLLECTION_NAME].drop()
            cmd = {
                "shardCollection": f"{DB_NAME}.{COLLECTION_NAME}",
                "key": {"pickup_day": "hashed"},
                "numInitialChunks": int(args.num_initial_chunks),
            }
            client.admin.command(cmd)
            print_summary("After reshard (initial chunks)", client)
        except PyMongoError as e:
            print(f"Reshard failed: {e}")
            return
        return

    try:
        set_chunk_size(client, args.chunk_mb)
        print(f"\nSet chunk size to {args.chunk_mb} MB")
    except PyMongoError as e:
        print(f"Failed to set chunk size: {e}")
        return

    enable_balancer(client)

    print(f"\nWaiting up to {args.wait_seconds}s for distribution...")
    start = time.time()
    distributed = False
    last_chunks = None
    last_shards = None
    while time.time() - start < args.wait_seconds:
        ok, chunks, shards_with_docs = has_distribution(client)
        last_chunks = chunks
        last_shards = shards_with_docs
        if ok:
            distributed = True
            break
        time.sleep(args.poll_every)

    print_summary("After", client)

    if distributed:
        print("\nResult: distribution improved (more than 1 chunk and docs on >1 shard).")
    else:
        print(
            f"\nResult: still not distributed after {args.wait_seconds}s "
            f"(chunks={last_chunks}, shards_with_docs={last_shards})."
        )


if __name__ == "__main__":
    main()
