import os

from pymongo import MongoClient


DB_NAME = "mysimbdp"
COLLECTION_NAME = "yellow_trips"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018")


def main():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    print(f"Mongo URI: {MONGO_URI}")

    try:
        shards = client.admin.command("listShards").get("shards", [])
        print("\nShards:")
        for s in shards:
            print(s)
    except Exception as e:
        print(f"\nlistShards not available: {e}")

    try:
        print("\nChunk distribution:")
        pipeline = [
            {"$match": {"ns": f"{DB_NAME}.{COLLECTION_NAME}"}},
            {"$group": {"_id": "$shard", "chunks": {"$sum": 1}}},
            {"$sort": {"chunks": -1}},
        ]
        for row in client["config"]["chunks"].aggregate(pipeline):
            print(row)
    except Exception as e:
        print(f"Chunk distribution not available: {e}")

    try:
        stats = db.command("collStats", COLLECTION_NAME)
        print("\nCollection stats (short):")
        print(
            {
                "sharded": stats.get("sharded"),
                "count": stats.get("count"),
                "size": stats.get("size"),
            }
        )
        if "shards" in stats:
            print("Shard sizes:")
            print(stats["shards"])
    except Exception as e:
        print(f"collStats not available: {e}")

    sample = collection.find_one({"pickup_day": {"$ne": None}}, {"pickup_day": 1})
    if sample and "pickup_day" in sample:
        day = sample["pickup_day"]
        count = collection.count_documents({"pickup_day": day})
        print(f"\nExample query: pickup_day={day} -> count={count}")
    else:
        print("\nNo pickup_day found for example query.")


if __name__ == "__main__":
    main()
