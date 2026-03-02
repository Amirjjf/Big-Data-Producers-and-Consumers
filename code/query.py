import os

from pymongo import MongoClient


DB_NAME = "mysimbdp"
COLLECTION_NAME = "yellow_trips"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018")


def main():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    collection = client[DB_NAME][COLLECTION_NAME]

    total = collection.count_documents({})
    print(f"Total records: {total}")

    print("\nTop 5 most expensive trips (by total_amount):")
    for doc in collection.find({}, {"total_amount": 1, "trip_distance": 1}).sort("total_amount", -1).limit(5):
        print(doc)

    print("\nTrips per payment_type:")
    for row in collection.aggregate([
        {"$group": {"_id": "$payment_type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]):
        print(row)

    avg = list(collection.aggregate([
        {"$group": {"_id": None, "avg_distance": {"$avg": "$trip_distance"}}}
    ]))
    avg_distance = avg[0]["avg_distance"] if avg else None
    print(f"\nAverage trip distance: {avg_distance}")


if __name__ == "__main__":
    main()
