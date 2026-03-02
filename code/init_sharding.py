import time

from pymongo import ASCENDING, MongoClient
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError


CONFIG_URI = "mongodb://127.0.0.1:27019/?directConnection=true"
SHARD1_URI = "mongodb://127.0.0.1:27021/?directConnection=true"
SHARD2_URI = "mongodb://127.0.0.1:27022/?directConnection=true"
MONGOS_URI = "mongodb://127.0.0.1:27018/?directConnection=true"


def wait_for_ready(uri, name, max_attempts=90, sleep_seconds=2):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            print(f"{name} is ready.")
            return True
        except (ServerSelectionTimeoutError, OperationFailure) as e:
            last_error = str(e)
        except Exception as e:
            last_error = str(e)
        if attempt % 5 == 0:
            print(f"Waiting for {name}... attempt {attempt}/{max_attempts}")
        time.sleep(sleep_seconds)
    print(f"{name} not ready. Last error: {last_error}")
    return False

# to handle cases where the command was already executed in a previous run, or if the shard is already added, etc.  
def safe_admin_command(client, cmd):
    try:
        return client.admin.command(cmd)
    except OperationFailure as e:
        message = str(e).lower()
        code_name = (e.details or {}).get("codeName", "")
        if code_name == "AlreadyInitialized" or "already initialized" in message or "already exists" in message:
            print(f"Already done: {cmd}")
            return None
        if "already enabled" in message or "already sharded" in message:
            print(f"Already done: {cmd}")
            return None
        raise

# Creates an index, but if it already exists, it won’t crash.
def safe_create_index(collection, keys, index_name):
    try:
        collection.create_index(keys, name=index_name)
    except OperationFailure as e:
        message = str(e).lower()
        if "already exists" in message or "indexoptionsconflict" in message:
            print(f"Index already there: {index_name}")
            return
        raise


def initiate_replset(uri, config, name):
    client = MongoClient(uri, serverSelectionTimeoutMS=4000)
    print(f"Initiating {name} ...")
    for attempt in range(1, 11):
        try:
            # This connects to the first node of that replica set (configsvr1, shard1a, shard2a )
            safe_admin_command(client, {"replSetInitiate": config})
            return True
        except Exception as e:
            if attempt == 10:
                print(f"Failed to initiate {name}: {e}")
                return False
            print(f"Retrying {name} initiate... {attempt}/10")
            time.sleep(2)


def wait_for_primary(uri, name, max_attempts=90, sleep_seconds=2):
    client = MongoClient(uri, serverSelectionTimeoutMS=4000)
    for attempt in range(1, max_attempts + 1):
        try:
            status = client.admin.command("replSetGetStatus")
            primary = None
            for member in status.get("members", []):
                if member.get("stateStr") == "PRIMARY":
                    primary = member.get("name")
                    break
            if primary:
                print(f"{name} primary: {primary}")
                return True
        except Exception:
            pass
        if attempt % 5 == 0:
            print(f"Waiting for PRIMARY in {name}... attempt {attempt}/{max_attempts}")
        time.sleep(sleep_seconds)
    print(f"PRIMARY not ready for {name}")
    return False


def main():
    if not wait_for_ready(CONFIG_URI, "configsvr1"):
        return
    if not wait_for_ready(SHARD1_URI, "shard1a"):
        return
    if not wait_for_ready(SHARD2_URI, "shard2a"):
        return

    config_repl = {
        "_id": "configReplSet",
        "configsvr": True,
        "members": [
            {"_id": 0, "host": "configsvr1:27019"},
            {"_id": 1, "host": "configsvr2:27019"},
            {"_id": 2, "host": "configsvr3:27019"},
        ],
    }
    shard1_repl = {
        "_id": "shard1",
        "members": [
            {"_id": 0, "host": "shard1a:27021"},
            {"_id": 1, "host": "shard1b:27021"},
            {"_id": 2, "host": "shard1c:27021"},
        ],
    }
    shard2_repl = {
        "_id": "shard2",
        "members": [
            {"_id": 0, "host": "shard2a:27022"},
            {"_id": 1, "host": "shard2b:27022"},
            {"_id": 2, "host": "shard2c:27022"},
        ],
    }

    if not initiate_replset(CONFIG_URI, config_repl, "configReplSet"):
        return
    if not wait_for_primary(CONFIG_URI, "configReplSet"):
        return

    if not initiate_replset(SHARD1_URI, shard1_repl, "shard1"):
        return
    if not wait_for_primary(SHARD1_URI, "shard1"):
        return

    if not initiate_replset(SHARD2_URI, shard2_repl, "shard2"):
        return
    if not wait_for_primary(SHARD2_URI, "shard2"):
        return

    if not wait_for_ready(MONGOS_URI, "mongos"):
        return

    mongos = MongoClient(MONGOS_URI, serverSelectionTimeoutMS=4000)
    print("Adding shards ...")
    safe_admin_command(
        mongos,
        {"addShard": "shard1/shard1a:27021,shard1b:27021,shard1c:27021"},
    )
    safe_admin_command(
        mongos,
        {"addShard": "shard2/shard2a:27022,shard2b:27022,shard2c:27022"},
    )

    print("Enabling sharding for DB mysimbdp ...")
    safe_admin_command(mongos, {"enableSharding": "mysimbdp"})

    trips = mongos["mysimbdp"]["yellow_trips"]
    print("Creating shard-key index first ...")
    safe_create_index(trips, [("pickup_day", "hashed")], "pickup_day_hashed")

    ns = "mysimbdp.yellow_trips"
    coll_meta = mongos["config"]["collections"].find_one(
        {"_id": ns},
        {"key": 1, "dropped": 1},
    )
    if coll_meta and coll_meta.get("key") and not coll_meta.get("dropped", False):
        print(f"{ns} is already sharded. Skipping shardCollection.")
    else:
        shard_cmd = {"shardCollection": ns, "key": {"pickup_day": "hashed"}}
        print(f"Sharding command: {shard_cmd}")
        safe_admin_command(mongos, shard_cmd)

    print("Creating indexes on mysimbdp.yellow_trips ...")
    safe_create_index(trips, [("pickup_day", ASCENDING)], "pickup_day_1")
    safe_create_index(trips, [("PULocationID", ASCENDING)], "PULocationID_1")
    safe_create_index(trips, [("DOLocationID", ASCENDING)], "DOLocationID_1")
    safe_create_index(trips, [("tpep_pickup_datetime", ASCENDING)], "tpep_pickup_datetime_1")

    print("Sharding setup done.")


if __name__ == "__main__":
    main()
