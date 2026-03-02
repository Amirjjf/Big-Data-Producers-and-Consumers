# Run Instructions

## Single-node MongoDB 

### 1) Start MongoDB
From repo root:
```bash
docker compose -f scripts/docker-compose.yml up -d
```

### 2) Install dependencies
From repo root:
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 3) Ingest data (basic)
From repo root (PowerShell):
```powershell
$env:MONGO_URI="mongodb://localhost:27017"
python code/ingest.py --drop
```
This uses default write concern **w=1**.

---

## Using Write Concern Options (Consistency Settings)

The ingest script supports two consistency modes:
- **w1** → faster, less strict (acknowledged by one node)
- **majority** → stronger consistency (acknowledged by majority of replica set)


### Examples 

Default mode (w1):
```powershell
$env:MONGO_URI="mongodb://localhost:27017"
python code/ingest.py --drop --write_concern w1
```

Majority write concern:
```powershell
$env:MONGO_URI="mongodb://localhost:27017"
python code/ingest.py --drop --write_concern majority
```

With journaling disabled:
```powershell
python code/ingest.py --drop --write_concern majority --journal false
```

Custom write timeout:
```powershell
python code/ingest.py --drop --write_concern majority --wtimeout_ms 20000
```

### Logs
Each ingestion run automatically creates a log file in:
```
logs/ingest_<timestamp>_<writeconcern>.json
```

This file contains:
- total inserted documents
- total time
- throughput
- retries and errors
- used write concern settings

---

## Sharded Cluster 

### 1) Start sharded MongoDB
From the repo root:
```bash
docker compose -f scripts/docker-compose-sharded.yml down
docker compose -f scripts/docker-compose-sharded.yml up -d
```

This starts:
- config server RS: configsvr1, configsvr2, configsvr3  
- shard1 RS: shard1a, shard1b, shard1c  
- shard2 RS: shard2a, shard2b, shard2c  
- one mongos on localhost:27018  


### 2) Initialize sharding
From repo root:
```bash
python code/init_sharding.py
```

### 3) Ingest data via mongos
For sharded ingestion you must point MONGO_URI to mongos:
```powershell
$env:MONGO_URI="mongodb://localhost:27018"
```

Default ingest (w1):
```bash
python code/ingest.py --drop
```

Ingest with majority consistency:
```bash
python code/ingest.py --drop --write_concern majority
```

Majority + custom timeout:
```bash
python code/ingest.py --drop --write_concern majority --wtimeout_ms 15000
```

**Notes:**
- For sharded cluster always use the mongos port (27018)
- `--drop` clears docs using delete_many 
- `--hard-drop` performs real drop() with retries; use only if necessary
- ingest.py retries inserts because mongos/config servers can be temporarily unavailable.

### 4) Run queries + sharding check
```powershell
$env:MONGO_URI="mongodb://localhost:27018"
python code/check_sharding.py
python code/query.py
```

---

## Mongo URIs

- Single-node Mongo: `mongodb://localhost:27017`
- Sharded cluster (mongos): `mongodb://localhost:27018`

Example:
```powershell
$env:MONGO_URI="mongodb://localhost:27018"
python code/ingest.py --drop --write_concern majority
python code/query.py
python code/check_sharding.py
```

---

## Quick Sharded Flow

```bash
docker compose -f scripts/docker-compose-sharded.yml up -d
python code/init_sharding.py
```
```powershell
$env:MONGO_URI="mongodb://localhost:27018"
python code/ingest.py --drop --write_concern majority
python code/query.py
```

---

## Producer Benchmark 

This runs multiple concurrent writers and saves a JSON result file in `logs/`.

Sharded cluster examples (PowerShell):
```powershell
$env:MONGO_URI="mongodb://localhost:27018"
python code/bench_producers.py --concurrency 1,5,10 --limit 50000 --write_concern w1 --runs 1
python code/bench_producers.py --concurrency 1,5,10 --limit 50000 --write_concern majority --runs 1
```

Throttled run:
```powershell
$env:MONGO_URI="mongodb://localhost:27018"
python code/bench_producers.py --concurrency 1,5,10 --limit 20000 --rate_docs_per_sec 2000 --write_concern w1
```

Output log file (default):
```
logs/bench_producers_<timestamp>.json
```

---

## Consumer Benchmark (Part 2.5)

This runs concurrent readers and reports query throughput and latency.

Basic:
```powershell
python code/bench_consumers.py --concurrency 1,5,10 --duration 30 --per_query_timeout_ms 3000
```

With producers running in the background:
```powershell
python code/bench_consumers.py --concurrency 1,5,10 --duration 30 --per_query_timeout_ms 3000 --with_producers true
```

Output log file (default):
```
logs/bench_consumers_<timestamp>.json
```

---

## Force Data Distribution Across Shards (For Demo Presentation)

This lowers chunk size so data spreads across shards.

```powershell
$env:MONGO_URI="mongodb://localhost:27018"
python code/force_balance.py --chunk_mb 64 --wait_seconds 180
```

You can reset chunk size back to default (128MB) by deleting the config setting or setting it again:
```
db.getSiblingDB("config").settings.remove({ _id: "chunksize" })
```
or set it back to `128`.

---

## Reproducibility Notes

### Versions (from local .venv)
- Python: 3.12.4  
- pymongo: 4.16.0  
- pandas: 3.0.0  
- pyarrow: 23.0.0  
- python-dateutil: 2.9.0.post0  

---

## Troubleshooting

Check running containers:
```bash
docker ps -a | findstr mysimbdp
```

Check logs:
```bash
docker logs mysimbdp-configsvr1 --tail 80
```

Re-run sharding init if needed:
```bash
python code/init_sharding.py
```

Restart cluster cleanly:
```bash
docker compose -f scripts/docker-compose-sharded.yml up -d --remove-orphans
```

If ports are already allocated, run compose down and verify no old containers are using ports 27018/27019/27021/27022.
