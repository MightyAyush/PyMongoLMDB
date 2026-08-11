# ⚡ PyMongoLMDB

A high-performance, asynchronous Python database library that seamlessly integrates **MongoDB Atlas** with a local **LMDB** key-value caching system. Built on PyMongo's official asynchronous driver, this library implements a **Write-Behind (Group Commit)** strategy, **generational query caching**, and a **redundant write-bypass check** to optimize write throughput and eliminate database query latency.

---

## ✨ Features

- **Double-Value Return Signature**: Every function returns `(done, result_or_error)` tuples. If a database query fails or a connection drops, it returns `(False, exception_instance)` instead of crashing your program.
- **Pythonic Collection Attributes**: Access collections directly as attributes (`db.collection_name.find_one()`) or dictionary indices (`db["collection_name"].find_one()`).
- **Cache-Matching Write Bypass**: Checks the cache before writing in `add()`. If the document exists in cache and all fields match perfectly, it bypasses the MongoDB network write entirely.
- **Database Seeding (`load_database_to_cache`)**: Exposes a warm-up function to pull a complete shadow copy of your database collections into LMDB on startup.
- **Query Caching (Generational Caching)**: Caches queries like `find`, `count_documents`, `distinct`, and `aggregate` using collection-based versioning to guarantee data consistency.
- **Write-Behind (Group Commit)**: Buffers writes and flushes them in bulk (using `bulk_write`) when:
  - 2 seconds elapse without new write requests.
  - The queue reaches 50 pending operations.
- **Dual-Space Caching**: Supports both **TTL (Time-To-Live)** caching for transient data and **Permanent** caching for static records.
- **Fail-Safe Robustness**: Auto-clears cache when it is full (`lmdb.MapFullError`) and handles cache failure gracefully, keeping your app running.

---

## 📊 Benchmark Performance Results

Our test benchmarks on a live MongoDB Atlas database demonstrate extreme performance gains:

| Operation | Target | Latency / Speed | Throughput (Ops/sec) |
| :--- | :--- | :--- | :--- |
| **Direct DB Read** | MongoDB Atlas | 15.84 s (50 reads) | **3.16 ops/sec** |
| **Cached Read** | Local LMDB Cache | 0.03 s (1000 reads) | **26,693.08 ops/sec** |
| **Write (Buffered)** | 100 concurrent writes | 14.72 s (100 writes) | **6.79 ops/sec** |

> [!TIP]
> **Performance Gain**: Cache reads are **8,400x+ faster** than direct network queries to MongoDB Atlas!
> **RAM Stability**: Memory remains low and stable (~53 MB total process footprint) throughout heavy benchmark runs.

---

## 🚀 Installation & Setup

Install the library locally in editable mode:
```bash
pip install -e .
```

Place your MongoDB credentials in a `.env` file in the root of your project:
```env
MONGO_URI="mongodb+srv://<username>:<password>@cluster.mongodb.net/?appName=YourAppName"
MONGO_DB_NAME="your_database_name"
```

---

## 🛠️ Usage Guide

### 1. Initialization

Import and instantiate `MongoDB` inside your async code:

```python
import asyncio
from PyMongoLMDB import MongoDB

async def main():
    db = MongoDB(
        uri="mongodb+srv://...",
        db_name="your_database_name",
        cache_path="./lmdb_data",      # Optional: path to LMDB cache directory
        cache_map_size=100.0           # Optional: max cache limit in Megabytes (100MB default)
    )
    
    # Optional: Load database shadow copy to LMDB cache on startup
    done, msg = await db.load_database_to_cache()
    if done:
        print("Warm cache setup complete!")
```

---

### 2. Double-Value Returns & CRUD Examples

All CRUD operations return `(success, result_or_error)`.

#### Add / Upsert Documents

The `add()` method checks if a document exists by `_id`. If it does, and all fields match the cache perfectly, the DB write is **skipped**. If they differ, the document is updated and the write is queued.

```python
# Caches immediately and queues bulk database write in the background
done, result = await db.served_chats.add(
    doc_id="chat_12345",
    fields={"name": "Alice", "role": "admin", "status": "active"},
    ttl=False  # Cache permanently
)
if done:
    print(f"Upsert queued or skipped (matches cache): {result}")
```

#### Find One (Query Cache / Database)

```python
done, chat = await db.served_chats.find_one({"_id": "chat_12345"})
if done and chat:
    print(f"Fetched chat: {chat}")
```

#### Find Multiple Documents (Cached)

```python
# Cached under query version key; hits are returned instantly
done, active_chats = await db.served_chats.find({"status": "active"})
```

#### Other Operations

```python
# Insert a document (invalidates query caches)
done, res = await db.served_chats.insert_one({"_id": "chat_abc", "name": "Bob"})

# Count documents (caching protects your MongoDB database from repeated count requests)
done, total_active = await db.served_chats.count_documents({"status": "active"})

# Distinct values query
done, names = await db.served_chats.distinct("name")

# Aggregation pipelines
done, results = await db.served_chats.aggregate([
    {"$match": {"status": "active"}},
    {"$group": {"_id": "$role", "count": {"$sum": 1}}}
])

# Update document (invalidates cache copy)
done, res = await db.served_chats.update_one({"_id": "chat_abc"}, {"$set": {"status": "inactive"}})

# Delete document
done, res = await db.served_chats.delete_one({"_id": "chat_abc"})
```

---

## 🛡️ Optional Control Arguments

All major operations support optional control arguments:

- **`no_cache=True`**: Bypasses the LMDB cache entirely (reads and writes directly to/from MongoDB).
- **`ttl`**: 
  - `ttl=False`: Stores the document permanently inside the permanent cache database (never expires).
  - `ttl=True`: Caches with default TTL (1 hour).
  - `ttl=600`: Custom TTL (caches the document with a 10-minute expiry).

---

## 🌪️ Fail-Safe Caching (Graceful Degradation)

If LMDB runs out of space or encounters any structural error, it handles it gracefully:
1. **MapFullError Handling**: The library catches `lmdb.MapFullError`, clears the cache automatically (`txn.drop()`), and continues servicing writes safely.
2. **Crash Prevention**: All cache operations degrade gracefully. If the local disk runs out of space or the cache file is locked, operations bypass the cache and read/write directly from MongoDB.

---

## 👨‍💻 Author & Support

- **Author**: Ayush
- **GitHub**: [@MightyAyush](https://github.com/MightyAyush)
- **Telegram**: [@MightyAyush](https://t.me/MightyAyush)
- **Support Community**: [@BuildTalk](https://t.me/BuildTalk) on Telegram

