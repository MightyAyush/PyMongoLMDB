import asyncio
import logging
import pickle
import time
from typing import Any, Dict, List, Optional, Union, Tuple
import pymongo
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from .cache import LMDBCache

# Set up logging for the mongo module
logger = logging.getLogger("PyMongoLMDB.mongo")

def extract_id_from_filter(filter_dict: Any) -> Any:
    """Helper to extract a simple _id value from a query filter."""
    if isinstance(filter_dict, dict) and "_id" in filter_dict:
        doc_id = filter_dict["_id"]
        if not isinstance(doc_id, dict):
            return doc_id
    return None

class CollectionWrapper:
    """
    Pythonic collection wrapper matching PyMongo's collection attribute access interface.
    Delegates all CRUD methods to MongoDB wrapper class, returning (success, result_or_error) tuples.
    """
    def __init__(self, db_client: 'MongoDB', collection_name: str):
        self.db_client = db_client
        self.collection_name = collection_name

    async def add(self, doc_id: Any, fields: Optional[dict] = None, no_cache: bool = False, ttl: Union[bool, int, float] = True) -> Tuple[bool, Any]:
        return await self.db_client.add(self.collection_name, doc_id, fields, no_cache, ttl)

    async def find_one(self, filter: dict, projection: Optional[dict] = None, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        return await self.db_client.find_one(self.collection_name, filter, projection, no_cache, ttl, **kwargs)

    async def find(self, filter: dict, projection: Optional[dict] = None, limit: int = 0, skip: int = 0, sort: Optional[Any] = None, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        return await self.db_client.find(self.collection_name, filter, projection, limit, skip, sort, no_cache, ttl, **kwargs)

    async def insert_one(self, document: dict, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        return await self.db_client.insert_one(self.collection_name, document, no_cache, ttl, **kwargs)

    async def insert_many(self, documents: List[dict], no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        return await self.db_client.insert_many(self.collection_name, documents, no_cache, ttl, **kwargs)

    async def update_one(self, filter: dict, update: dict, upsert: bool = False, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        return await self.db_client.update_one(self.collection_name, filter, update, upsert, no_cache, ttl, **kwargs)

    async def update_many(self, filter: dict, update: dict, upsert: bool = False, no_cache: bool = False, **kwargs) -> Tuple[bool, Any]:
        return await self.db_client.update_many(self.collection_name, filter, update, upsert, no_cache, **kwargs)

    async def replace_one(self, filter: dict, replacement: dict, upsert: bool = False, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        return await self.db_client.replace_one(self.collection_name, filter, replacement, upsert, no_cache, ttl, **kwargs)

    async def delete_one(self, filter: dict, no_cache: bool = False, **kwargs) -> Tuple[bool, Any]:
        return await self.db_client.delete_one(self.collection_name, filter, no_cache, **kwargs)

    async def delete_many(self, filter: dict, no_cache: bool = False, **kwargs) -> Tuple[bool, Any]:
        return await self.db_client.delete_many(self.collection_name, filter, no_cache, **kwargs)

    async def count_documents(self, filter: dict, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        return await self.db_client.count_documents(self.collection_name, filter, no_cache, ttl, **kwargs)

    async def estimated_document_count(self, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        return await self.db_client.estimated_document_count(self.collection_name, no_cache, ttl, **kwargs)

    async def distinct(self, key: str, filter: Optional[dict] = None, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        return await self.db_client.distinct(self.collection_name, key, filter, no_cache, ttl, **kwargs)

    async def aggregate(self, pipeline: list, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        return await self.db_client.aggregate(self.collection_name, pipeline, no_cache, ttl, **kwargs)


class MongoDB:
    def __init__(self, uri: str, db_name: str, cache_path: str = "./lmdb_data", cache_map_size: float = 100.0, **pymongo_options):
        """
        Initializes the PyMongoLMDB client.
        
        Parameters:
            uri (str): MongoDB Atlas URI connection string.
            db_name (str): Database name.
            cache_path (str): Filepath directory for local LMDB files.
            cache_map_size (float): Maximum cache size in Megabytes (MB). Defaults to 100.0 MB.
            **pymongo_options: Additional client parameters forwarded directly to AsyncMongoClient.
        """
        self.client = AsyncMongoClient(uri, **pymongo_options)
        self.db = self.client[db_name]
        
        # Convert MB size limit input directly to bytes for LMDB initialization
        map_size_bytes = int(cache_map_size * 1024 * 1024)
        
        self.cache = LMDBCache()
        self.cache.initialize(path=cache_path, map_size=map_size_bytes)
        
        # Buffering state per collection
        self.write_queue: Dict[str, List[Tuple[Any, asyncio.Future]]] = {}  # coll_name -> [(op, future)]
        self.queue_event: Dict[str, asyncio.Event] = {}  # coll_name -> Event
        self.flush_task: Dict[str, asyncio.Task] = {}   # coll_name -> Task
        
        logger.info(f"MongoDB client connected to database: {db_name}")
        logger.info(f"LMDB Cache initialized at: {cache_path} with capacity {cache_map_size:.1f} MB")

    def __getattr__(self, name: str) -> CollectionWrapper:
        """Enables collection attribute access (e.g. database.served_chats)."""
        if name.startswith('_'):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
        return CollectionWrapper(self, name)

    def __getitem__(self, name: str) -> CollectionWrapper:
        """Enables collection dictionary access (e.g. database['served_chats'])."""
        return CollectionWrapper(self, name)

    async def close(self):
        """Closes MongoDB client connection and LMDB Cache environment."""
        if self.client:
            await self.client.close()
        if self.cache:
            self.cache.close()

    # ================= WRITE BUFFERING INTERNALS =================

    async def _queue_write(self, collection_name: str, op: Any) -> Any:
        """Queues a write operation and returns a Future that completes when flushed."""
        if collection_name not in self.write_queue:
            self.write_queue[collection_name] = []
            self.queue_event[collection_name] = asyncio.Event()

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.write_queue[collection_name].append((op, fut))

        # Flush immediately if queue reaches 50
        if len(self.write_queue[collection_name]) >= 50:
            self.queue_event[collection_name].set()

        # Start background worker if not already running
        if collection_name not in self.flush_task:
            self.flush_task[collection_name] = asyncio.create_task(
                self._flush_worker(collection_name)
            )

        return await fut

    async def _flush_worker(self, collection_name: str):
        """Background task that waits 2 seconds or until signaled to flush."""
        event = self.queue_event.get(collection_name)
        if not event:
            return
        try:
            try:
                await asyncio.wait_for(event.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                raise
            
            await self._perform_flush(collection_name)
        except asyncio.CancelledError:
            pass
        finally:
            self.flush_task.pop(collection_name, None)
            self.queue_event.pop(collection_name, None)

    async def _perform_flush(self, collection_name: str):
        """Executes the bulk write on MongoDB and resolves pending futures."""
        queue = self.write_queue.pop(collection_name, [])
        if not queue:
            return
        
        ops = [item[0] for item in queue]
        futures = [item[1] for item in queue]
        
        try:
            db_coll = self.db[collection_name]
            result = await db_coll.bulk_write(ops, ordered=False)
            for fut in futures:
                if not fut.done():
                    fut.set_result(result)
        except Exception as e:
            logger.error(f"Bulk write failed for {collection_name}: {e}")
            for fut in futures:
                if not fut.done():
                    fut.set_exception(e)

    async def flush_collection(self, collection_name: str):
        """Synchronously flushes all pending writes for a collection."""
        task = self.flush_task.get(collection_name)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        await self._perform_flush(collection_name)

    async def _flush_before_read(self, collection_name: str):
        """Checks if there are pending writes for the collection, flushing if so."""
        if self.write_queue.get(collection_name):
            await self.flush_collection(collection_name)

    # ================= DATABASE WARMUP/SEEDING METHOD =================

    async def load_database_to_cache(self, collections: Optional[List[str]] = None) -> Tuple[bool, Any]:
        """
        Downloads all documents from the MongoDB database and stores them in the LMDB cache (shadow copy).
        If collections list is specified, only those collections are synced.
        """
        try:
            colls = collections
            if colls is None:
                colls = await self.db.list_collection_names()
                
            for coll_name in colls:
                if coll_name.startswith("system."):
                    continue
                    
                logger.info(f"Seeding collection '{coll_name}' to cache shadow copy...")
                cursor = self.db[coll_name].find({})
                docs = await cursor.to_list(length=None)
                
                if self.cache:
                    for doc in docs:
                        doc_id = doc.get("_id")
                        if doc_id is not None:
                            # Save permanently as a shadow database copy
                            self.cache.set(coll_name, doc_id, doc, ttl=False)
                            
                    # Update generational query cache version
                    self.cache.increment_collection_version(coll_name)
                       
            logger.info("Database loaded successfully into shadow cache.")
            return True, "Database loaded successfully into shadow cache."
        except Exception as e:
            logger.error(f"Failed loading database to cache: {e}")
            return False, e

    # ================= MONGODB METHODS WITH AUTOMATIC CACHING =================

    async def add(self, collection: str, doc_id: Any, fields: Optional[dict] = None, no_cache: bool = False, ttl: Union[bool, int, float] = True) -> Tuple[bool, Any]:
        """
        Upserts a document by _id.
        Check Cache First: If document exists and the update fields match perfectly,
        it skips the MongoDB write operation entirely.
        """
        try:
            # 1. Update LMDB cache immediately (document-level)
            if not no_cache and self.cache:
                cached_doc = self.cache.get(collection, doc_id)
                if cached_doc is not None:
                    # If fields match perfectly, avoid DB write!
                    if fields and all(k in cached_doc and cached_doc[k] == fields[k] for k in fields):
                        logger.info(f"Redundant write avoided for doc '{doc_id}' in '{collection}'; matches cache.")
                        return True, cached_doc
                    
                    if fields:
                        cached_doc.update(fields)
                    self.cache.set(collection, doc_id, cached_doc, ttl=ttl)
                    self.cache.increment_collection_version(collection)
                else:
                    cached_doc = {"_id": doc_id}
                    if fields:
                        cached_doc.update(fields)
                    self.cache.set(collection, doc_id, cached_doc, ttl=ttl)
                    self.cache.increment_collection_version(collection)

            # 2. Prepare and queue MongoDB upsert operation
            if fields:
                op = pymongo.UpdateOne({"_id": doc_id}, {"$set": fields}, upsert=True)
            else:
                op = pymongo.UpdateOne({"_id": doc_id}, {"$setOnInsert": {}}, upsert=True)

            res = await self._queue_write(collection, op)
            return True, res
        except Exception as e:
            logger.error(f"Error in add(): {e}")
            return False, e

    async def find_one(self, collection: str, filter: dict, projection: Optional[dict] = None, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        """
        Queries a single document. If querying by _id and projection is None:
          - Tries cache first.
          - On miss, flushes queue, queries MongoDB, and caches result.
        Otherwise:
          - Checks query-level cache first.
          - On miss, flushes queue, queries MongoDB, and caches result.
        """
        try:
            doc_id = extract_id_from_filter(filter)
            
            # Scenario A: ID-based Lookup
            if doc_id is not None and projection is None and not no_cache and self.cache:
                doc = self.cache.get(collection, doc_id)
                if doc is not None:
                    matches = True
                    for k, v in filter.items():
                        if k != "_id" and (k not in doc or doc[k] != v):
                            matches = False
                            break
                    if matches:
                        return True, doc

                await self._flush_before_read(collection)
                db_doc = await self.db[collection].find_one(filter, projection, **kwargs)
                if db_doc is not None:
                    self.cache.set(collection, doc_id, db_doc, ttl=ttl)
                return True, db_doc

            # Scenario B: General Query (utilizing Generational Query Caching)
            version = 0
            if not no_cache and self.cache:
                version = self.cache.get_collection_version(collection)
                query_key = ("find_one", version, filter, projection)
                cached_result = self.cache.get(collection, query_key)
                if cached_result is not None:
                    return True, (None if cached_result == "__NONE__" else cached_result)

            await self._flush_before_read(collection)
            db_doc = await self.db[collection].find_one(filter, projection, **kwargs)
            
            if not no_cache and self.cache:
                self.cache.set(collection, query_key, db_doc if db_doc is not None else "__NONE__", ttl=ttl)
                if db_doc is not None and projection is None:
                    db_doc_id = db_doc.get("_id")
                    if db_doc_id is not None:
                        self.cache.set(collection, db_doc_id, db_doc, ttl=ttl)
                    
            return True, db_doc
        except Exception as e:
            logger.error(f"Error in find_one(): {e}")
            return False, e

    async def find(self, collection: str, filter: dict, projection: Optional[dict] = None, limit: int = 0, skip: int = 0, sort: Optional[Any] = None, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        """
        Queries multiple documents. Returns a list of dictionaries.
        Checks query-level cache first. Caches returned documents individually.
        """
        try:
            version = 0
            if not no_cache and self.cache:
                version = self.cache.get_collection_version(collection)
                query_key = ("find", version, filter, projection, limit, skip, sort)
                cached_docs = self.cache.get(collection, query_key)
                if cached_docs is not None:
                    return True, cached_docs

            await self._flush_before_read(collection)
            cursor = self.db[collection].find(filter, projection, limit=limit, skip=skip, sort=sort, **kwargs)
            docs = await cursor.to_list(length=None)
            
            if not no_cache and self.cache:
                self.cache.set(collection, query_key, docs, ttl=ttl)
                if projection is None:
                    for doc in docs:
                        doc_id = doc.get("_id")
                        if doc_id is not None:
                            self.cache.set(collection, doc_id, doc, ttl=ttl)
                        
            return True, docs
        except Exception as e:
            logger.error(f"Error in find(): {e}")
            return False, e

    async def insert_one(self, collection: str, document: dict, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        """
        Inserts a single document.
        Caches it immediately, increments collection version, and queues insert.
        """
        try:
            if "_id" not in document:
                from bson import ObjectId
                document["_id"] = ObjectId()

            doc_id = document["_id"]

            if not no_cache and self.cache:
                self.cache.set(collection, doc_id, document, ttl=ttl)
                self.cache.increment_collection_version(collection)

            op = pymongo.InsertOne(document)
            res = await self._queue_write(collection, op)
            return True, res
        except Exception as e:
            logger.error(f"Error in insert_one(): {e}")
            return False, e

    async def insert_many(self, collection: str, documents: List[dict], no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        """
        Inserts multiple documents immediately.
        Caches all inserted documents and increments collection version.
        """
        try:
            await self._flush_before_read(collection)

            from bson import ObjectId
            for doc in documents:
                if "_id" not in doc:
                    doc["_id"] = ObjectId()

            result = await self.db[collection].insert_many(documents, **kwargs)

            if not no_cache and self.cache:
                self.cache.increment_collection_version(collection)
                for doc in documents:
                    doc_id = doc.get("_id")
                    if doc_id is not None:
                        self.cache.set(collection, doc_id, doc, ttl=ttl)

            return True, result
        except Exception as e:
            logger.error(f"Error in insert_many(): {e}")
            return False, e

    async def update_one(self, collection: str, filter: dict, update: dict, upsert: bool = False, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        """
        Updates a document. 
        Invalidates document cache immediately, increments version, and queues UpdateOne.
        """
        try:
            doc_id = extract_id_from_filter(filter)
            if doc_id is not None:
                if not no_cache and self.cache:
                    self.cache.delete(collection, doc_id)
                    self.cache.increment_collection_version(collection)
                op = pymongo.UpdateOne(filter, update, upsert=upsert)
                res = await self._queue_write(collection, op)
                return True, res
            else:
                await self._flush_before_read(collection)
                doc = await self.db[collection].find_one(filter, projection={"_id": 1})
                if doc and not no_cache and self.cache:
                    self.cache.delete(collection, doc["_id"])
                
                if not no_cache and self.cache:
                    self.cache.increment_collection_version(collection)

                result = await self.db[collection].update_one(filter, update, upsert=upsert, **kwargs)
                
                if upsert and result.upserted_id and not no_cache and self.cache:
                    self.cache.delete(collection, result.upserted_id)
                return True, result
        except Exception as e:
            logger.error(f"Error in update_one(): {e}")
            return False, e

    async def update_many(self, collection: str, filter: dict, update: dict, upsert: bool = False, no_cache: bool = False, **kwargs) -> Tuple[bool, Any]:
        """
        Updates multiple documents.
        Resolves matching doc _ids, invalidates them, increments version, and runs immediately.
        """
        try:
            await self._flush_before_read(collection)
            
            if not no_cache and self.cache:
                cursor = self.db[collection].find(filter, projection={"_id": 1})
                docs = await cursor.to_list(length=None)
                doc_ids = [d["_id"] for d in docs]
            else:
                doc_ids = []

            result = await self.db[collection].update_many(filter, update, upsert=upsert, **kwargs)

            if not no_cache and self.cache:
                self.cache.increment_collection_version(collection)
                for doc_id in doc_ids:
                    self.cache.delete(collection, doc_id)
                if upsert and result.upserted_id:
                    self.cache.delete(collection, result.upserted_id)

            return True, result
        except Exception as e:
            logger.error(f"Error in update_many(): {e}")
            return False, e

    async def replace_one(self, collection: str, filter: dict, replacement: dict, upsert: bool = False, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        """
        Replaces a document.
        Updates document cache immediately, increments version, and queues ReplaceOne.
        """
        try:
            doc_id = extract_id_from_filter(filter)
            
            final_replacement = replacement.copy()
            if doc_id is not None and "_id" not in final_replacement:
                final_replacement["_id"] = doc_id

            if doc_id is not None:
                if not no_cache and self.cache:
                    self.cache.set(collection, doc_id, final_replacement, ttl=ttl)
                    self.cache.increment_collection_version(collection)
                op = pymongo.ReplaceOne(filter, replacement, upsert=upsert)
                res = await self._queue_write(collection, op)
                return True, res
            else:
                await self._flush_before_read(collection)
                doc = await self.db[collection].find_one(filter, projection={"_id": 1})
                resolved_id = doc["_id"] if doc else None
                
                if resolved_id is not None and "_id" not in final_replacement:
                    final_replacement["_id"] = resolved_id

                if not no_cache and self.cache:
                    self.cache.increment_collection_version(collection)

                result = await self.db[collection].replace_one(filter, replacement, upsert=upsert, **kwargs)
                
                if not no_cache and self.cache:
                    target_id = resolved_id or (result.upserted_id if upsert else None)
                    if target_id is not None:
                        if "_id" not in final_replacement:
                            final_replacement["_id"] = target_id
                        self.cache.set(collection, target_id, final_replacement, ttl=ttl)
                return True, result
        except Exception as e:
            logger.error(f"Error in replace_one(): {e}")
            return False, e

    async def delete_one(self, collection: str, filter: dict, no_cache: bool = False, **kwargs) -> Tuple[bool, Any]:
        """
        Deletes a single document.
        Deletes document cache, increments version, and queues DeleteOne.
        """
        try:
            doc_id = extract_id_from_filter(filter)
            if doc_id is not None:
                if not no_cache and self.cache:
                    self.cache.delete(collection, doc_id)
                    self.cache.increment_collection_version(collection)
                op = pymongo.DeleteOne(filter)
                res = await self._queue_write(collection, op)
                return True, res
            else:
                await self._flush_before_read(collection)
                doc = await self.db[collection].find_one(filter, projection={"_id": 1})
                if doc and not no_cache and self.cache:
                    self.cache.delete(collection, doc["_id"])
                if not no_cache and self.cache:
                    self.cache.increment_collection_version(collection)
                res = await self.db[collection].delete_one(filter, **kwargs)
                return True, res
        except Exception as e:
            logger.error(f"Error in delete_one(): {e}")
            return False, e

    async def delete_many(self, collection: str, filter: dict, no_cache: bool = False, **kwargs) -> Tuple[bool, Any]:
        """
        Deletes multiple documents.
        Resolves doc _ids, invalidates them, increments version, and runs immediately.
        """
        try:
            await self._flush_before_read(collection)

            if not no_cache and self.cache:
                cursor = self.db[collection].find(filter, projection={"_id": 1})
                docs = await cursor.to_list(length=None)
                doc_ids = [d["_id"] for d in docs]
            else:
                doc_ids = []

            result = await self.db[collection].delete_many(filter, **kwargs)

            if not no_cache and self.cache:
                self.cache.increment_collection_version(collection)
                for doc_id in doc_ids:
                    self.cache.delete(collection, doc_id)

            return True, result
        except Exception as e:
            logger.error(f"Error in delete_many(): {e}")
            return False, e

    # ================= READ-ONLY & UTILITY METHODS WITH CACHING =================

    async def count_documents(self, collection: str, filter: dict, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        """Returns document count, caching the result."""
        try:
            version = 0
            if not no_cache and self.cache:
                version = self.cache.get_collection_version(collection)
                query_key = ("count", version, filter)
                cached_count = self.cache.get(collection, query_key)
                if cached_count is not None:
                    return True, cached_count

            await self._flush_before_read(collection)
            count = await self.db[collection].count_documents(filter, **kwargs)
            
            if not no_cache and self.cache:
                self.cache.set(collection, query_key, count, ttl=ttl)
                
            return True, count
        except Exception as e:
            logger.error(f"Error in count_documents(): {e}")
            return False, e

    async def estimated_document_count(self, collection: str, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        """Returns estimated document count, caching the result."""
        try:
            version = 0
            if not no_cache and self.cache:
                version = self.cache.get_collection_version(collection)
                query_key = ("est_count", version)
                cached_count = self.cache.get(collection, query_key)
                if cached_count is not None:
                    return True, cached_count

            await self._flush_before_read(collection)
            count = await self.db[collection].estimated_document_count(**kwargs)
            
            if not no_cache and self.cache:
                self.cache.set(collection, query_key, count, ttl=ttl)
                
            return True, count
        except Exception as e:
            logger.error(f"Error in estimated_document_count(): {e}")
            return False, e

    async def distinct(self, collection: str, key: str, filter: Optional[dict] = None, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        """Returns distinct values, caching the result."""
        try:
            version = 0
            if not no_cache and self.cache:
                version = self.cache.get_collection_version(collection)
                query_key = ("distinct", version, key, filter)
                cached_values = self.cache.get(collection, query_key)
                if cached_values is not None:
                    return True, cached_values

            await self._flush_before_read(collection)
            values = await self.db[collection].distinct(key, filter, **kwargs)
            
            if not no_cache and self.cache:
                self.cache.set(collection, query_key, values, ttl=ttl)
                
            return True, values
        except Exception as e:
            logger.error(f"Error in distinct(): {e}")
            return False, e

    async def aggregate(self, collection: str, pipeline: list, no_cache: bool = False, ttl: Union[bool, int, float] = True, **kwargs) -> Tuple[bool, Any]:
        """Runs aggregation pipeline, caching the results."""
        try:
            version = 0
            if not no_cache and self.cache:
                version = self.cache.get_collection_version(collection)
                query_key = ("aggregate", version, pipeline)
                cached_results = self.cache.get(collection, query_key)
                if cached_results is not None:
                    return True, cached_results

            await self._flush_before_read(collection)
            cursor = self.db[collection].aggregate(pipeline, **kwargs)
            results = await cursor.to_list(length=None)
            
            if not no_cache and self.cache:
                self.cache.set(collection, query_key, results, ttl=ttl)
                
            return True, results
        except Exception as e:
            logger.error(f"Error in aggregate(): {e}")
            return False, e
