import os
import lmdb
import pickle
import time
import logging
from typing import Union, Any, Optional

# Set up logging for the cache module
logger = logging.getLogger("PyMongoLMDB.cache")

class LMDBCache:
    """
    In-process LMDB cache handler managing named databases:
    - "ttl_cache" for documents with an expiration timestamp.
    - "perm_cache" for permanent documents and metadata.
    """
    def __init__(self):
        self.env: Optional[lmdb.Environment] = None
        self.ttl_db: Optional[lmdb._Database] = None
        self.perm_db: Optional[lmdb._Database] = None
        self.path: Optional[str] = None

    def initialize(self, path: str = "./lmdb_data", map_size: int = 100 * 1024 * 1024):
        """Initializes the LMDB environment with named sub-databases."""
        try:
            self.path = path
            os.makedirs(path, exist_ok=True)
            # Open environment; max_dbs=2 permits named databases
            self.env = lmdb.open(path, map_size=map_size, max_dbs=2)
            self.ttl_db = self.env.open_db(b"ttl_cache")
            self.perm_db = self.env.open_db(b"perm_cache")
            logger.info(f"LMDB cache environment initialized at {path}")
        except Exception as e:
            logger.error(f"Failed to initialize LMDB cache: {e}")
            self.env = None

    def _get_key(self, collection: str, doc_id: Any) -> bytes:
        """Serializes collection name and document ID to avoid type collision."""
        return pickle.dumps((collection, doc_id))

    def set(self, collection: str, doc_id: Any, document: dict, ttl: Union[bool, int, float] = True, default_ttl: int = 3600):
        """Saves a document to the cache database (TTL or permanent). Handles MapFullError by clearing cache."""
        if self.env is None:
            return

        try:
            key = self._get_key(collection, doc_id)
            
            if ttl is False:
                # Store permanently (no expiration)
                serialized_doc = pickle.dumps(document)
                with self.env.begin(write=True) as txn:
                    txn.delete(key, db=self.ttl_db)
                    txn.put(key, serialized_doc, db=self.perm_db)
            else:
                # Store with TTL expiration
                duration = ttl if isinstance(ttl, (int, float)) and not isinstance(ttl, bool) else default_ttl
                expiry = time.time() + duration
                serialized_entry = pickle.dumps((expiry, document))
                with self.env.begin(write=True) as txn:
                    txn.delete(key, db=self.perm_db)
                    txn.put(key, serialized_entry, db=self.ttl_db)
        except lmdb.MapFullError as e:
            # Catch DB full error, log warning, and evict all cache keys
            logger.error(f"LMDB Cache is full: {e}. Clearing cache to reclaim space...")
            self.clear()
        except Exception as e:
            logger.error(f"Error writing to LMDB cache: {e}")

    def get(self, collection: str, doc_id: Any) -> Optional[dict]:
        """Retrieves a document from LMDB, checking and evicting expired TTL values."""
        if self.env is None:
            return None

        try:
            key = self._get_key(collection, doc_id)

            # 1. Search permanent database
            with self.env.begin(db=self.perm_db) as txn:
                data = txn.get(key)
                if data is not None:
                    try:
                        return pickle.loads(data)
                    except Exception as e:
                        logger.error(f"Error deserializing permanent cache: {e}")
                        return None

            # 2. Search TTL database
            with self.env.begin(db=self.ttl_db) as txn:
                data = txn.get(key)
                if data is None:
                    return None
                
                try:
                    expiry, document = pickle.loads(data)
                    expired = time.time() > expiry
                except Exception as e:
                    logger.error(f"Error deserializing TTL cache: {e}")
                    return None

            # Clean up expired entry in a write transaction
            if expired:
                with self.env.begin(write=True) as txn:
                    txn.delete(key, db=self.ttl_db)
                return None

            return document
        except Exception as e:
            logger.error(f"Error reading from LMDB cache: {e}")
            return None

    def delete(self, collection: str, doc_id: Any):
        """Deletes a key from all sub-databases."""
        if self.env is None:
            return

        try:
            key = self._get_key(collection, doc_id)
            with self.env.begin(write=True) as txn:
                txn.delete(key, db=self.ttl_db)
                txn.delete(key, db=self.perm_db)
        except Exception as e:
            logger.error(f"Error deleting key from LMDB cache: {e}")

    def clear(self):
        """Clears all caches by dropping and re-creating sub-databases."""
        if self.env is None:
            return
        try:
            with self.env.begin(write=True) as txn:
                txn.drop(self.ttl_db, delete=False)
                txn.drop(self.perm_db, delete=False)
            logger.info("LMDB cache databases cleared successfully.")
        except Exception as e:
            logger.error(f"Error clearing LMDB cache databases: {e}")

    def close(self):
        """Gracefully closes the LMDB environment."""
        if self.env:
            try:
                self.env.close()
            except Exception as e:
                logger.error(f"Error closing LMDB environment: {e}")
            self.env = None

    def get_collection_version(self, collection: str) -> int:
        """Retrieves the generational version of a collection. Defaults to 0."""
        if self.env is None:
            return 0
        try:
            key = self._get_key(collection, "_version_")
            with self.env.begin(db=self.perm_db) as txn:
                data = txn.get(key)
                if data is not None:
                    return pickle.loads(data)
        except Exception as e:
            logger.error(f"Error retrieving version for {collection}: {e}")
        return 0

    def increment_collection_version(self, collection: str) -> int:
        """Increments and returns the generational version of a collection to invalidate query caches."""
        if self.env is None:
            return 0
        try:
            key = self._get_key(collection, "_version_")
            with self.env.begin(write=True) as txn:
                data = txn.get(key, db=self.perm_db)
                version = 0
                if data is not None:
                    try:
                        version = pickle.loads(data)
                    except Exception:
                        pass
                new_version = version + 1
                txn.put(key, pickle.dumps(new_version), db=self.perm_db)
                logger.info(f"Incremented collection version for '{collection}' to {new_version}")
                return new_version
        except Exception as e:
            logger.error(f"Error incrementing version for {collection}: {e}")
            return 0
