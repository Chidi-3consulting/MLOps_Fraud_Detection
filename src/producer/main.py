#!/usr/bin/env python3
"""
pure_api_to_kafka.py - Only reads from API, no mock data
"""

import os
import json
import time
import logging
import requests
from typing import Dict, Any, Optional
from datetime import datetime
import signal
import hashlib
import sqlite3
import time as _time

try:
    from confluent_kafka import Producer
except ImportError:
    class DummyProducer:
        def __init__(self, *args, **kwargs):
            pass
        def produce(self, topic, key=None, value=None, callback=None):
            if callback:
                try:
                    callback(None, type('Msg', (), {'topic': lambda: topic, 'partition': lambda: 0})())
                except Exception:
                    pass
        def poll(self, timeout=0):
            return None
        def flush(self, timeout=None):
            return 0
        def close(self):
            return None
    Producer = DummyProducer

try:
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    AdminClient = None
    NewTopic = None

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class PureAPItoKafkaProducer:
    def __init__(self):
        # Optional Redis import (lazy to keep runtime flexible)
        try:
            import redis as _redis
            self._redis_lib = _redis
        except Exception:
            self._redis_lib = None

        self.api_url = os.getenv("FRAUD_API_URL", "https://3consult-ng.com/fraud_api.php")
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.kafka_username = os.getenv("KAFKA_USERNAME")
        self.kafka_password = os.getenv("KAFKA_PASSWORD")
        # Force the topic to 'Fraud_transactions' regardless of environment to avoid misconfiguration
        env_topic = os.getenv("KAFKA_TOPIC")
        if env_topic and env_topic != "Fraud_transactions":
            logger.warning(f"Ignoring KAFKA_TOPIC='{env_topic}' and forcing topic to 'Fraud_transactions'")
        self.topic = "Fraud_transactions"
        self.batch_size = int(os.getenv("BATCH_SIZE", "10000"))
        
        # Offset tracking - THIS PREVENTS DUPLICATES ACROSS RUNS
        self.offset_file = "api_offset.txt"
        self.current_offset = self._load_last_offset()
        
        self.running = False
        self.total_sent = 0
        self._batch_delivered = 0
        self.dedup_skipped = 0

        # Optional Redis-based cross-run deduplication
        self.redis = self._init_redis_client()
        self.redis_set_key = os.getenv("REDIS_DEDUP_SET_KEY", "fraud:seen_transaction_ids")
        # Optional Redis-backed offset key
        self.offset_redis_key = os.getenv("REDIS_OFFSET_KEY", "fraud:producer_offset")
        # TTL (seconds) for inflight reservations; prevents duplicates across short restarts
        self.redis_inflight_ttl = int(os.getenv("REDIS_INFLIGHT_TTL", "900"))
        # Local fallback guard within process in case Redis is unavailable
        self.local_seen_keys = set()

        # Durable local fallback (works even if Redis is down): SQLite-backed state
        self.state_db_path = os.getenv("DEDUP_DB_PATH", os.path.join("state", "producer_state.sqlite"))
        self._init_sqlite_state()
        
        # Ensure topic exists before producing (best-effort; works if credentials permit)
        self._ensure_topic_exists()

        self.producer = self._create_kafka_producer()
        signal.signal(signal.SIGINT, self.shutdown)
        
        logger.info(f"Kafka bootstrap: {self.bootstrap_servers}, topic: {self.topic}")
        logger.info(f"Pure API to Kafka Producer - Starting from offset: {self.current_offset}")

    def _load_last_offset(self) -> int:
        """Load where we left off to avoid re-reading the same data"""
        # Prefer Redis if available
        if hasattr(self, "redis") and self.redis:
            try:
                val = self.redis.get(self.offset_redis_key)
                if val is not None:
                    return int(val)
            except Exception as e:
                logger.warning(f"Failed to load offset from Redis, falling back to file: {e}")
        # Next, try SQLite durable store
        try:
            sqlite_offset = self._sqlite_get_offset()
            if sqlite_offset is not None:
                return sqlite_offset
        except Exception as e:
            logger.warning(f"Failed to load offset from SQLite, falling back to file: {e}")
        # Fallback to local file
        try:
            if os.path.exists(self.offset_file):
                with open(self.offset_file, 'r') as f:
                    return int(f.read().strip())
        except Exception:
            pass
        return 0  # Start from beginning on first run

    def _save_last_offset(self, offset: int):
        """Save progress so next run continues from here"""
        # Write to Redis if available
        if hasattr(self, "redis") and self.redis:
            try:
                self.redis.set(self.offset_redis_key, str(offset))
            except Exception as e:
                logger.warning(f"Failed to save offset to Redis: {e}")
        # Also persist to SQLite
        try:
            self._sqlite_set_offset(offset)
        except Exception as e:
            logger.warning(f"Failed to save offset to SQLite: {e}")
        # Always persist to file as a backup
        try:
            with open(self.offset_file, 'w') as f:
                f.write(str(offset))
        except Exception as e:
            logger.error(f"Failed to save offset file: {e}")

    def _create_kafka_producer(self):
        producer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "api-producer"
        }

        if self.kafka_username and self.kafka_password:
            producer_config.update({
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": self.kafka_username,
                "sasl.password": self.kafka_password,
                "ssl.endpoint.identification.algorithm": "https",
                "broker.address.family": "v4",
                "request.timeout.ms": 20000,
                "acks": "all",
                "enable.idempotence": True,
            })

        return Producer(producer_config)

    def _ensure_topic_exists(self):
        """Best-effort check/create of topic on startup.
        For Confluent Cloud, this requires appropriate permissions; otherwise it no-ops.
        """
        if AdminClient is None or NewTopic is None:
            logger.info("AdminClient not available; skipping topic existence check.")
            return

        admin_config = {
            "bootstrap.servers": self.bootstrap_servers
        }
        if self.kafka_username and self.kafka_password:
            admin_config.update({
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": self.kafka_username,
                "sasl.password": self.kafka_password,
                "ssl.endpoint.identification.algorithm": "https",
                "broker.address.family": "v4",
            })

        try:
            admin = AdminClient(admin_config)
            md = admin.list_topics(timeout=10)
            topic_meta = md.topics.get(self.topic)
            if topic_meta is not None and not topic_meta.error:
                return

            logger.info(f"Topic '{self.topic}' not found; attempting to create...")
            futures = admin.create_topics([NewTopic(self.topic, num_partitions=3, replication_factor=3)])
            for _, f in futures.items():
                try:
                    f.result()
                    logger.info(f"Topic '{self.topic}' created successfully.")
                except Exception as e:
                    # If the topic already exists or creation is forbidden, log and continue
                    logger.warning(f"Topic create result: {e}")
        except Exception as e:
            logger.warning(f"Skipping topic check/create due to error: {e}")

    def download_batch(self) -> Optional[list]:
        """Download batch from API using current offset"""
        logger.info(f"Downloading from API - Offset: {self.current_offset}, Limit: {self.batch_size}")
        
        try:
            params = {'limit': self.batch_size, 'offset': self.current_offset}
            response = requests.get(self.api_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('data'):
                    transactions = data['data']
                    logger.info(f"✅ Received {len(transactions)} transactions from API")

                    return transactions
                else:
                    logger.warning("API returned no data or error")
                    return None
            else:
                logger.error(f"HTTP Error: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"API download failed: {e}")
            return None

    def delivery_report(self, err, msg):
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            self._batch_delivered += 1
            # On successful delivery, remember the key as seen
            try:
                key_bytes = msg.key()
                if key_bytes is None:
                    return
                key_str = key_bytes.decode("utf-8") if isinstance(key_bytes, (bytes, bytearray)) else str(key_bytes)
                self._finalize_key(key_str)
            except Exception as e:
                logger.warning(f"Failed to mark key as seen in Redis: {e}")

    def send_to_kafka(self, transaction: Dict[str, Any]) -> bool:
        try:
            # Deterministic key for deduplication downstream (log compaction/upserts)
            tx_id = transaction.get('transaction_id')
            if tx_id:
                key_str = str(tx_id)
            else:
                payload = json.dumps(transaction, sort_keys=True, separators=(',', ':'))
                key_str = hashlib.sha256(payload.encode('utf-8')).hexdigest()

            # Strong dedup: reserve key before producing to prevent duplicates across restarts
            if not self._reserve_key(key_str):
                self.dedup_skipped += 1
                return False

            self.producer.produce(
                self.topic,
                key=key_str,
                value=json.dumps(transaction),
                callback=self.delivery_report
            )
            self.producer.poll(0)
            return True
        except Exception as e:
            logger.error(f"Send failed: {e}")
            return False

    def _reserve_key(self, key_str: str) -> bool:
        """Reserve a key before producing to avoid duplicates across restarts.
        Strategy:
          - If Redis available:
              - If key already in 'seen' set -> skip
              - Use SETNX on 'fraud:inflight:{key}' with TTL -> if exists, skip; else proceed
          - Else if SQLite available:
               - If key in seen_keys -> skip
               - Insert into inflight_keys with expires_at -> if conflict, skip
          - If neither available: fall back to in-memory set for current process
        """
        try:
            if self.redis:
                if self.redis.sismember(self.redis_set_key, key_str):
                    return False
                inflight_key = f"{self.redis_set_key}:inflight:{key_str}"
                # Reserve if not exists
                reserved = self.redis.setnx(inflight_key, "1")
                if not reserved:
                    return False
                # Ensure it expires to avoid permanent lock if process dies
                self.redis.expire(inflight_key, self.redis_inflight_ttl)
                return True

            if self._sqlite_conn is not None:
                # Clean expired inflight
                now = int(_time.time())
                try:
                    self._sqlite_conn.execute("DELETE FROM inflight_keys WHERE expires_at IS NOT NULL AND expires_at < ?", (now,))
                    self._sqlite_conn.commit()
                except Exception:
                    pass
                # Check seen
                cur = self._sqlite_conn.execute("SELECT 1 FROM seen_keys WHERE key = ? LIMIT 1", (key_str,))
                if cur.fetchone():
                    return False
                # Try reserve inflight
                expires_at = now + self.redis_inflight_ttl
                try:
                    self._sqlite_conn.execute("INSERT INTO inflight_keys(key, expires_at) VALUES(?, ?)", (key_str, expires_at))
                    self._sqlite_conn.commit()
                    return True
                except sqlite3.IntegrityError:
                    return False
                except Exception as e:
                    logger.warning(f"SQLite reserve failed, falling back to local memory: {e}")
                    # Fallthrough to local

            # Fallback local-only guard
            if key_str in self.local_seen_keys:
                return False
            self.local_seen_keys.add(key_str)
            return True
        except Exception as e:
            logger.warning(f"Reserve key failed, proceeding without Redis reservation: {e}")
            # Last resort: allow, relying on idempotence and downstream compaction
            return True

    def _finalize_key(self, key_str: str) -> None:
        """Mark key as seen and release inflight reservation on successful delivery."""
        try:
            if self.redis:
                self.redis.sadd(self.redis_set_key, key_str)
                inflight_key = f"{self.redis_set_key}:inflight:{key_str}"
                try:
                    self.redis.delete(inflight_key)
                except Exception:
                    pass
            if self._sqlite_conn is not None:
                try:
                    self._sqlite_conn.execute("INSERT OR IGNORE INTO seen_keys(key) VALUES(?)", (key_str,))
                    self._sqlite_conn.execute("DELETE FROM inflight_keys WHERE key = ?", (key_str,))
                    self._sqlite_conn.commit()
                except Exception as e:
                    logger.warning(f"SQLite finalize failed: {e}")
            # Always add to local guard too
            self.local_seen_keys.add(key_str)
        except Exception as e:
            logger.warning(f"Finalize key failed: {e}")

    def _init_sqlite_state(self) -> None:
        """Initialize durable local SQLite state for offsets and dedup when Redis is unavailable."""
        try:
            state_dir = os.path.dirname(self.state_db_path)
            if state_dir and not os.path.exists(state_dir):
                os.makedirs(state_dir, exist_ok=True)
            self._sqlite_conn = sqlite3.connect(self.state_db_path, check_same_thread=False)
            self._sqlite_conn.execute("PRAGMA journal_mode=WAL;")
            self._sqlite_conn.execute("PRAGMA synchronous=NORMAL;")
            # Create tables
            self._sqlite_conn.execute("CREATE TABLE IF NOT EXISTS offsets (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL)")
            self._sqlite_conn.execute("CREATE TABLE IF NOT EXISTS seen_keys (key TEXT PRIMARY KEY)")
            self._sqlite_conn.execute("CREATE TABLE IF NOT EXISTS inflight_keys (key TEXT PRIMARY KEY, expires_at INTEGER)")
            # Ensure single row for offsets
            cur = self._sqlite_conn.execute("SELECT value FROM offsets WHERE id = 1")
            if cur.fetchone() is None:
                self._sqlite_conn.execute("INSERT INTO offsets(id, value) VALUES(1, 0)")
            self._sqlite_conn.commit()
            logger.info(f"SQLite state initialized at {self.state_db_path}")
        except Exception as e:
            self._sqlite_conn = None
            logger.warning(f"SQLite state not available: {e}")

    def _sqlite_get_offset(self) -> Optional[int]:
        if self._sqlite_conn is None:
            return None
        cur = self._sqlite_conn.execute("SELECT value FROM offsets WHERE id = 1")
        row = cur.fetchone()
        return int(row[0]) if row else None

    def _sqlite_set_offset(self, offset: int) -> None:
        if self._sqlite_conn is None:
            return
        self._sqlite_conn.execute("UPDATE offsets SET value = ? WHERE id = 1", (int(offset),))
        self._sqlite_conn.commit()

    def _init_redis_client(self):
        """Initialize Redis client if configured via environment."""
        if self._redis_lib is None:
            return None
        redis_url = os.getenv("REDIS_URL")
        host = os.getenv("REDIS_HOST")
        if not redis_url and not host:
            return None
        try:
            if redis_url:
                client = self._redis_lib.Redis.from_url(redis_url, decode_responses=True)
            else:
                port = int(os.getenv("REDIS_PORT", "6379"))
                password = os.getenv("REDIS_PASSWORD")
                db = int(os.getenv("REDIS_DB", "0"))
                client = self._redis_lib.Redis(host=host, port=port, password=password, db=db, decode_responses=True)
            client.ping()
            logger.info("Connected to Redis for deduplication.")
            return client
        except Exception as e:
            logger.warning(f"Redis not available for deduplication: {e}")
            return None

    def stream_continuously(self):
        """Stream data continuously - each run continues from last offset"""
        self.running = True
        logger.info("🚀 Starting continuous streaming from API...")
        
        try:
            while self.running:
                transactions = self.download_batch()
                
                if not transactions:
                    logger.info("No more data from API, waiting...")
                    time.sleep(10)
                    continue
                
                # Send all transactions
                self._batch_delivered = 0
                for transaction in transactions:
                    self.send_to_kafka(transaction)

                # Wait for delivery callbacks and advance offset only by successes
                self.producer.flush(30)
                delivered_now = self._batch_delivered
                self.total_sent += delivered_now
                self.current_offset += delivered_now
                if delivered_now:
                    self._save_last_offset(self.current_offset)
                
                logger.info(f"📦 Sent batch - Delivered: {delivered_now}, Total: {self.total_sent}, Next offset: {self.current_offset}")
                time.sleep(2)  # Be nice to the API
                
        except KeyboardInterrupt:
            logger.info("Stopped by user")
        finally:
            self.shutdown()

    def stream_count(self, count: int):
        """Stream specific number of records"""
        self.running = True
        logger.info(f"Streaming {count} records from API...")
        
        try:
            while self.running and self.total_sent < count:
                transactions = self.download_batch()
                
                if not transactions:
                    logger.info("No more data available")
                    break
                
                self._batch_delivered = 0
                for transaction in transactions:
                    if self.total_sent >= count:
                        break
                    self.send_to_kafka(transaction)

                self.producer.flush(30)
                delivered_now = self._batch_delivered
                self.total_sent += delivered_now
                self.current_offset += delivered_now
                if delivered_now:
                    self._save_last_offset(self.current_offset)
                
                logger.info(f"Progress: {self.total_sent}/{count}, Next offset: {self.current_offset}")
            
            logger.info(f"✅ Streamed {self.total_sent} records")
            
        except KeyboardInterrupt:
            logger.info("Stopped by user")
        finally:
            self.shutdown()

    def shutdown(self, signum=None, frame=None):
        if self.running:
            self.running = False
            logger.info(f"🛑 Shutdown - Final offset: {self.current_offset}, Total sent: {self.total_sent}")
            self.producer.flush(10)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['continuous', 'batch'], default='continuous')
    parser.add_argument('--count', type=int, default=1000)
    
    args = parser.parse_args()
    
    producer = PureAPItoKafkaProducer()
    
    try:
        if args.mode == 'batch':
            producer.stream_count(args.count)
        else:
            producer.stream_continuously()
    except Exception as e:
        logger.error(f"Failed: {e}")