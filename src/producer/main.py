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
        # Resolve topic from env with sensible default
        env_topic = os.getenv("KAFKA_TOPIC")
        self.topic = env_topic if env_topic else "topic_fraud_2"
        self.batch_size = int(os.getenv("BATCH_SIZE", "50000"))
        
        # Offset tracking - THIS PREVENTS DUPLICATES ACROSS RUNS
        self.offset_file = "api_offset.txt"
        
        self.running = False
        self.total_sent = 0
        self._batch_delivered = 0
        self.dedup_skipped = 0

        # Optional Redis-based cross-run deduplication
        self.redis = self._init_redis_client()
        self.redis_set_key = os.getenv("REDIS_DEDUP_SET_KEY", "fraud:seen_transaction_ids")
        # Optional Redis-backed offset key
        self.offset_redis_key = os.getenv("REDIS_OFFSET_KEY", "fraud:producer_offset")
        # Optional Redis-backed distributed offset allocator for multi-replica
        self.distributed_offsets = str(os.getenv("DISTRIBUTED_OFFSET", "false")).lower() in {"1", "true", "yes", "on"}
        self.offset_alloc_key = os.getenv("REDIS_OFFSET_ALLOC_KEY", "fraud:producer_offset_alloc")
        # TTL (seconds) for inflight reservations; prevents duplicates across short restarts
        self.redis_inflight_ttl = int(os.getenv("REDIS_INFLIGHT_TTL", "900"))
        # Local fallback guard within process in case Redis is unavailable
        self.local_seen_keys = set()

        # Durable local fallback (works even if Redis is down): SQLite-backed state
        self.state_db_path = os.getenv("DEDUP_DB_PATH", os.path.join("state", "producer_state.sqlite"))
        self._sqlite_conn = None
        self._init_sqlite_state()

        # Now that SQLite state is initialized, load the last offset
        self.current_offset = self._load_last_offset()
        # Initialize distributed allocator to current_offset if enabled and not set yet
        if self.distributed_offsets and self.redis:
            try:
                exists = self.redis.exists(self.offset_alloc_key)
                if not exists:
                    # allocator represents next start offset to allocate; set to current_offset
                    self.redis.set(self.offset_alloc_key, int(self.current_offset))
            except Exception as e:
                logger.warning(f"Failed to init distributed offset allocator: {e}")
        
        # Ensure topic exists before producing (best-effort; works if credentials permit)
        self._ensure_topic_exists()

        self.producer = self._create_kafka_producer()
        signal.signal(signal.SIGINT, self.shutdown)
        
        logger.info(f"Kafka bootstrap: {self.bootstrap_servers}, topic: {self.topic}")
        logger.info(f"Pure API to Kafka Producer - Starting from offset: {self.current_offset}")
        if self.distributed_offsets:
            if not self.redis:
                logger.warning("DISTRIBUTED_OFFSET is enabled but Redis is unavailable; falling back to single-instance offset handling.")
                self.distributed_offsets = False
            else:
                logger.info("Distributed offset allocation enabled (Redis backed).")

    def reset_state(self, *, reset_offsets: bool = False, reset_dedup: bool = False, start_offset: int | None = None) -> None:
        """Reset offsets and/or dedup state on demand; optionally force a starting offset."""
        if reset_dedup:
            # Clear Redis seen and inflight
            try:
                if self.redis:
                    # Best-effort scan deletion for inflight keys
                    try:
                        cursor = 0
                        pattern = f"{self.redis_set_key}:inflight:*"
                        while True:
                            cursor, keys = self.redis.scan(cursor=cursor, match=pattern, count=500)
                            if keys:
                                self.redis.delete(*keys)
                            if cursor == 0:
                                break
                    except Exception:
                        pass
                    try:
                        self.redis.delete(self.redis_set_key)
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"Failed clearing Redis dedup: {e}")

            # Clear SQLite seen and inflight
            try:
                if self._sqlite_conn is not None:
                    self._sqlite_conn.execute("DELETE FROM seen_keys")
                    self._sqlite_conn.execute("DELETE FROM inflight_keys")
                    self._sqlite_conn.commit()
            except Exception as e:
                logger.warning(f"Failed clearing SQLite dedup: {e}")

            # Local in-memory
            self.local_seen_keys.clear()

        if reset_offsets or start_offset is not None:
            new_offset = 0 if start_offset is None else int(start_offset)
            # Redis offset
            try:
                if self.redis:
                    self.redis.set(self.offset_redis_key, str(new_offset))
            except Exception as e:
                logger.warning(f"Failed resetting Redis offset: {e}")
            # SQLite offset
            try:
                if self._sqlite_conn is not None:
                    self._sqlite_set_offset(new_offset)
            except Exception as e:
                logger.warning(f"Failed resetting SQLite offset: {e}")
            # File offset
            try:
                with open(self.offset_file, 'w') as f:
                    f.write(str(new_offset))
            except Exception as e:
                logger.warning(f"Failed resetting file offset: {e}")
            self.current_offset = new_offset
            logger.info(f"Offset reset. New starting offset: {self.current_offset}")
            if self.distributed_offsets and self.redis:
                try:
                    self.redis.set(self.offset_alloc_key, int(new_offset))
                except Exception as e:
                    logger.warning(f"Failed resetting distributed allocator offset: {e}")

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
            "client.id": "api-producer",
            # Ensure idempotent, exactly-once-in-producer semantics where possible
            "acks": "all",
            "enable.idempotence": True,
            # Improve reliability on Confluent Cloud
            "message.timeout.ms": 1200000,  # 20 minutes
            "socket.keepalive.enable": True,
            "max.in.flight.requests.per.connection": 5,
            "reconnect.backoff.ms": 100,
            "reconnect.backoff.max.ms": 10000,
            "retry.backoff.ms": 100,
            "message.send.max.retries": 2147483647,  # effectively unlimited
        }

        # Optional tunables via env (match docker-compose KAFKA_* vars if present)
        optional_mappings = {
            "KAFKA_BATCH_SIZE": "batch.size",
            "KAFKA_LINGER_MS": "linger.ms",
            "KAFKA_COMPRESSION_TYPE": "compression.type",
            "KAFKA_QUEUE_BUFFERING_MAX_MS": "queue.buffering.max.ms",
            "KAFKA_QUEUE_BUFFERING_MAX_MESSAGES": "queue.buffering.max.messages",
            "KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION": "max.in.flight.requests.per.connection",
        }
        for env_key, conf_key in optional_mappings.items():
            val = os.getenv(env_key)
            if val:
                # Cast numeric fields where applicable
                try:
                    if conf_key.endswith(".ms") or conf_key in {"batch.size", "queue.buffering.max.messages", "max.in.flight.requests.per.connection"}:
                        producer_config[conf_key] = int(val)
                    else:
                        producer_config[conf_key] = val
                except Exception:
                    producer_config[conf_key] = val

        if self.kafka_username and self.kafka_password:
            producer_config.update({
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": self.kafka_username,
                "sasl.password": self.kafka_password,
                "ssl.endpoint.identification.algorithm": "https",
                "broker.address.family": "v4",
                "request.timeout.ms": 20000,
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
        # Determine offset source (distributed allocator or local current_offset)
        api_offset = self.current_offset
        if self.distributed_offsets and self.redis:
            try:
                claimed = int(self.redis.incrby(self.offset_alloc_key, self.batch_size))
                api_offset = claimed - self.batch_size
            except Exception as e:
                logger.warning(f"Distributed offset allocation failed, using local offset: {e}")
        # Track the offset used for this batch
        self.current_offset = api_offset
        logger.info(f"Downloading from API - Offset: {api_offset}, Limit: {self.batch_size}")
        
        try:
            params = {'limit': self.batch_size, 'offset': api_offset}
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
            # Release inflight reservation so we can retry later
            try:
                key_bytes = msg.key()
                if key_bytes is not None:
                    key_str = key_bytes.decode("utf-8") if isinstance(key_bytes, (bytes, bytearray)) else str(key_bytes)
                    self._release_reservation(key_str)
            except Exception as e:
                logger.warning(f"Failed to release reservation on error: {e}")
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
            # On immediate send failure, release reservation so future attempts can retry
            try:
                self._release_reservation(key_str)
            except Exception:
                pass
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

    def _release_reservation(self, key_str: str) -> None:
        """Release inflight reservation without marking as seen (e.g., on delivery failure)."""
        try:
            if self.redis:
                inflight_key = f"{self.redis_set_key}:inflight:{key_str}"
                try:
                    self.redis.delete(inflight_key)
                except Exception:
                    pass
            if self._sqlite_conn is not None:
                try:
                    self._sqlite_conn.execute("DELETE FROM inflight_keys WHERE key = ?", (key_str,))
                    self._sqlite_conn.commit()
                except Exception:
                    pass
            # Local guard: allow retry by removing from set if present
            try:
                if key_str in self.local_seen_keys:
                    # Do not remove from seen set here; only inflight would be in a separate structure.
                    # Since local fallback uses seen set for both, we cannot distinguish reliably.
                    # Intentionally not removing from seen to avoid duplicates in local-only mode.
                    pass
            except Exception:
                pass
        except Exception:
            pass

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
                # Only advance and persist local offsets when not using distributed allocator
                if self.distributed_offsets and self.redis:
                    if delivered_now:
                        self.current_offset += delivered_now
                else:
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
                if self.distributed_offsets and self.redis:
                    if delivered_now:
                        self.current_offset += delivered_now
                else:
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
    import os
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['continuous', 'batch'], default='continuous')
    parser.add_argument('--count', type=int, default=1000)
    parser.add_argument('--reset-offsets', action='store_true', help='Reset stored offsets to 0 before starting')
    parser.add_argument('--reset-dedup', action='store_true', help='Clear dedup state (seen/inflight) before starting')
    parser.add_argument('--start-offset', type=int, default=None, help='Force a specific starting offset (overrides stored offset)')
    
    args = parser.parse_args()
    
    # Allow environment overrides for containerized runs
    def _env_truthy(name: str) -> bool:
        val = os.getenv(name)
        return str(val).lower() in {"1", "true", "yes", "on"} if val is not None else False
    env_reset_offsets = _env_truthy("RESET_OFFSETS")
    env_reset_dedup = _env_truthy("RESET_DEDUP")
    env_start_offset = os.getenv("START_OFFSET")
    env_mode = os.getenv("STREAM_MODE")
    env_count = os.getenv("STREAM_COUNT")

    producer = PureAPItoKafkaProducer()

    # Apply optional resets before streaming
    reset_offsets = args.reset_offsets or env_reset_offsets
    reset_dedup = args.reset_dedup or env_reset_dedup
    start_offset = args.start_offset if args.start_offset is not None else (int(env_start_offset) if env_start_offset is not None else None)
    if reset_offsets or reset_dedup or start_offset is not None:
        producer.reset_state(
            reset_offsets=reset_offsets,
            reset_dedup=reset_dedup,
            start_offset=start_offset
        )
    
    try:
        run_mode = env_mode if env_mode in {'continuous', 'batch'} else args.mode
        run_count = int(env_count) if env_count is not None else args.count
        if run_mode == 'batch':
            producer.stream_count(run_count)
        else:
            producer.stream_continuously()
    except Exception as e:
        logger.error(f"Failed: {e}")