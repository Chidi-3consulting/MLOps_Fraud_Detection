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
        self.topic = env_topic if env_topic else "ecommerce_fraud"
        
        # Adaptive batch sizing configuration
        self.adaptive_batching = str(os.getenv("ADAPTIVE_BATCHING", "true")).lower() in {"1", "true", "yes", "on"}
        initial_batch_size = int(os.getenv("BATCH_SIZE", "20000"))
        self.batch_size = initial_batch_size
        self.min_batch_size = int(os.getenv("MIN_BATCH_SIZE", "1000"))
        self.max_batch_size = int(os.getenv("MAX_BATCH_SIZE", "50000"))
        
        # Adaptive batching state
        self.batch_size_history = [initial_batch_size]  # Keep last N batch sizes
        self.response_times = []  # Track API response times
        self.error_count = 0  # Track consecutive errors
        self.success_count = 0  # Track consecutive successes
        self.adaptive_window_size = int(os.getenv("ADAPTIVE_WINDOW_SIZE", "10"))  # Number of batches to consider
        self.target_response_time = float(os.getenv("TARGET_RESPONSE_TIME", "5.0"))  # Target API response time in seconds
        self.max_response_time = float(os.getenv("MAX_RESPONSE_TIME", "30.0"))  # Max acceptable response time
        
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
        # Make SQLite path unique per replica to avoid locking issues
        import socket
        hostname = socket.gethostname()
        default_db_name = f"producer_state_{hostname}.sqlite"
        default_db_path = os.path.join("state", default_db_name)
        self.state_db_path = os.getenv("DEDUP_DB_PATH", default_db_path)
        self._sqlite_conn = None
        # Only initialize SQLite if not using distributed offsets (to avoid conflicts)
        # Or if explicitly enabled via env var
        use_sqlite_fallback = str(os.getenv("USE_SQLITE_FALLBACK", "true")).lower() in {"1", "true", "yes", "on"}
        if not (self.distributed_offsets and self.redis) or use_sqlite_fallback:
            self._init_sqlite_state()
        else:
            logger.info("SQLite fallback disabled - using Redis-only for distributed offsets")

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
        logger.info(f"Batch size: {self.batch_size} (min: {self.min_batch_size}, max: {self.max_batch_size})")
        if self.adaptive_batching:
            logger.info(f"✅ Adaptive batching enabled (target response time: {self.target_response_time}s, max: {self.max_response_time}s)")
        else:
            logger.info("Adaptive batching disabled - using fixed batch size")
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

    def _adjust_batch_size(self, response_time: float, success: bool):
        """Adaptively adjust batch size based on performance metrics."""
        if not self.adaptive_batching:
            return
        
        # Track response time
        self.response_times.append(response_time)
        if len(self.response_times) > self.adaptive_window_size:
            self.response_times.pop(0)
        
        # Track success/failure
        if success:
            self.success_count += 1
            self.error_count = 0
        else:
            self.error_count += 1
            self.success_count = 0
        
        # Calculate average response time
        avg_response_time = sum(self.response_times) / len(self.response_times) if self.response_times else response_time
        
        # Adjust batch size based on performance
        old_batch_size = self.batch_size
        adjustment_factor = 1.0
        
        # If we have errors, reduce batch size aggressively
        if self.error_count >= 2:
            adjustment_factor = 0.5  # Cut in half
            logger.warning(f"Multiple errors detected ({self.error_count}), reducing batch size")
        # If response time is too high, reduce batch size
        elif avg_response_time > self.max_response_time:
            # Reduce proportionally to how much we're over
            overage_ratio = avg_response_time / self.max_response_time
            adjustment_factor = 1.0 / overage_ratio
            logger.info(f"Response time too high ({avg_response_time:.2f}s > {self.max_response_time}s), reducing batch size")
        # If response time is good and we have successes, increase batch size
        elif avg_response_time < self.target_response_time and self.success_count >= 3:
            # Increase by 20% if we're doing well
            adjustment_factor = 1.2
            logger.info(f"Performance good (avg {avg_response_time:.2f}s < {self.target_response_time}s), increasing batch size")
        # If response time is in target range, keep it stable
        else:
            # Small adjustments to fine-tune
            if avg_response_time > self.target_response_time * 1.2:
                adjustment_factor = 0.9  # Slight decrease
            elif avg_response_time < self.target_response_time * 0.8:
                adjustment_factor = 1.1  # Slight increase
        
        # Calculate new batch size
        new_batch_size = int(self.batch_size * adjustment_factor)
        
        # Clamp to min/max bounds
        new_batch_size = max(self.min_batch_size, min(self.max_batch_size, new_batch_size))
        
        # Only log if there's a significant change (more than 10%)
        if abs(new_batch_size - self.batch_size) > self.batch_size * 0.1:
            self.batch_size = new_batch_size
            self.batch_size_history.append(self.batch_size)
            if len(self.batch_size_history) > self.adaptive_window_size:
                self.batch_size_history.pop(0)
            logger.info(f"📊 Batch size adjusted: {old_batch_size} → {self.batch_size} (avg response: {avg_response_time:.2f}s, errors: {self.error_count}, successes: {self.success_count})")
        else:
            self.batch_size = new_batch_size

    def download_batch(self) -> Optional[tuple]:
        """Download batch from API using current offset
        Returns: (transactions_list, api_offset_used, used_distributed) or None
        """
        # Determine offset source (distributed allocator or local current_offset)
        api_offset = self.current_offset
        used_distributed = False
        if self.distributed_offsets and self.redis:
            try:
                # Atomically allocate a range of offsets to prevent duplicates across replicas
                claimed = int(self.redis.incrby(self.offset_alloc_key, self.batch_size))
                api_offset = claimed - self.batch_size
                used_distributed = True
                logger.debug(f"Allocated offset range {api_offset} to {claimed - 1} from distributed allocator")
            except Exception as e:
                logger.warning(f"Distributed offset allocation failed, falling back to local offset: {e}")
                # Fall back to local offset tracking
                used_distributed = False
        
        logger.info(f"Downloading from API - Offset: {api_offset}, Limit: {self.batch_size}")
        
        start_time = time.time()
        try:
            # Dynamic timeout based on batch size (at least 2s per 1000 records, minimum 30s)
            timeout = max(30, int(self.batch_size / 1000 * 2))
            params = {'limit': self.batch_size, 'offset': api_offset}
            response = requests.get(self.api_url, params=params, timeout=timeout)
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('data'):
                    transactions = data['data']
                    logger.info(f"✅ Received {len(transactions)} transactions from API (offset {api_offset}, response time: {response_time:.2f}s)")
                    
                    # Adjust batch size based on performance
                    self._adjust_batch_size(response_time, success=True)
                    
                    # Return transactions, offset used, and whether distributed allocation was used
                    return (transactions, api_offset, used_distributed)
                else:
                    logger.warning(f"API returned no data or error: {data.get('message', 'Unknown error')}")
                    # Adjust batch size (treat as failure)
                    self._adjust_batch_size(response_time, success=False)
                    # If we allocated distributed offsets but got no data, we still consumed that range
                    # This is intentional to prevent duplicates, but log it
                    if used_distributed:
                        logger.info(f"Note: Allocated {self.batch_size} offsets but received 0 records. Range {api_offset}-{api_offset + self.batch_size - 1} reserved.")
                    return None
            else:
                logger.error(f"HTTP Error: {response.status_code}")
                response_time = time.time() - start_time
                # Adjust batch size (treat as failure)
                self._adjust_batch_size(response_time, success=False)
                # If we allocated distributed offsets but request failed, the range is still consumed
                # This prevents duplicates but means we skip that range on retry
                if used_distributed:
                    logger.warning(f"Request failed after allocating offset range. Range {api_offset}-{api_offset + self.batch_size - 1} was reserved.")
                return None
                
        except requests.Timeout:
            response_time = time.time() - start_time
            logger.error(f"API request timed out after {response_time:.2f}s (batch size: {self.batch_size})")
            # Timeout is a clear signal to reduce batch size
            self._adjust_batch_size(response_time, success=False)
            if used_distributed:
                logger.warning(f"Timeout after allocating offset range. Range {api_offset}-{api_offset + self.batch_size - 1} was reserved.")
            return None
        except Exception as e:
            response_time = time.time() - start_time
            logger.error(f"API download failed: {e}")
            self._adjust_batch_size(response_time, success=False)
            if used_distributed:
                logger.warning(f"Exception after allocating offset range. Range {api_offset}-{api_offset + self.batch_size - 1} was reserved.")
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
                except sqlite3.OperationalError:
                    # Database locked - skip SQLite check, rely on Redis
                    pass
                except Exception:
                    pass
                # Check seen
                try:
                    cur = self._sqlite_conn.execute("SELECT 1 FROM seen_keys WHERE key = ? LIMIT 1", (key_str,))
                    if cur.fetchone():
                        return False
                except sqlite3.OperationalError:
                    # Database locked - skip SQLite check, rely on Redis
                    pass
                # Try reserve inflight
                expires_at = now + self.redis_inflight_ttl
                try:
                    self._sqlite_conn.execute("INSERT INTO inflight_keys(key, expires_at) VALUES(?, ?)", (key_str, expires_at))
                    self._sqlite_conn.commit()
                    return True
                except sqlite3.IntegrityError:
                    return False
                except sqlite3.OperationalError:
                    # Database locked - skip SQLite, rely on Redis or local memory
                    pass
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
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e).lower():
                        # Silently ignore lock errors - Redis is handling deduplication anyway
                        pass
                    else:
                        logger.warning(f"SQLite finalize failed: {e}")
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
            # Use WAL mode and set timeout for better concurrency handling
            # timeout=5.0 means wait up to 5 seconds for locks
            self._sqlite_conn = sqlite3.connect(
                self.state_db_path, 
                check_same_thread=False,
                timeout=5.0  # Wait up to 5 seconds for locks
            )
            self._sqlite_conn.execute("PRAGMA journal_mode=WAL;")
            self._sqlite_conn.execute("PRAGMA synchronous=NORMAL;")
            self._sqlite_conn.execute("PRAGMA busy_timeout=5000;")  # 5 second busy timeout
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
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                logger.warning(f"SQLite database is locked (another replica may be using it). This is normal with multiple replicas. Using Redis-only mode.")
            else:
                logger.warning(f"SQLite operational error: {e}")
            self._sqlite_conn = None
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
        """Initialize Redis client if configured via environment.
        Retries connection with exponential backoff for better reliability.
        """
        if self._redis_lib is None:
            logger.warning("Redis library not available. Install 'redis' package for distributed deduplication.")
            return None
        redis_url = os.getenv("REDIS_URL")
        host = os.getenv("REDIS_HOST")
        if not redis_url and not host:
            logger.info("Redis not configured (no REDIS_URL or REDIS_HOST). Using SQLite fallback for deduplication.")
            return None
        
        # Retry connection with exponential backoff
        max_retries = 5
        for attempt in range(max_retries):
            try:
                if redis_url:
                    client = self._redis_lib.Redis.from_url(
                        redis_url, 
                        decode_responses=True,
                        socket_connect_timeout=5,
                        socket_timeout=5,
                        retry_on_timeout=True
                    )
                else:
                    port = int(os.getenv("REDIS_PORT", "6379"))
                    password = os.getenv("REDIS_PASSWORD")
                    db = int(os.getenv("REDIS_DB", "0"))
                    client = self._redis_lib.Redis(
                        host=host, 
                        port=port, 
                        password=password, 
                        db=db, 
                        decode_responses=True,
                        socket_connect_timeout=5,
                        socket_timeout=5,
                        retry_on_timeout=True
                    )
                # Test connection
                client.ping()
                logger.info(f"✅ Connected to Redis at {host or 'URL'} for deduplication and distributed offsets.")
                return client
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s, 8s
                    logger.warning(f"Redis connection attempt {attempt + 1}/{max_retries} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"Redis not available after {max_retries} attempts: {e}. Falling back to SQLite for deduplication.")
                    return None
        return None

    def stream_continuously(self):
        """Stream data continuously - each run continues from last offset"""
        self.running = True
        logger.info("🚀 Starting continuous streaming from API...")
        
        try:
            while self.running:
                result = self.download_batch()
                
                if not result:
                    logger.info("No more data from API, waiting...")
                    time.sleep(10)
                    continue
                
                # Unpack result: (transactions, api_offset_used, used_distributed)
                if len(result) == 3:
                    transactions, api_offset_used, used_distributed = result
                else:
                    # Backward compatibility
                    transactions, api_offset_used = result
                    used_distributed = False
                
                if not transactions:
                    logger.info("Empty batch received, waiting...")
                    time.sleep(10)
                    continue
                
                # Send all transactions
                self._batch_delivered = 0
                sent_count = 0
                batch_len = len(transactions)
                # Poll more frequently for large batches to avoid buffer overflow
                poll_interval = max(500, batch_len // 50)  # Poll ~50 times per batch
                
                for idx, transaction in enumerate(transactions):
                    if self.send_to_kafka(transaction):
                        sent_count += 1
                    # Poll periodically to avoid buffer overflow and trigger callbacks
                    if (idx + 1) % poll_interval == 0:
                        self.producer.poll(0)
                
                # Final poll to process any remaining messages
                self.producer.poll(0)

                # Wait for delivery callbacks with longer timeout for large batches
                flush_timeout = max(30, batch_len // 1000)  # At least 30s, or 1s per 1000 records
                self.producer.flush(flush_timeout)
                delivered_now = self._batch_delivered
                self.total_sent += delivered_now
                
                # Update offset: next offset = api_offset_used + number of transactions actually received
                # This ensures we don't skip or duplicate records
                next_offset = api_offset_used + len(transactions)
                
                if used_distributed and self.distributed_offsets and self.redis:
                    # With distributed offsets, the allocator already advanced by batch_size atomically
                    # We track the actual progress for logging, but the allocator manages the next range
                    self.current_offset = next_offset
                    # Note: We don't save offset when using distributed allocator
                    # as it's managed centrally in Redis. The allocator ensures no duplicates.
                else:
                    # Update and save local offset (fallback mode or non-distributed)
                    self.current_offset = next_offset
                    self._save_last_offset(self.current_offset)
                
                logger.info(f"📦 Sent batch - Received: {len(transactions)}, Delivered: {delivered_now}, Skipped (duplicates): {self.dedup_skipped}, Total: {self.total_sent}, Next offset: {self.current_offset}")
                # Reset dedup counter for next batch
                self.dedup_skipped = 0
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
                result = self.download_batch()
                
                if not result:
                    logger.info("No more data available")
                    break
                
                # Unpack result: (transactions, api_offset_used, used_distributed)
                if len(result) == 3:
                    transactions, api_offset_used, used_distributed = result
                else:
                    # Backward compatibility
                    transactions, api_offset_used = result
                    used_distributed = False
                
                if not transactions:
                    logger.info("Empty batch received")
                    break
                
                self._batch_delivered = 0
                sent_count = 0
                batch_len = len(transactions)
                # Poll more frequently for large batches to avoid buffer overflow
                poll_interval = max(500, batch_len // 50)  # Poll ~50 times per batch
                
                for idx, transaction in enumerate(transactions):
                    if self.total_sent >= count:
                        break
                    if self.send_to_kafka(transaction):
                        sent_count += 1
                    # Poll periodically to avoid buffer overflow and trigger callbacks
                    if (idx + 1) % poll_interval == 0:
                        self.producer.poll(0)
                
                # Final poll to process any remaining messages
                self.producer.poll(0)

                # Wait for delivery callbacks with longer timeout for large batches
                flush_timeout = max(30, batch_len // 1000)  # At least 30s, or 1s per 1000 records
                self.producer.flush(flush_timeout)
                delivered_now = self._batch_delivered
                self.total_sent += delivered_now
                
                # Update offset: next offset = api_offset_used + number of transactions actually received
                next_offset = api_offset_used + len(transactions)
                
                if used_distributed and self.distributed_offsets and self.redis:
                    # With distributed offsets, allocator manages the next range
                    self.current_offset = next_offset
                else:
                    # Update and save local offset
                    self.current_offset = next_offset
                    self._save_last_offset(self.current_offset)
                
                logger.info(f"Progress: {self.total_sent}/{count}, Received: {len(transactions)}, Delivered: {delivered_now}, Next offset: {self.current_offset}")
            
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