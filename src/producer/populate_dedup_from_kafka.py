#!/usr/bin/env python3
"""
Script to populate Redis deduplication set from existing Kafka topic messages.
Reads all messages from the topic and adds their transaction_ids to Redis.
"""

import os
import json
import logging
from dotenv import load_dotenv

try:
    from confluent_kafka import Consumer
except ImportError:
    print("ERROR: confluent_kafka not installed. Install with: pip install confluent-kafka")
    exit(1)

try:
    import redis
except ImportError:
    print("ERROR: redis not installed. Install with: pip install redis")
    exit(1)

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


def main():
    # Get configuration from environment
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_username = os.getenv("KAFKA_USERNAME")
    kafka_password = os.getenv("KAFKA_PASSWORD")
    topic = os.getenv("KAFKA_TOPIC", "ecommerce_fraud2")
    
    # Redis configuration - use Docker Redis if REDIS_HOST not set
    redis_host = os.getenv("REDIS_HOST")
    if not redis_host:
        # Try to detect if we're in Docker or local
        # If local, try to connect to Docker Redis via host.docker.internal or localhost
        redis_host = "localhost"  # Will try localhost first, user can override
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    redis_dedup_key = f"fraud:seen_transaction_ids:{topic}"
    
    logger.info(f"Connecting to Kafka: {bootstrap_servers}, topic: {topic}")
    logger.info(f"Connecting to Redis: {redis_host}:{redis_port}, key: {redis_dedup_key}")
    
    # Connect to Redis - try multiple hosts if localhost fails
    redis_client = None
    redis_hosts_to_try = [redis_host]
    
    # If localhost failed, try Docker hostnames
    if redis_host == "localhost":
        redis_hosts_to_try.extend(["host.docker.internal", "127.0.0.1"])
    
    for host in redis_hosts_to_try:
        try:
            logger.info(f"Trying to connect to Redis at {host}:{redis_port}...")
            redis_client = redis.Redis(
                host=host,
                port=redis_port,
                db=redis_db,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            redis_client.ping()
            logger.info(f"✅ Connected to Redis at {host}:{redis_port}")
            break
        except Exception as e:
            logger.warning(f"Failed to connect to {host}:{e}")
            redis_client = None
    
    if redis_client is None:
        logger.error(f"❌ Failed to connect to Redis at any host. Tried: {redis_hosts_to_try}")
        logger.error("💡 Tip: If running locally, make sure Redis is running or use: REDIS_HOST=host.docker.internal")
        return 1
    
    # Configure Kafka consumer
    consumer_config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": "dedup-populator",
        "auto.offset.reset": "earliest",  # Read from beginning
        "enable.auto.commit": False,  # Manual commit
    }
    
    if kafka_username and kafka_password:
        consumer_config.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": kafka_username,
            "sasl.password": kafka_password,
            "ssl.endpoint.identification.algorithm": "https",
        })
    
    # Create consumer
    try:
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])
        logger.info(f"✅ Subscribed to topic: {topic}")
    except Exception as e:
        logger.error(f"❌ Failed to create consumer: {e}")
        return 1
    
    # Read all messages and collect transaction IDs
    transaction_ids = set()
    message_count = 0
    batch_size = 1000  # Add to Redis in batches
    
    try:
        logger.info("📖 Reading messages from Kafka...")
        timeout_count = 0
        max_timeouts = 3  # Stop after 3 consecutive timeouts
        
        while True:
            msg = consumer.poll(timeout=5.0)  # 5 second timeout
            
            if msg is None:
                timeout_count += 1
                if timeout_count >= max_timeouts:
                    logger.info(f"No more messages (timeout {timeout_count}/{max_timeouts})")
                    break
                continue
            
            timeout_count = 0  # Reset on successful read
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            try:
                # Parse message value
                value = msg.value()
                if isinstance(value, bytes):
                    value = value.decode('utf-8')
                
                data = json.loads(value)
                transaction_id = data.get('transaction_id')
                
                if transaction_id:
                    transaction_ids.add(str(transaction_id))
                    message_count += 1
                    
                    # Batch add to Redis every 1000 IDs
                    if len(transaction_ids) >= batch_size:
                        redis_client.sadd(redis_dedup_key, *transaction_ids)
                        logger.info(f"✅ Processed {message_count} messages, added {len(transaction_ids)} unique transaction IDs to Redis")
                        transaction_ids.clear()
                    
                    if message_count % 10000 == 0:
                        logger.info(f"📊 Processed {message_count} messages so far...")
                
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse message: {e}")
                continue
            except Exception as e:
                logger.warning(f"Error processing message: {e}")
                continue
        
        # Add remaining transaction IDs
        if transaction_ids:
            redis_client.sadd(redis_dedup_key, *transaction_ids)
            logger.info(f"✅ Added final batch of {len(transaction_ids)} transaction IDs")
        
        # Get final count
        final_count = redis_client.scard(redis_dedup_key)
        
        logger.info("=" * 80)
        logger.info(f"✅ COMPLETE")
        logger.info(f"   Messages processed: {message_count}")
        logger.info(f"   Unique transaction IDs in Redis: {final_count}")
        logger.info(f"   Redis key: {redis_dedup_key}")
        logger.info("=" * 80)
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        # Add any remaining IDs before exiting
        if transaction_ids:
            redis_client.sadd(redis_dedup_key, *transaction_ids)
    except Exception as e:
        logger.error(f"Error reading from Kafka: {e}")
        return 1
    finally:
        consumer.close()
    
    return 0


if __name__ == "__main__":
    exit(main())

