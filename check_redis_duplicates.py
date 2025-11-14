#!/usr/bin/env python3
"""Check if Redis has duplicate transaction IDs (it shouldn't, it's a SET)"""
import os
from dotenv import load_dotenv

load_dotenv()

try:
    import redis
except ImportError:
    print("ERROR: redis not installed")
    exit(1)

redis_host = os.getenv("REDIS_HOST", "redis")
redis_port = int(os.getenv("REDIS_PORT", "6379"))
redis_db = int(os.getenv("REDIS_DB", "0"))
topic = os.getenv("KAFKA_TOPIC", "ecommerce_fraud2")
redis_key = f"fraud:seen_transaction_ids:{topic}"

print("=" * 80)
print("🔍 CHECKING REDIS FOR DUPLICATES")
print("=" * 80)

try:
    # Try connecting to Redis in Docker
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True, socket_connect_timeout=5)
    redis_client.ping()
    print(f"✅ Connected to Redis at {redis_host}:{redis_port}")
    
    # Get count
    count = redis_client.scard(redis_key)
    print(f"📊 Total transaction IDs in Redis: {count:,}")
    
    # Get all IDs and check for duplicates (shouldn't happen with SET, but let's verify)
    print("📖 Fetching all transaction IDs from Redis...")
    all_ids = list(redis_client.smembers(redis_key))
    
    print(f"✅ Fetched {len(all_ids):,} transaction IDs")
    
    # Check for duplicates in the list (shouldn't happen, but verify)
    unique_ids = set(all_ids)
    if len(all_ids) != len(unique_ids):
        print(f"❌ DUPLICATES FOUND! {len(all_ids) - len(unique_ids)} duplicate IDs in the list")
    else:
        print(f"✅ No duplicates in the list (as expected for Redis SET)")
    
    # Check if any IDs are None or empty
    empty_ids = [id for id in all_ids if not id or id.strip() == ""]
    if empty_ids:
        print(f"⚠️ WARNING: {len(empty_ids)} empty or None transaction IDs found")
    
    print("=" * 80)
    print(f"📈 SUMMARY:")
    print(f"   Redis SET count: {count:,}")
    print(f"   List length: {len(all_ids):,}")
    print(f"   Unique in list: {len(unique_ids):,}")
    print(f"   Empty/None IDs: {len(empty_ids):,}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    print(f"   Trying to connect to Redis at {redis_host}:{redis_port}")
    print(f"   Make sure Redis is running and accessible")

