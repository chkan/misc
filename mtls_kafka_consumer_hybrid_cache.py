import asyncio
import argparse
import ssl
import sys
import threading
import time
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from concurrent.futures import ThreadPoolExecutor
from cachetools import TTLCache

# Thread-safe TTL cache for decryption keys
key_cache = TTLCache(maxsize=100000, ttl=3600)
key_cache_lock = threading.Lock()


def decrypt_message(data: bytes, key_id: str) -> str:
    with key_cache_lock:
        key = key_cache.get(key_id)

    if key is None:
        raise KeyError(f"Missing decryption key for: {key_id}")

    # Simulated decryption (replace with real logic)
    if b"fail" in data:
        raise ValueError("Simulated decryption error")

    time.sleep(0.5)
    return data.decode("utf-8").upper()


def handle_message(data: bytes, offset: int, partition: int, key_id: str):
    thread_id = threading.get_ident()
    try:
        result = decrypt_message(data, key_id)
        print(f"[Thread {thread_id}] Decrypted (offset {offset}): {result}")
        return True, offset, partition
    except Exception as e:
        print(f"[Thread {thread_id}] Error processing offset {offset}: {e}", file=sys.stderr)
        return False, offset, partition


async def preload_key_cache(bootstrap_servers: str, key_topic: str):
    consumer = AIOKafkaConsumer(
        key_topic,
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        auto_offset_reset="earliest"
    )

    await consumer.start()
    print(f"Preloading key cache from topic '{key_topic}'")
    try:
        async for msg in consumer:
            key = msg.key.decode()
            with key_cache_lock:
                if msg.value is None:
                    key_cache.pop(key, None)
                    print(f"Key '{key}' removed from cache")
                else:
                    key_cache[key] = msg.value
                    print(f"Key '{key}' updated in cache")
    finally:
        await consumer.stop()


async def consume(loop, args):
    # Setup SSL context for mTLS
    ssl_context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH,
        cafile=args.ca_cert
    )
    ssl_context.load_cert_chain(certfile=args.client_cert, keyfile=args.client_key)

    # Preload key cache from compacted topic
    await preload_key_cache(args.bootstrap_servers, args.key_topic)

    consumer = AIOKafkaConsumer(
        args.data_topic,
        loop=loop,
        bootstrap_servers=args.bootstrap_servers,
        security_protocol="SSL",
        ssl_context=ssl_context,
        group_id="aiokafka-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )

    await consumer.start()
    print(f"Started consumer on topic '{args.data_topic}'")
    try:
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            while True:
                msg = await consumer.getone()
                key_id = msg.key.decode() if msg.key else "default"

                future = loop.run_in_executor(
                    executor,
                    handle_message,
                    msg.value,
                    msg.offset,
                    msg.partition,
                    key_id
                )

                success, offset, partition = await future
                if success:
                    tp = TopicPartition(args.data_topic, partition)
                    await consumer.commit({tp: offset + 1})
                    print(f"[Offset {offset}] Commit successful")
                else:
                    print(f"[Offset {offset}] Skipped commit due to error", file=sys.stderr)

    finally:
        await consumer.stop()


def parse_args():
    parser = argparse.ArgumentParser(description="aiokafka mTLS consumer with hybrid key cache")
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--data-topic", required=True, help="Kafka topic for encrypted data")
    parser.add_argument("--key-topic", required=True, help="Kafka compacted topic with decryption keys")
    parser.add_argument("--ca-cert", required=True)
    parser.add_argument("--client-cert", required=True)
    parser.add_argument("--client-key", required=True)
    parser.add_argument("--threads", type=int, default=4)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(consume(asyncio.get_event_loop(), args))
