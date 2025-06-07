import asyncio
import argparse
import ssl
import sys
import threading
import time
import logging
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from concurrent.futures import ThreadPoolExecutor
from cachetools import TTLCache
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

key_cache = TTLCache(maxsize=100000, ttl=3600)
key_cache_lock = threading.Lock()
processed_offsets = defaultdict(int)
processed_offsets_lock = threading.Lock()

shutdown_event = asyncio.Event()


def decrypt_message(data: bytes, key_id: str) -> str:
    with key_cache_lock:
        key = key_cache.get(key_id)

    if key is None:
        raise KeyError(f"Missing decryption key for: {key_id}")

    if b"fail" in data:
        raise ValueError("Simulated decryption error")

    time.sleep(0.5)
    return data.decode("utf-8").upper()


def handle_message(data: bytes, offset: int, partition: int, key_id: str, topic: str):
    thread_id = threading.get_ident()
    try:
        result = decrypt_message(data, key_id)
        log.info(f"[Thread {thread_id}] Decrypted (offset {offset}): {result}")
        with processed_offsets_lock:
            tp = TopicPartition(topic, partition)
            processed_offsets[tp] = max(processed_offsets[tp], offset)
    except Exception as e:
        log.error(f"[Thread {thread_id}] Error processing offset {offset}: {e}", exc_info=False)


async def periodic_committer(consumer, interval=5):
    while not shutdown_event.is_set():
        await asyncio.sleep(interval)
        with processed_offsets_lock:
            if processed_offsets:
                await consumer.commit(processed_offsets.copy())
                log.info(f"Committed offsets: {processed_offsets}")
    log.info("Shutting down periodic committer")


async def preload_key_cache(bootstrap_servers: str, key_topic: str):
    consumer = AIOKafkaConsumer(
        key_topic,
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        auto_offset_reset="earliest"
    )

    await consumer.start()
    log.info(f"Preloading key cache from topic '{key_topic}'")
    try:
        async for msg in consumer:
            key = msg.key.decode()
            with key_cache_lock:
                if msg.value is None:
                    key_cache.pop(key, None)
                    log.info(f"Key '{key}' removed from cache")
                else:
                    key_cache[key] = msg.value
                    log.info(f"Key '{key}' updated in cache")
    finally:
        await consumer.stop()


async def message_producer(consumer, queue: asyncio.Queue):
    while not shutdown_event.is_set():
        results = await consumer.getmany(timeout_ms=500, max_records=100)
        for tp, messages in results.items():
            for msg in messages:
                await queue.put(msg)
    log.info("Shutting down message producer")


async def message_worker(queue: asyncio.Queue, executor, topic: str):
    loop = asyncio.get_running_loop()
    while not shutdown_event.is_set():
        try:
            msg = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        key_id = msg.key.decode() if msg.key else "default"
        loop.run_in_executor(
            executor,
            handle_message,
            msg.value,
            msg.offset,
            msg.partition,
            key_id,
            topic
        )
        queue.task_done()
    log.info("Shutting down message worker")


def handle_shutdown():
    log.info("Shutdown signal received, initiating graceful shutdown...")
    shutdown_event.set()


async def consume(loop, args):
    ssl_context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH,
        cafile=args.ca_cert
    )
    ssl_context.load_cert_chain(certfile=args.client_cert, keyfile=args.client_key)

    await preload_key_cache(args.bootstrap_servers, args.key_topic)

    consumer = AIOKafkaConsumer(
        args.data_topic,
        loop=loop,
        bootstrap_servers=args.bootstrap_servers,
        security_protocol="SSL",
        ssl_context=ssl_context,
        group_id="aiokafka-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        fetch_min_bytes=1048576,
        fetch_max_wait_ms=500
    )

    await consumer.start()
    log.info(f"Started consumer on topic '{args.data_topic}'")

    queue = asyncio.Queue(maxsize=1000)
    executor = ThreadPoolExecutor(max_workers=args.threads)

    loop.add_signal_handler(signal.SIGINT, handle_shutdown)
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown)

    try:
        await asyncio.gather(
            message_producer(consumer, queue),
            message_worker(queue, executor, args.data_topic),
            periodic_committer(consumer, interval=5)
        )
    except asyncio.CancelledError:
        pass
    finally:
        await queue.join()
        await consumer.stop()
        executor.shutdown(wait=True)
        log.info("Shutdown complete")


def parse_args():
    parser = argparse.ArgumentParser(description="aiokafka optimized mTLS consumer with graceful shutdown")
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--data-topic", required=True)
    parser.add_argument("--key-topic", required=True)
    parser.add_argument("--ca-cert", required=True)
    parser.add_argument("--client-cert", required=True)
    parser.add_argument("--client-key", required=True)
    parser.add_argument("--threads", type=int, default=4)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(consume(asyncio.get_event_loop(), args))
