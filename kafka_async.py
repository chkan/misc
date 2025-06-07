import asyncio
import argparse
import ssl
import sys
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from concurrent.futures import ThreadPoolExecutor
import threading
import time


def decrypt_message(data: bytes) -> str:
    # Simulate a decryption failure for some payloads (example logic)
    if b"fail" in data:
        raise ValueError("Simulated decryption error")
    time.sleep(0.5)
    return data.decode("utf-8").upper()


def handle_message(data: bytes, offset: int, partition: int):
    thread_id = threading.get_ident()
    try:
        result = decrypt_message(data)
        print(f"[Thread {thread_id}] Decrypted (offset {offset}): {result}")
        return True, offset, partition
    except Exception as e:
        print(f"[Thread {thread_id}] Error processing offset {offset}: {e}", file=sys.stderr)
        return False, offset, partition


async def consume(loop, args):
    # Setup mTLS SSL context
    ssl_context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH,
        cafile=args.ca_cert
    )
    ssl_context.load_cert_chain(certfile=args.client_cert, keyfile=args.client_key)

    consumer = AIOKafkaConsumer(
        args.topic,
        loop=loop,
        bootstrap_servers=args.bootstrap_servers,
        security_protocol="SSL",
        ssl_context=ssl_context,
        group_id="aiokafka-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False  # Manual commit
    )

    await consumer.start()
    print(f"Started consumer on topic '{args.topic}'")
    try:
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            while True:
                msg = await consumer.getone()

                # Submit processing to thread pool
                future = loop.run_in_executor(
                    executor,
                    handle_message,
                    msg.value,
                    msg.offset,
                    msg.partition
                )

                # Wait for processing to complete
                success, offset, partition = await future

                # Commit only if processing succeeded
                if success:
                    tp = TopicPartition(args.topic, partition)
                    await consumer.commit({tp: offset + 1})
                    print(f"[Offset {offset}] Commit successful")
                else:
                    print(f"[Offset {offset}] Skipped commit due to error", file=sys.stderr)

    finally:
        await consumer.stop()


def parse_args():
    parser = argparse.ArgumentParser(description="aiokafka mTLS consumer with error handling")
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--ca-cert", required=True)
    parser.add_argument("--client-cert", required=True)
    parser.add_argument("--client-key", required=True)
    parser.add_argument("--threads", type=int, default=4)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(consume(loop, args))
