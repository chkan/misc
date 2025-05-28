import os
import uuid
import time
import asyncio
import aiofiles
import aioboto3
from tqdm.asyncio import tqdm_asyncio

# Configuration
BUCKET_NAME = 'your-bucket-name'  # Replace with your bucket
REGION = 'us-east-1'
FILE_SIZE_MB = 100
CONCURRENT_TRANSFERS = 5
TEST_KEY_PREFIX = f'test-s3-speed-{uuid.uuid4()}'
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

# File paths
temp_dir = "/tmp"
upload_files = [os.path.join(temp_dir, f"upload_{i}.bin") for i in range(CONCURRENT_TRANSFERS)]
download_files = [os.path.join(temp_dir, f"download_{i}.bin") for i in range(CONCURRENT_TRANSFERS)]


def generate_test_file(path, size_mb):
    with open(path, 'wb') as f:
        f.write(os.urandom(size_mb * 1024 * 1024))


async def upload_file(s3, index):
    key = f"{TEST_KEY_PREFIX}/file_{index}.bin"
    file_path = upload_files[index]
    file_size = os.path.getsize(file_path)
    async with aiofiles.open(file_path, 'rb') as f:
        await s3.upload_file(file_path, BUCKET_NAME, key)
    return key


async def download_file(s3, index, key):
    file_path = download_files[index]
    await s3.download_file(BUCKET_NAME, key, file_path)


async def run_speed_test():
    for path in upload_files:
        generate_test_file(path, FILE_SIZE_MB)

    session = aioboto3.Session(region_name=REGION)
    async with session.client('s3') as s3:
        print("Uploading...")
        start_upload = time.time()
        upload_keys = await tqdm_asyncio.gather(*(upload_file(s3, i) for i in range(CONCURRENT_TRANSFERS)))
        end_upload = time.time()

        print("Downloading...")
        start_download = time.time()
        await tqdm_asyncio.gather(*(download_file(s3, i, upload_keys[i]) for i in range(CONCURRENT_TRANSFERS)))
        end_download = time.time()

        total_mb = FILE_SIZE_MB * CONCURRENT_TRANSFERS
        upload_speed = total_mb / (end_upload - start_upload)
        download_speed = total_mb / (end_download - start_download)

        print(f"Upload Time: {(end_upload - start_upload) * 1000:.0f} ms")
        print(f"Upload Speed: {upload_speed:.2f} MB/s")
        print(f"Download Time: {(end_download - start_download) * 1000:.0f} ms")
        print(f"Download Speed: {download_speed:.2f} MB/s")

        print("Cleaning up...")
        for key in upload_keys:
            await s3.delete_object(Bucket=BUCKET_NAME, Key=key)
        for path in upload_files + download_files:
            if os.path.exists(path):
                os.remove(path)

if __name__ == '__main__':
    asyncio.run(run_speed_test())
