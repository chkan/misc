import os
import uuid
import time
import asyncio
import aiofiles
import aioboto3
from tqdm.asyncio import tqdm_asyncio

# Configuration
BUCKET_NAME = 'your-bucket-name'  # Replace with your bucket name
REGION = 'us-east-1'
FILE_SIZE_MB = 100
CONCURRENT_TRANSFERS = 2
PART_SIZE = 8 * 1024 * 1024  # 8 MB
TEST_KEY_PREFIX = f'test-s3-speed-{uuid.uuid4()}'

# File paths
temp_dir = "/tmp"
upload_files = [os.path.join(temp_dir, f"upload_{i}.bin") for i in range(CONCURRENT_TRANSFERS)]
download_files = [os.path.join(temp_dir, f"download_{i}.bin") for i in range(CONCURRENT_TRANSFERS)]

def generate_test_file(path, size_mb):
    with open(path, 'wb') as f:
        f.write(os.urandom(size_mb * 1024 * 1024))

async def multipart_upload(s3, file_path, key):
    file_size = os.path.getsize(file_path)
    mpu = await s3.create_multipart_upload(Bucket=BUCKET_NAME, Key=key)
    upload_id = mpu['UploadId']
    parts = []
    try:
        async with aiofiles.open(file_path, 'rb') as f:
            part_number = 1
            while True:
                data = await f.read(PART_SIZE)
                if not data:
                    break
                response = await s3.upload_part(
                    Bucket=BUCKET_NAME,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data
                )
                parts.append({'PartNumber': part_number, 'ETag': response['ETag']})
                part_number += 1
        await s3.complete_multipart_upload(
            Bucket=BUCKET_NAME,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
    except Exception:
        await s3.abort_multipart_upload(Bucket=BUCKET_NAME, Key=key, UploadId=upload_id)
        raise

async def multipart_download(s3, key, file_path):
    head = await s3.head_object(Bucket=BUCKET_NAME, Key=key)
    file_size = head['ContentLength']
    num_parts = (file_size + PART_SIZE - 1) // PART_SIZE

    async with aiofiles.open(file_path, 'wb') as f:
        for i in range(num_parts):
            range_header = f'bytes={i*PART_SIZE}-{min((i+1)*PART_SIZE-1, file_size-1)}'
            response = await s3.get_object(Bucket=BUCKET_NAME, Key=key, Range=range_header)
            data = await response['Body'].read()
            await f.write(data)

async def run_speed_test():
    for path in upload_files:
        generate_test_file(path, FILE_SIZE_MB)

    session = aioboto3.Session(region_name=REGION)
    async with session.client('s3') as s3:
        print("Uploading with multipart...")
        start_upload = time.time()
        upload_tasks = [multipart_upload(s3, upload_files[i], f"{TEST_KEY_PREFIX}/file_{i}.bin") for i in range(CONCURRENT_TRANSFERS)]
        await tqdm_asyncio.gather(*upload_tasks)
        end_upload = time.time()

        print("Downloading with multipart...")
        start_download = time.time()
        download_tasks = [multipart_download(s3, f"{TEST_KEY_PREFIX}/file_{i}.bin", download_files[i]) for i in range(CONCURRENT_TRANSFERS)]
        await tqdm_asyncio.gather(*download_tasks)
        end_download = time.time()

        total_mb = FILE_SIZE_MB * CONCURRENT_TRANSFERS
        upload_speed = total_mb / (end_upload - start_upload)
        download_speed = total_mb / (end_download - start_download)

        print(f"Upload Time: {(end_upload - start_upload) * 1000:.0f} ms")
        print(f"Upload Speed: {upload_speed:.2f} MB/s")
        print(f"Download Time: {(end_download - start_download) * 1000:.0f} ms")
        print(f"Download Speed: {download_speed:.2f} MB/s")

        print("Cleaning up...")
        for i in range(CONCURRENT_TRANSFERS):
            await s3.delete_object(Bucket=BUCKET_NAME, Key=f"{TEST_KEY_PREFIX}/file_{i}.bin")
        for path in upload_files + download_files:
            if os.path.exists(path):
                os.remove(path)

if __name__ == '__main__':
    asyncio.run(run_speed_test())
