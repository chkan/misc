import boto3
import time
import os
import uuid

# Configuration
BUCKET_NAME = 'your-bucket-name'  # Change this to your S3 bucket
REGION = 'us-east-1'              # Change if needed
TEST_FILE_SIZE_MB = 100           # Size of the test file
TEST_KEY = f'test-s3-speed-{uuid.uuid4()}.bin'

def generate_test_file(file_path, size_mb):
    with open(file_path, 'wb') as f:
        f.write(os.urandom(size_mb * 1024 * 1024))

def measure_upload(s3_client, file_path, key):
    start = time.time()
    s3_client.upload_file(file_path, BUCKET_NAME, key)
    end = time.time()
    return end - start

def measure_download(s3_client, file_path, key):
    start = time.time()
    s3_client.download_file(BUCKET_NAME, key, file_path)
    end = time.time()
    return end - start

def main():
    s3 = boto3.client('s3', region_name=REGION)
    temp_file = '/tmp/s3_speed_test_file.bin'
    download_file = '/tmp/s3_speed_downloaded.bin'
    
    print(f'Generating test file ({TEST_FILE_SIZE_MB} MB)...')
    generate_test_file(temp_file, TEST_FILE_SIZE_MB)
    
    print('Uploading...')
    upload_time = measure_upload(s3, temp_file, TEST_KEY)
    upload_speed = TEST_FILE_SIZE_MB / upload_time
    print(f'Upload time: {upload_time:.2f}s, Speed: {upload_speed:.2f} MB/s')
    
    print('Downloading...')
    download_time = measure_download(s3, download_file, TEST_KEY)
    download_speed = TEST_FILE_SIZE_MB / download_time
    print(f'Download time: {download_time:.2f}s, Speed: {download_speed:.2f} MB/s')

    # Cleanup
    print('Cleaning up...')
    s3.delete_object(Bucket=BUCKET_NAME, Key=TEST_KEY)
    os.remove(temp_file)
    os.remove(download_file)

if __name__ == '__main__':
    main()
