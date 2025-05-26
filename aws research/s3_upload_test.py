import boto3
import datetime
import time
import uuid
import os

s3 = boto3.client('s3')
bucket_name = 'chkanief-bucket-for-karu-hilal-1'

def generate_and_upload_files(num_files=10):
    now = datetime.datetime.utcnow()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    hour = now.strftime('%H')
    epoch_time = str(int(time.time()))

    success_count = 0

    for _ in range(num_files):
        unique_id = str(uuid.uuid4())
        key = f"{year}/{month}/{day}/{hour}/test_{epoch_time}_{unique_id}.bin"
        binary_data = os.urandom(10 * 1024 * 1024)

        try:
            response = s3.put_object(Bucket=bucket_name, Key=key, Body=binary_data)
            status = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
            if status == 200:
                print(f"Uploaded: s3://{bucket_name}/{key}")
                success_count += 1
            else:
                print(f"Failed upload: {status} for {key}")
        except Exception as e:
            print(f"Error: {e} for {key}")
        time.sleep(1)

    print(f"{success_count} out of {num_files} files uploaded.")

if __name__ == "__main__":
    generate_and_upload_files()
