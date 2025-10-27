import boto3
from botocore.client import Config

# MinIO connection settings
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='admin123',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

bucket_name = 'test-bucket'
file_name = 'hello.txt'

# Create bucket (if not exists)
buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
if bucket_name not in buckets:
    s3.create_bucket(Bucket=bucket_name)

# Upload a file
with open(file_name, 'w') as f:
    f.write('Hello MinIO!')

s3.upload_file(file_name, bucket_name, file_name)
print(f"✅ Uploaded '{file_name}' to '{bucket_name}'")

# List objects
print("Objects in bucket:")
for obj in s3.list_objects(Bucket=bucket_name).get('Contents', []):
    print(f"- {obj['Key']}")
