from fastapi import FastAPI, UploadFile, File, HTTPException
import boto3
from botocore.client import Config
import os

# MinIO settings (can also come from env vars)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
REGION = os.getenv("MINIO_REGION", "us-east-1")
# URL joignable depuis le navigateur (dans Docker, MINIO_ENDPOINT = http://minio:9000 n'est pas résolvable depuis l'hôte)
MINIO_PUBLIC_ENDPOINT = os.getenv("MINIO_PUBLIC_ENDPOINT", MINIO_ENDPOINT)


def make_client(endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name=REGION
    )


# Client interne (API -> MinIO) et client utilisé uniquement pour signer les URLs publiques
s3 = make_client(MINIO_ENDPOINT)
s3_public = make_client(MINIO_PUBLIC_ENDPOINT)

app = FastAPI(title="MinIO Query Service")

@app.get("/buckets")
def list_buckets():
    """List all buckets"""
    response = s3.list_buckets()
    return {"buckets": [b["Name"] for b in response["Buckets"]]}


@app.get("/buckets/{bucket_name}/objects")
def list_objects(bucket_name: str):
    """List all objects in a bucket"""
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        objects = [obj["Key"] for obj in response.get("Contents", [])]
        return {"bucket": bucket_name, "objects": objects}
    except s3.exceptions.NoSuchBucket:
        raise HTTPException(status_code=404, detail="Bucket not found")


@app.post("/buckets/{bucket_name}/upload")
async def upload_file(bucket_name: str, file: UploadFile = File(...)):
    """Upload a file to a bucket"""
    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        s3.create_bucket(Bucket=bucket_name)
    
    s3.upload_fileobj(file.file, bucket_name, file.filename)
    return {"message": f"File '{file.filename}' uploaded to bucket '{bucket_name}'"}


@app.get("/buckets/{bucket_name}/download/{filename}")
def download_file(bucket_name: str, filename: str):
    """Generate a presigned URL to download a file"""
    try:
        url = s3_public.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": filename},
            ExpiresIn=3600  # 1 hour
        )
        return {"download_url": url}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
