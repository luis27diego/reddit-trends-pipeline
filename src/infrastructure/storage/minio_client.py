from prefect_aws.s3 import S3Bucket
from prefect_aws import AwsCredentials
from src.config.settings import settings

async def get_minio_bucket():
    """Retorna un cliente S3 compatible con MinIO."""
    creds = await AwsCredentials.load(settings.MINIO_BLOCK_NAME)
    return S3Bucket(bucket_name=settings.MINIO_BUCKET_NAME, credentials=creds)

async def file_exists(bucket: S3Bucket, key: str) -> bool:
    try:
        await bucket.read_path(key)
        return True
    except:
        return False

async def upload_file(bucket: S3Bucket, key: str, content: bytes):
    await bucket.write_path(path=key, content=content)
