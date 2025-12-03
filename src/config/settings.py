import os

class Settings:
    # MinIO
    MINIO_BLOCK_NAME = "minio-data-storage"
    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET")
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")  # ✅ NEW
    MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")  # ✅ NEW
    MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    RAW_FOLDER = "raw"
    PROCESSED_FOLDER = "processed"
    
    # Spark
    SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")
    SPARK_APP_NAME = "reddit-trends-processing"
    
    # Database
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASS = os.getenv("POSTGRES_PASSWORD")
    DB_HOST = os.getenv("POSTGRES_HOST")
    DB_PORT = "5432"
    DB_NAME = "reddit_analytics"
    
    @property
    def DATABASE_URL(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

settings = Settings()

if __name__ == "__main__":
    print("MINIO_BUCKET_NAME:", settings.MINIO_BUCKET_NAME)
    print("SPARK_MASTER_URL:", settings.SPARK_MASTER_URL)
    print("DB_USER:", settings.DB_USER)
    print("DB_PASS:", settings.DB_PASS)
    print("DB_HOST:", settings.DB_HOST)
    print("DATABASE_URL:", settings.DATABASE_URL)
