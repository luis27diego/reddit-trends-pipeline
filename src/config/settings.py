import os

class Settings:
    # MinIO
    MINIO_BLOCK_NAME = "minio-data-storage"
    MINIO_BUCKET_NAME = "tendencias-reddit"
    RAW_FOLDER = "raw"
    PROCESSED_FOLDER = "processed"
    
    # Spark
    SPARK_MASTER_URL = "spark://spark-master:7077"
    SPARK_APP_NAME = "reddit-trends-processing"
    
    # Database
    DB_USER = os.getenv("POSTGRES_USER", "prefect")
    DB_PASS = os.getenv("POSTGRES_PASSWORD", "prefect")
    DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
    DB_PORT = "5432"
    DB_NAME = "reddit_analytics"
    
    @property
    def DATABASE_URL(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

settings = Settings()
