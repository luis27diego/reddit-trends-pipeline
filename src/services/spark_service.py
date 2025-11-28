# src/services/spark_service.py
from pyspark.sql import SparkSession
from src.config.spark_config import SPARK_MASTER_URL, APP_NAME

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .master(SPARK_MASTER_URL)
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.shuffle.partitions", "400")
        .config("spark.default.parallelism", "400")
        .config("spark.sql.files.maxPartitionBytes", "64mb")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.rdd.compress", "true")
        .config("spark.speculation", "false")
        
        #Configuración MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        
        # --- SOLUCIÓN DEL ERROR 403 ---
        # 1. Fuerza el uso de las claves definidas arriba, ignora variables de entorno
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        # 2. Necesario para MinIO (evita errores de DNS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        
        # 3. Definición de clase (por seguridad)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        .getOrCreate()
    )
    return spark