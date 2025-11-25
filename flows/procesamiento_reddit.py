from prefect import flow, task, get_run_logger
from pyspark.sql import SparkSession
from prefect_aws import AwsCredentials # Para cargar credenciales de MinIO

# --- CONFIGURACIÓN DE BLOQUES Y PATHS ---
MINIO_BLOCK_NAME = "minio-data-storage"
MINIO_BUCKET_NAME = "tendencias-reddit"
PROCESSED_FILE_KEY = "processed/word_counts"
# Master de Spark en Docker (por nombre de servicio)
SPARK_MASTER = "spark://spark-master:7077" 
# Si el script se ejecutara desde otro contenedor Spark, usarías spark://spark-master:7077
# Como Prefect se ejecuta fuera de Docker, usamos localhost
# ----------------------------------------


@task
async def ejecutar_spark_local(minio_key_entrada: str) -> str:
    """
    Ejecuta el procesamiento de PySpark localmente, conectado al cluster Docker.
    """
    logger = get_run_logger()

    # 1. Cargar credenciales de MinIO
    aws_credentials = await AwsCredentials.load(MINIO_BLOCK_NAME)
    minio_access_key = aws_credentials.aws_access_key_id
    minio_secret_key = aws_credentials.aws_secret_access_key.get_secret_value()
    # El Cloudflare Tunnel Endpoint (ej: "therapist-champagne-mercury-bee.trycloudflare.com")
    minio_endpoint = "watts-inputs-prefers-annex.trycloudflare.com"
    
    # 2. Configurar y crear Spark Session
    # Necesitas los JARs de Hadoop/S3 (se asume que están en el entorno Prefect)
    spark = SparkSession.builder \
        .appName("RedditWordCountLocal") \
        .master(SPARK_MASTER) \
        .config("fs.s3a.access.key", minio_access_key) \
        .config("fs.s3a.secret.key", minio_secret_key) \
        .config("fs.s3a.endpoint", f"https://{minio_endpoint}") \
        .config("fs.s3a.path.style.access", "true") \
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    logger.info(f" Spark Session creada, conectada a: {SPARK_MASTER}")
    
    s3_input_path = f"s3a://{MINIO_BUCKET_NAME}/{minio_key_entrada}"
    s3_output_path = f"s3a://{MINIO_BUCKET_NAME}/{PROCESSED_FILE_KEY}"
    
    try:
        # 3. Lógica de procesamiento de PySpark
        logger.info(f" Leyendo datos desde: {s3_input_path}")
        df = spark.read.text(s3_input_path)

        word_counts = (
            df.rdd.flatMap(lambda line: line[0].lower().split(" "))
            .filter(lambda word: len(word) > 3)
            .map(lambda word: (word, 1))
            .reduceByKey(lambda a, b: a + b)
            .toDF(["word", "count"])
            .sort("count", ascending=False)
        )

        logger.info(f" Escribiendo resultado en: {s3_output_path}")
        word_counts.write.mode("overwrite").parquet(s3_output_path)
        
    except Exception as e:
        logger.error(f" Error de Spark: {e}")
        raise e
    finally:
        spark.stop()
    
    return PROCESSED_FILE_KEY


@flow(name="Flujo de Procesamiento de Palabras (Local Docker Spark)")
async def flujo_procesamiento_local(minio_key_entrada: str):
    """
    Flow principal que ejecuta el procesamiento de datos en Spark local.
    """
    logger = get_run_logger()
    logger.info(f" Iniciando procesamiento local de: {minio_key_entrada}")
    
    processed_key = await ejecutar_spark_local(minio_key_entrada=minio_key_entrada)
    
    logger.info(f" Procesamiento completado. Resultado en: s3://{MINIO_BUCKET_NAME}/{processed_key}")
    
    return processed_key
