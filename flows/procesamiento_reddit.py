from prefect import flow, task, get_run_logger
from pyspark.sql import SparkSession
from prefect_aws import AwsCredentials


MINIO_BLOCK_NAME = "minio-data-storage"
MINIO_BUCKET_NAME = "tendencias-reddit"
PROCESSED_FILE_KEY = "processed/word_counts"
SPARK_MASTER = "spark://spark-master:7077"


@task
async def ejecutar_spark_local(minio_key_entrada: str) -> str:
    logger = get_run_logger()

    # 1. Cargar credenciales del Bloque
    aws_credentials = await AwsCredentials.load(MINIO_BLOCK_NAME)

    access_key = aws_credentials.aws_access_key_id
    secret_key = aws_credentials.aws_secret_access_key.get_secret_value()

    # ⚠️ Ahora el endpoint también sale del bloque
    # Prefect lo guarda en: `aws_credentials.endpoint_url`
    endpoint = aws_credentials.endpoint_url.rstrip("/")  # evita doble slash

    logger.info(f" Usando MinIO endpoint del bloque: {endpoint}")

    # 2. SparkSession
    spark = (
        SparkSession.builder
        .appName("RedditWordCountLocal")
        .master(SPARK_MASTER)
        .config("fs.s3a.access.key", access_key)
        .config("fs.s3a.secret.key", secret_key)
        .config("fs.s3a.endpoint", endpoint)
        .config("fs.s3a.path.style.access", "true")
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    s3_input_path = f"s3a://{MINIO_BUCKET_NAME}/{minio_key_entrada}"
    s3_output_path = f"s3a://{MINIO_BUCKET_NAME}/{PROCESSED_FILE_KEY}"

    try:
        df = spark.read.text(s3_input_path)

        word_counts = (
            df.rdd.flatMap(lambda line: line[0].lower().split(" "))
            .filter(lambda w: len(w) > 3)
            .map(lambda w: (w, 1))
            .reduceByKey(lambda a, b: a + b)
            .toDF(["word", "count"])
            .sort("count", ascending=False)
        )

        word_counts.write.mode("overwrite").csv(s3_output_path)

    finally:
        spark.stop()

    return PROCESSED_FILE_KEY


@flow
async def flujo_procesamiento_local(minio_key_entrada: str):
    return await ejecutar_spark_local(minio_key_entrada)
