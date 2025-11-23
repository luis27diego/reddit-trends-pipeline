from prefect import flow, task, get_run_logger
from prefect_databricks import DatabricksCredentials
from prefect_databricks.jobs import jobs_runs_submit
from prefect_databricks.models.jobs import (
    JobTaskSettings,
    SparkPythonTask,
    NewCluster,
    AutoScale,
)
from prefect_aws import AwsCredentials

# --- CONFIGURACIÓN DE BLOQUES Y PATHS ---
DATABRICKS_BLOCK_NAME = "databricks-spark-executor"
MINIO_BLOCK_NAME = "minio-data-storage"
MINIO_BUCKET_NAME = "tendencias-reddit"
PROCESSED_FILE_KEY = "processed/word_counts"

# Ruta al script en Databricks Repos
DATABRICKS_SCRIPT_PATH = "/Repos/tu_email_o_usuario/reddit-pipeline-repo/src/procesamiento_data.py"

# Configuración del cluster
CLUSTER_SPARK_VERSION = "13.3.x-scala2.12"
CLUSTER_NODE_TYPE = "i3.xlarge"
# ----------------------------------------


@task(retries=2, retry_delay_seconds=60)
async def ejecutar_spark_en_databricks(minio_key_entrada: str) -> str:
    """
    Ejecuta el script de PySpark en Databricks, pasando las credenciales de MinIO.
    
    Args:
        minio_key_entrada: Ruta del archivo en MinIO (ej: "raw/archivo.txt")
    
    Returns:
        Clave del archivo procesado en MinIO
    """
    logger = get_run_logger()

    # Construir rutas S3 completas
    s3_input_path = f"s3a://{MINIO_BUCKET_NAME}/{minio_key_entrada}"
    s3_output_path = f"s3a://{MINIO_BUCKET_NAME}/{PROCESSED_FILE_KEY}"

    logger.info(f"📥 Entrada: {s3_input_path}")
    logger.info(f"📤 Salida: {s3_output_path}")

    try:
        # Cargar credenciales de MinIO
        logger.info(f"🔑 Cargando credenciales de MinIO...")
        aws_credentials = await AwsCredentials.load(MINIO_BLOCK_NAME)
        
        # Obtener las credenciales
        minio_access_key = aws_credentials.aws_access_key_id
        minio_secret_key = aws_credentials.aws_secret_access_key.get_secret_value()
        minio_endpoint = aws_credentials.aws_session_token or "localhost:9000"  # Endpoint de MinIO
        
        logger.info(f"✅ Credenciales de MinIO cargadas")

        # Cargar credenciales de Databricks
        databricks_credentials = await DatabricksCredentials.load(DATABRICKS_BLOCK_NAME)
        logger.info(f"✅ Credenciales de Databricks cargadas")

        # Configurar auto-escalado del cluster
        auto_scale = AutoScale(
            min_workers=1,
            max_workers=2
        )

        # Configurar nuevo cluster CON credenciales de MinIO
        new_cluster = NewCluster(
            spark_version=CLUSTER_SPARK_VERSION,
            node_type_id=CLUSTER_NODE_TYPE,
            autoscale=auto_scale,
            spark_conf={
                "spark.speculation": "true",
                # ← AQUÍ PASAMOS LAS CREDENCIALES DE MINIO →
                "spark.hadoop.fs.s3a.endpoint": f"http://{minio_endpoint}",
                "spark.hadoop.fs.s3a.access.key": minio_access_key,
                "spark.hadoop.fs.s3a.secret.key": minio_secret_key,
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
            }
        )
        logger.info(f"🔧 Cluster configurado con credenciales de MinIO")

        # Configurar tarea de Spark Python (solo 2 parámetros: input y output)
        spark_python_task = SparkPythonTask(
            python_file=DATABRICKS_SCRIPT_PATH,
            parameters=[s3_input_path, s3_output_path]
        )

        # Configurar settings del job
        job_task_settings = JobTaskSettings(
            new_cluster=new_cluster,
            spark_python_task=spark_python_task,
            task_key="procesar-reddit-data",
            timeout_seconds=3600
        )

        logger.info(f"🚀 Ejecutando script: {DATABRICKS_SCRIPT_PATH}")
        
        # Ejecutar el job en Databricks
        run_result = await jobs_runs_submit(
            databricks_credentials=databricks_credentials,
            run_name="prefect-reddit-processing",
            tasks=[job_task_settings]
        )
        
        logger.info(f"✅ Job ejecutado exitosamente. Run ID: {run_result.get('run_id')}")
        logger.info(f"📊 Resultado disponible en: {s3_output_path}")
        
        return PROCESSED_FILE_KEY

    except Exception as e:
        logger.error(f"❌ Error al ejecutar el trabajo de Databricks: {e}")
        raise e


@flow(name="Flujo de Procesamiento de Palabras (Databricks)")
async def flujo_procesamiento_databricks(minio_key_entrada: str):
    """
    Flow principal que ejecuta el procesamiento de datos en Databricks.
    
    Args:
        minio_key_entrada: Clave del archivo en MinIO (ej: "raw/archivo.txt")
    
    Returns:
        Clave del archivo procesado
    """
    logger = get_run_logger()
    
    logger.info(f"🎯 Iniciando procesamiento de: {minio_key_entrada}")
    
    processed_key = await ejecutar_spark_en_databricks(minio_key_entrada=minio_key_entrada)
    
    logger.info(f"🎉 Procesamiento completado exitosamente")
    logger.info(f"📦 Resultado en: s3://{MINIO_BUCKET_NAME}/{processed_key}")
    
    return processed_key


if __name__ == "__main__":
    import asyncio
    
    # Ejemplo de uso con archivo TXT
    input_file = "raw/archivo.txt"
    asyncio.run(flujo_procesamiento_databricks(input_file))