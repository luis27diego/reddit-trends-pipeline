from prefect import flow, task, get_run_logger
from prefect_databricks import DatabricksCredentials
from prefect_databricks.jobs import jobs_runs_submit
from prefect_databricks.models.jobs import (
    JobTaskSettings,
    NotebookTask,
)
from prefect_aws import AwsCredentials

# --- CONFIGURACIÓN DE BLOQUES Y PATHS ---
DATABRICKS_BLOCK_NAME = "databricks-spark-executor"
MINIO_BLOCK_NAME = "minio-data-storage"
MINIO_BUCKET_NAME = "tendencias-reddit"
PROCESSED_FILE_KEY = "processed/word_counts"

# Ruta al notebook en Databricks Workspace
# IMPORTANTE: Actualiza con tu ruta real del notebook
# Ejemplo: /Users/tu_email@gmail.com/procesamiento_reddit
DATABRICKS_NOTEBOOK_PATH = "/Workspace/procesamiento_reddit"
# ----------------------------------------


@task(retries=2, retry_delay_seconds=60)
async def ejecutar_spark_en_databricks(minio_key_entrada: str) -> str:
    """
    Ejecuta el notebook de PySpark en Databricks usando serverless compute.
    
    Args:
        minio_key_entrada: Ruta del archivo en MinIO (ej: "raw/archivo.txt")
    
    Returns:
        Clave del archivo procesado en MinIO
    """
    logger = get_run_logger()

    # Construir rutas S3 completas
    s3_input_path = f"s3a://{MINIO_BUCKET_NAME}/{minio_key_entrada}"
    s3_output_path = f"s3a://{MINIO_BUCKET_NAME}/{PROCESSED_FILE_KEY}"

    logger.info(f" Entrada: {s3_input_path}")
    logger.info(f" Salida: {s3_output_path}")

    try:
        # Cargar credenciales de MinIO
        logger.info(f" Cargando credenciales de MinIO...")
        aws_credentials = await AwsCredentials.load(MINIO_BLOCK_NAME)
        
        # Obtener las credenciales
        minio_access_key = aws_credentials.aws_access_key_id
        minio_secret_key = aws_credentials.aws_secret_access_key.get_secret_value()
        minio_endpoint = aws_credentials.aws_session_token or "localhost:9000"
        
        logger.info(f" Credenciales de MinIO cargadas")
        
        # Cargar credenciales de Databricks
        databricks_credentials = await DatabricksCredentials.load(DATABRICKS_BLOCK_NAME)
        logger.info(f" Credenciales de Databricks cargadas")

        # Configurar tarea de Notebook con parámetros (INCLUYE CREDENCIALES)
        notebook_task = NotebookTask(
            notebook_path=DATABRICKS_NOTEBOOK_PATH,
            base_parameters={
                "input_path": s3_input_path,
                "output_path": s3_output_path,
                "minio_access_key": minio_access_key,
                "minio_secret_key": minio_secret_key,
                "minio_endpoint": minio_endpoint
            }
        )
        logger.info(f" minio_endpoint = {minio_endpoint}")

        # Configurar settings del job (sin especificar cluster = usa serverless)
        job_task_settings = JobTaskSettings(
            notebook_task=notebook_task,
            task_key="procesar-reddit-data",
            timeout_seconds=3600
        )

        logger.info(f" Ejecutando notebook serverless: {DATABRICKS_NOTEBOOK_PATH}")
        logger.info(f" Parámetros: input={s3_input_path}, output={s3_output_path}")
        
        # Ejecutar el job en Databricks (usará serverless automáticamente)
        run_result = await jobs_runs_submit(
            databricks_credentials=databricks_credentials,
            run_name="prefect-reddit-processing",
            tasks=[job_task_settings]
        )
        
        run_id = run_result.get('run_id')
        logger.info(f" Job enviado exitosamente")
        logger.info(f" Run ID: {run_id}")
        logger.info(f" Ver en Databricks: https://dbc-8613e21f-c701.cloud.databricks.com/#job/{run_id}")
        logger.info(f" Resultado estará en: {s3_output_path}")
        
        return PROCESSED_FILE_KEY

    except Exception as e:
        logger.error(f" Error al ejecutar el trabajo de Databricks: {e}")
        raise e


@flow(name="Flujo de Procesamiento de Palabras (Databricks)")
async def flujo_procesamiento_databricks(minio_key_entrada: str):
    """
    Flow principal que ejecuta el procesamiento de datos en Databricks Serverless.
    
    Args:
        minio_key_entrada: Clave del archivo en MinIO (ej: "raw/archivo.txt")
    
    Returns:
        Clave del archivo procesado
    """
    logger = get_run_logger()
    
    logger.info(f" Iniciando procesamiento serverless de: {minio_key_entrada}")
    
    processed_key = await ejecutar_spark_en_databricks(minio_key_entrada=minio_key_entrada)
    
    logger.info(f" Procesamiento completado exitosamente")
    logger.info(f" Resultado en: s3://{MINIO_BUCKET_NAME}/{processed_key}")
    
    return processed_key


if __name__ == "__main__":
    import asyncio
    
    # Ejemplo de uso con archivo TXT
    input_file = "raw/1342-0.txt"
    asyncio.run(flujo_procesamiento_databricks(input_file))