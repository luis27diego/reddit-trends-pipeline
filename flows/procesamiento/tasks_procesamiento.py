from prefect import task
from src.services.minio_service import get_minio_bucket
from src.services.spark_service import create_spark_session
from src.services.text_processing_service import procesar_texto
from src.utils.text_utils import bytes_to_text
from src.config.minio_config import PROCESSED_FOLDER

@task
async def cargar_archivo_minio(minio_key: str):
    """
    Descarga el archivo desde MinIO.
    """
    bucket = await get_minio_bucket()
    contenido = await bucket.read_path(minio_key)
    return bytes_to_text(contenido)

@task
def procesar_con_spark(texto: str):
    """
    Procesa el texto usando Spark.
    """
    spark = create_spark_session()
    df = spark.createDataFrame([(texto,)], ["text"])
    resultado = procesar_texto(df)
    return resultado

@task
async def guardar_resultado_minio(df_resultado, nombre_archivo: str):
    """
    Guarda el DataFrame de conteo de palabras nuevamente en MinIO como CSV.
    """
    ruta = f"{PROCESSED_FOLDER}/{nombre_archivo}"

    csv_content = "\n".join([f"{r['palabra']},{r['count']}" for r in df_resultado.collect()])

    bucket = await get_minio_bucket()
    await bucket.write_path(ruta, csv_content.encode("utf-8"))

    return ruta
