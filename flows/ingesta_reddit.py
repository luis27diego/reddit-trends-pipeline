from prefect import flow, task
import requests
from prefect_aws.s3 import S3Bucket
from prefect_aws import AwsCredentials  # ← pequeño fix de import

# --- CONFIGURACIÓN DE MINIO ---
MINIO_BLOCK_NAME = "minio-data-storage"
MINIO_BUCKET_NAME = "tendencias-reddit"
# ------------------------------

@task
def crear_directorio():
    pass

@task(retries=1, retry_delay_seconds=10)
async def descargar_reddit_dump(url: str = "https://files.pushshift.io/reddit/comments/RC_2023-01.zst"):
    """
    Versión corregida y actualizada para Prefect 2.14+ / 3.x
    """
    file_name = url.split('/')[-1]
    MINIO_FILE_KEY = f"raw/{file_name}"

    try:
        # Cargar credenciales
        aws_credentials_block = AwsCredentials.load(MINIO_BLOCK_NAME)
        
        # Crear el bucket con las credenciales
        minio_bucket = S3Bucket(
            bucket_name=MINIO_BUCKET_NAME,
            credentials=aws_credentials_block
        )

        # ← AQUÍ ESTÁ EL CAMBIO PRINCIPAL →
        if await minio_bucket.exists(path=MINIO_FILE_KEY):
            print(f"Archivo ya existe en MinIO: s3://{MINIO_BUCKET_NAME}/{MINIO_FILE_KEY}")
            return MINIO_FILE_KEY
        # ← FIN DEL CAMBIO →

        print(f"Descargando dataset desde {url}...")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        
        data_content = response.content

        print("Subiendo archivo a MinIO...")
        await minio_bucket.write_data(data=data_content, key=MINIO_FILE_KEY)  # también async ahora
        
        print(f"Subida completada: s3://{MINIO_BUCKET_NAME}/{MINIO_FILE_KEY}")
        return MINIO_FILE_KEY

    except Exception as e:
        print(f"ERROR: {e}")
        raise e

@flow(name="Flujo de Ingesta de Reddit")
async def flujo_ingesta():  # ← también async ahora
    crear_directorio()
    minio_key = await descargar_reddit_dump()  # ← await aquí
    print(f"Flujo completado. Archivo disponible en: {minio_key}")

# Cambia el if __name__ para que funcione tanto sync como async
if __name__ == "__main__":
    import asyncio
    asyncio.run(flujo_ingesta())