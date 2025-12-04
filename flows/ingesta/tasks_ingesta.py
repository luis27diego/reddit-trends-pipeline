from prefect import task
from src.infrastructure.storage.minio_client import get_minio_bucket, file_exists, upload_file
from src.infrastructure.http.client import download_file
from src.config.settings import settings
from src.infrastructure.kaggle.downloader import download_dataset, get_csv_files
@task
def crear_directorio():
    pass  # si luego lo necesitas

@task(retries=1, retry_delay_seconds=10)
async def descargar_reddit_dump(url: str):
    file_name = url.split("/")[-1]
    key = f"{settings.RAW_FOLDER}/{file_name}"

    bucket = await get_minio_bucket()

    if await file_exists(bucket, key):
        print(f"Archivo ya existe: {key}")
        return key

    print(f"Descargando dataset desde {url}")
    content = download_file(url)

    print("Subiendo archivo a MinIO...")
    await upload_file(bucket, key, content)

    print(f"Archivo subido: {key}")
    return key
