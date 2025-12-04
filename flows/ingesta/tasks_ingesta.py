from prefect import task
from src.infrastructure.storage.minio_client import get_minio_bucket, file_exists, upload_file
from src.infrastructure.http.client import download_file
from src.config.settings import settings
from src.infrastructure.kaggle.downloader import download_dataset, get_csv_files
@task
def crear_directorio():
    pass  # si luego lo necesitas

@task(retries=1, retry_delay_seconds=10)
async def descargar_reddit_dump(dataset_slug: str = "pavellexyr/the-reddit-climate-change-dataset",target_file: str = "the-reddit-climate-change-dataset-comments.csv"):
    bucket = await get_minio_bucket()
    minio_key = f"{settings.RAW_FOLDER}/{target_file}"
    print(f"Verificando si el archivo ya existe en MinIO: {minio_key}")
    # Check if already exists in MinIO
    if await file_exists(bucket, minio_key):
        print(f"Archivo ya existe en MinIO: {minio_key}")
        return minio_key
    # url = "vv"
    # print(f"Descargando dataset desde {url}")
    # content = download_file(url)

    # print("Subiendo archivo a MinIO...")
    # await upload_file(bucket, key, content)

    # print(f"Archivo subido: {key}")
    return None
