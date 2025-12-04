from prefect import task
from src.infrastructure.storage.minio_client import get_minio_bucket, file_exists, upload_file
from src.infrastructure.http.client import download_file
from src.config.settings import settings
from src.infrastructure.kaggle.downloader import download_dataset, get_csv_files    

@task
def crear_directorio():
    pass  # si luego lo necesitas

@task(retries=1, retry_delay_seconds=10)
async def descargar_reddit_dump(dataset_slug: str = "pavellexyr/the-reddit-climate-change-dataset", target_file: str = "the-reddit-climate-change-dataset-comments.csv"):


    minio_key = f"{settings.RAW_FOLDER}/{target_file}"
    bucket = await get_minio_bucket()

    if await file_exists(bucket, minio_key):
        print(f"Archivo ya existe: {minio_key}")
        return minio_key

    print(f"Descargando dataset desde Kaggle: {dataset_slug}")

    return minio_key
