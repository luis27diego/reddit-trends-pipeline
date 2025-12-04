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
    dataset_slug = "pavellexyr/the-reddit-climate-change-dataset"
    target_file = "the-reddit-climate-change-dataset-comments.csv"
    bucket = await get_minio_bucket()
    minio_key = f"{settings.RAW_FOLDER}/{target_file}"
    
    # # Check if already exists in MinIO
    # if await file_exists(bucket, minio_key):
    #     print(f"Archivo ya existe en MinIO: {minio_key}")
    #     return minio_key

        # Download from Kaggle (runs in thread pool to avoid blocking)
    print(f"Descargando dataset desde Kaggle: {dataset_slug}")
    dataset_path  = download_dataset(dataset_slug)

    print(f"Archivos CSV encontrados: {csv_files}")
    csv_files = get_csv_files(dataset_path)

    print(f"Archivo subido: {dataset_path }")
    print(csv_files)
    return dataset_path 
