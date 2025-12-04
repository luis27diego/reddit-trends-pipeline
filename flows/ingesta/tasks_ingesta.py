import shutil # <--- IMPORTANTE: Necesario para borrar carpetas
from pathlib import Path
from prefect import task
from src.infrastructure.storage.minio_client import get_minio_bucket, upload_file
from src.infrastructure.kaggle.downloader import download_dataset, get_csv_files
from src.config.settings import settings

@task(retries=1, retry_delay_seconds=10)
async def descargar_reddit_dump(url: str):
    dataset_slug = "pavellexyr/the-reddit-climate-change-dataset"
    target_file = "the-reddit-climate-change-dataset-comments.csv"
    bucket = await get_minio_bucket()
    minio_key = f"{settings.RAW_FOLDER}/{target_file}"
    
    dataset_path = None # Inicializamos variable para el scope del finally

    try:
        # 1. Descargar desde Kaggle
        print(f"Descargando dataset desde Kaggle: {dataset_slug}")
        dataset_path = download_dataset(dataset_slug)
        
        # 2. Obtener archivos CSV
        csv_files = get_csv_files(dataset_path)
        print(f"Archivos CSV encontrados: {[f.name for f in csv_files]}")

        # 3. Buscar el archivo objetivo
        target_path = next((f for f in csv_files if f.name == target_file), None)
        
        if not target_path:
            raise FileNotFoundError(
                f"Archivo {target_file} no encontrado. "
                f"Archivos disponibles: {[f.name for f in csv_files]}"
            )

        # 4. Leer y subir a MinIO
        # NOTA: Cuidado con f.read() si el archivo es > 2GB (ver recomendación abajo)
        print(f"Subiendo {target_file} a MinIO...")
        with open(target_path, 'rb') as f:
            content = f.read()
        
        await upload_file(bucket, minio_key, content)
        print(f"Archivo subido exitosamente: {minio_key}")
        
        # Liberamos memoria de la variable content explícitamente antes de borrar disco
        del content 

    except Exception as e:
        print(f"Ocurrió un error en el proceso: {e}")
        raise e # Re-lanzamos el error para que Prefect lo registre

    finally:
        # --- ZONA DE LIMPIEZA ---
        # Este bloque se ejecuta SIEMPRE, haya error o éxito
        if dataset_path and dataset_path.exists():
            print(f"Eliminando archivos locales en: {dataset_path}")
            shutil.rmtree(dataset_path) # Borra la carpeta y todo su contenido
            print("Limpieza de espacio en disco completada.")

    return minio_key # Es mejor retornar la key de MinIO, ya que el path local ya no existe