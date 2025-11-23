from prefect import flow, task
import requests
import os
from prefect_aws.s3 import S3Bucket
from prefect_aws.credentials import AwsCredentials

# --- CONFIGURACIÓN DE MINIO ---
# Nombre del Bloque de credenciales que creaste en la UI
MINIO_BLOCK_NAME = "minio-data-storage"
# Nombre del Bucket que creaste en la consola de MinIO
MINIO_BUCKET_NAME = "tendencias-reddit"
# ------------------------------

@task
def crear_directorio():
    """
    Esta tarea se deja como un placeholder. 
    Se omite la creación de directorios locales ya que el almacenamiento
    principal ahora es MinIO, lo que es una mejor práctica en producción.
    """
    pass

@task
def descargar_reddit_dump(url: str = "https://files.pushshift.io/reddit/comments/RC_2023-01.zst"):
    """
    Descarga el archivo ZST de Reddit y lo sube directamente al bucket de MinIO
    utilizando el Bloque de credenciales configurado.
    """
    
    # 1. Definir la clave (ruta) del archivo dentro del bucket
    # Se genera un nombre de archivo limpio y se coloca en la carpeta 'raw'
    file_name = url.split('/')[-1]
    MINIO_FILE_KEY = f"raw/{file_name}"

    try:
        # 2. Cargar el Bloque de Credenciales (MinIO/AWS Credentials)
        # Esto obtiene la Access Key, Secret Key y el Endpoint URL:9000
        aws_credentials_block = AwsCredentials.load(MINIO_BLOCK_NAME)
        
        # 3. Crear el objeto S3Bucket para las operaciones de archivo
        # S3Bucket es la clase operacional que usa las credenciales para conectarse.
        minio_bucket = S3Bucket(
            bucket_name=MINIO_BUCKET_NAME,
            credentials=aws_credentials_block,
        )

        # 4. Verificar si el archivo ya existe en MinIO (Evita re-descargas costosas)
        if minio_bucket.key_exists(MINIO_FILE_KEY):
            print(f"Archivo ya existe en MinIO: s3://{MINIO_BUCKET_NAME}/{MINIO_FILE_KEY}, saltando la descarga y subida.")
            return MINIO_FILE_KEY

        # 5. Descarga del archivo
        print(f"Descargando dataset desde {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Leer el contenido completo
        data_content = response.content 
        
        # 6. Subir el archivo directamente a MinIO
        print("Subiendo archivo a MinIO...")
        # write_data maneja la conexión con el servidor de MinIO (vía el endpoint en las credenciales)
        minio_bucket.write_data(data_content, key=MINIO_FILE_KEY, overwrite=True)
        
        print(f"Descarga y subida completa a MinIO en: s3://{MINIO_BUCKET_NAME}/{MINIO_FILE_KEY}")
        
        return MINIO_FILE_KEY

    except Exception as e:
        print(f"ERROR: No se pudo completar la subida a MinIO. Asegúrese de que Docker/MinIO está activo y el Bloque '{MINIO_BLOCK_NAME}' es correcto.")
        # Re-lanza la excepción para que Prefect marque la tarea como fallida
        raise e

@flow(name="Flujo de Ingesta de Reddit")
def flujo_ingesta():
    """
    Flujo principal de Ingesta.
    """
    crear_directorio() 
    
    # La clave del archivo en MinIO es el output del Flow
    minio_key = descargar_reddit_dump() 
    
    # Aquí es donde se conectaría con la función run_deployment al Flow de Procesamiento

if __name__ == "__main__":
    flujo_ingesta()