from prefect import flow, task
from prefect.deployments import run_deployment
import requests
from prefect_aws.s3 import S3Bucket
from prefect_aws import AwsCredentials
from flows.procesamiento_reddit import flujo_procesamiento_local # Nuevo

# --- CONFIGURACIÓN DE MINIO ---
MINIO_BLOCK_NAME = "minio-data-storage"
MINIO_BUCKET_NAME = "tendencias-reddit"
# ------------------------------

@task
def crear_directorio():
    pass

@task(retries=1, retry_delay_seconds=10)
async def descargar_reddit_dump(url: str = "https://www.gutenberg.org/files/1342/1342-0.txt"):
    """
    Versión corregida y actualizada para Prefect 2.14+ / 3.x
    """
    file_name = url.split('/')[-1]
    MINIO_FILE_KEY = f"raw/{file_name}"

    try:
        # Cargar credenciales
        aws_credentials_block = await AwsCredentials.load(MINIO_BLOCK_NAME)
        
        # Crear el bucket con las credenciales
        minio_bucket = S3Bucket(
            bucket_name=MINIO_BUCKET_NAME,
            credentials=aws_credentials_block
        )

        # Verificar si el archivo ya existe usando read_path (si falla, no existe)
        try:
            await minio_bucket.read_path(MINIO_FILE_KEY)
            print(f" Archivo ya existe en MinIO: s3://{MINIO_BUCKET_NAME}/{MINIO_FILE_KEY}")
            return MINIO_FILE_KEY
        except Exception:
            # El archivo no existe, proceder con la descarga
            print(f" Archivo no encontrado, procediendo con la descarga...")

        print(f" Descargando dataset desde {url}...")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        
        data_content = response.content

        print(" Subiendo archivo a MinIO...")
        await minio_bucket.write_path(path=MINIO_FILE_KEY, content=data_content)
        
        print(f" Subida completada: s3://{MINIO_BUCKET_NAME}/{MINIO_FILE_KEY}")
        return MINIO_FILE_KEY

    except Exception as e:
        print(f" ERROR: {e}")
        raise e

@task
async def ejecutar_procesamiento(minio_key: str):
    """
    Ejecuta el flow de procesamiento de Databricks pasando la key del archivo.
    """
    print(f" Iniciando procesamiento de Spark Docker para: {minio_key}")
    
    try:
        # Ejecutar el deployment del flow de procesamiento
        flow_run = await run_deployment(
            name="Flujo de Procesamiento de Palabras (Local Docker Spark)/spark-local-processing-deployment",            
            parameters={"minio_key_entrada": minio_key},
            timeout=0  # Sin timeout, esperará indefinidamente
        )
        
        print(f" Flow de procesamiento ejecutado: {flow_run}")
        return flow_run
        
    except Exception as e:
        print(f" Error al ejecutar procesamiento: {e}")
        raise e

@flow(name="Flujo de Ingesta de Reddit")
async def flujo_ingesta():
    """
    Flow completo: Descarga archivo y ejecuta procesamiento en Databricks
    """
    crear_directorio()
    
    # Paso 1: Descargar/verificar archivo
    minio_key = await descargar_reddit_dump()
    print(f" Ingesta completada. Archivo disponible en: {minio_key}")
    
    # Paso 2: Ejecutar procesamiento en Databricks
    await ejecutar_procesamiento(minio_key)
    
    print(f" Pipeline completo finalizado exitosamente")

if __name__ == "__main__":
    import asyncio
    asyncio.run(flujo_ingesta())