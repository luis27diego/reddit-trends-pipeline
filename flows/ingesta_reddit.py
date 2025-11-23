from prefect import flow, task
import requests
import os

DATA_DIR = "data/raw"

@task
def crear_directorio():
    os.makedirs(DATA_DIR, exist_ok=True)

@task
def descargar_reddit_dump():
    url = "https://files.pushshift.io/reddit/comments/RC_2023-01.zst"
    ruta_archivo = f"{DATA_DIR}/reddit_2023_01.zst"

    if not os.path.exists(ruta_archivo):
        print("Descargando dataset...")
        respuesta = requests.get(url, stream=True)
        with open(ruta_archivo, "wb") as f:
            for chunk in respuesta.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Descarga completa OK")
    else:
        print("Archivo ya existe, saltando OK")

    return ruta_archivo

@flow(name="Ingesta Reddit")
def flujo_ingesta():
    crear_directorio()
    descargar_reddit_dump()

if __name__ == "__main__":
    flujo_ingesta()
