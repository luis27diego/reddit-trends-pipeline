from prefect import flow
from prefect.deployments import run_deployment
from flows.ingesta.tasks_ingesta import crear_directorio, descargar_reddit_dump

@flow(name="Flujo de Ingesta de Reddit")
async def flujo_ingesta(url="https://www.gutenberg.org/files/1342/1342-0.txt"):
    
    crear_directorio()

    # Paso 1: descarga
    minio_key = await descargar_reddit_dump(url)
    print(f"Ingesta completada: {minio_key}")

    # Paso 2: procesamiento
    await run_deployment(
        name="Flujo de Procesamiento de Palabras (Local Docker Spark)",
        parameters={"minio_key_entrada": minio_key},
        timeout=0
    )

    print("Pipeline finalizado correctamente")

if __name__ == "__main__":
    import asyncio
    asyncio.run(flujo_ingesta())
