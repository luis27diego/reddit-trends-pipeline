from prefect import flow
from flows.procesamiento.tasks_procesamiento import procesar_archivo_grande
from prefect.deployments import run_deployment

@flow(name="Flujo de Procesamiento de Palabras (Local Docker Spark)")
async def flujo_procesamiento_grande(minio_key_entrada: str):
    print(f"Procesando archivo: {minio_key_entrada}")
    
    ruta_salida = procesar_archivo_grande(minio_key_entrada)
    
    print(f"Archivo procesado y guardado en carpeta: {ruta_salida}")
    print("Puedes ver los CSVs en MinIO dentro de esa carpeta (part-00000.csv, etc.)")

    # Paso 2: carga de datos a la base de datos
    await run_deployment(
        name="Orquestador ETL: Spark + Carga BD/carga-bd-deployment",
        parameters={'rutas': ruta_salida},
        timeout=0
    )
    

# Para pruebas locales
if __name__ == "__main__":
    import asyncio
    asyncio.run(flujo_procesamiento_grande("raw/1342-0.txt"))
