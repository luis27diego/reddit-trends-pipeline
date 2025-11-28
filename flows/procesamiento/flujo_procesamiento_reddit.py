from prefect import flow
from flows.procesamiento.tasks_procesamiento import procesar_archivo_grande

@flow(name="Flujo de Procesamiento de Palabras (Local Docker Spark)")
def flujo_procesamiento_grande(minio_key_entrada: str):
    print(f"Procesando archivo: {minio_key_entrada}")
    
    ruta_salida = procesar_archivo_grande.submit(minio_key_entrada).result()
    
    print(f"Archivo procesado y guardado en carpeta: {ruta_salida}")
    print("Puedes ver los CSVs en MinIO dentro de esa carpeta (part-00000.csv, etc.)")
    return ruta_salida

# Para pruebas locales
if __name__ == "__main__":
    import asyncio
    asyncio.run(flujo_procesamiento_grande("raw/1342-0.txt"))
