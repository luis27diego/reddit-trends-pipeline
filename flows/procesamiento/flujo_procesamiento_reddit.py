from prefect import flow
from flows.procesamiento.tasks_procesamiento import (
    cargar_archivo_minio,
    procesar_con_spark,
    guardar_resultado_minio
)

@flow(name="Flujo de Procesamiento de Palabras (Local Docker Spark)")
async def flujo_procesamiento_local(minio_key_entrada: str):
    print(f"Procesando archivo: {minio_key_entrada}")

    texto = await cargar_archivo_minio(minio_key_entrada)
    
    df_resultado = procesar_con_spark(texto)

    output_name = minio_key_entrada.split("/")[-1] + "_counts.csv"

    ruta = await guardar_resultado_minio(df_resultado, output_name)

    print(f"Archivo procesado guardado en: {ruta}")
    return ruta

if __name__ == "__main__":
    import asyncio
    asyncio.run(flujo_procesamiento_local("raw/1342-0.txt"))
