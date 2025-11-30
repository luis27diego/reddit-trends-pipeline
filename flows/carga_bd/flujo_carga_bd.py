# src/flows/flujo_carga_bd.py

from prefect import flow
# Importamos las tareas de carga
from flows.carga_bd.tasks_carga_bd import task_cargar_resultados, task_crear_vistas

@flow(name="Orquestador ETL: Spark + Carga BD")
def flujo_principal_etl(rutas):
    """
    Flow principal que orquesta el procesamiento pesado con Spark 
    y la ingestión de resultados en la base de datos de analítica (Postgres).
    
    :param minio_key: Ruta del archivo raw en MinIO para procesar (ej: 'raw/data.csv').
    """
    
    # 2. Cargar los resultados de MinIO a Postgres (depende del resultado de Spark)
    # Utilizamos el output de la tarea anterior (rutas_output) como input para esta tarea.
    datos_cargados = task_cargar_resultados(rutas)
    
    # 3. Crear las Vistas (depende de que los datos se hayan cargado correctamente)
    # Las vistas son la capa final de presentación para BI/API.
    #task_crear_vistas(wait_for=[datos_cargados])
    
    # Retornamos el estado de éxito del flow
    return "Flow de ETL completado exitosamente."

# Ejemplo de ejecución local (esto no correrá en el worker, solo es para probar la definición)
# if __name__ == "__main__":
#     flujo_principal_etl(minio_key="raw/comments_sample.csv")