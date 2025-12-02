from prefect import task
from sqlalchemy import text
from prefect import get_run_logger
from src.services.loader.data_loader import cargar_resultados_a_db
from src.infrastructure.database.connection import engine
from src.infrastructure.database.sql_queries import SQL_CREATE_VIEWS

@task(log_prints=True, name="Cargar Resultados a Postgres")
def task_cargar_resultados(rutas_spark: dict):
    """Tarea que llama al servicio de carga de datos desde MinIO a PostgreSQL."""
    cargar_resultados_a_db(rutas_spark)
    return True

@task(log_prints=True, name="Crear Vistas SQL")
def task_crear_vistas():
    """Tarea para ejecutar el DDL de Vistas SQL post-carga."""
    logger = get_run_logger()
    
    try:
        with engine.begin() as connection:
            connection.execute(text(SQL_CREATE_VIEWS))
        logger.info("Vistas SQL creadas/actualizadas exitosamente.")
        return True
    except Exception as e:
        logger.error(f"Fallo al crear las vistas: {e}")
        raise