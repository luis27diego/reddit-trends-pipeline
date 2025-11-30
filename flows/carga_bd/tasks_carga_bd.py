# src/tasks/tasks_carga_bd.py

from prefect import task
from src.services.loader_service import cargar_resultados_a_db
from src.services.loader_service import engine
from sqlalchemy import text
from prefect import get_run_logger

@task(log_prints=True, name="Cargar Resultados a Postgres")
def task_cargar_resultados(rutas_spark: dict):
    """Tarea que llama al servicio de carga de datos desde MinIO a PostgreSQL."""
    # Llama a la función de lógica principal
    cargar_resultados_a_db(rutas_spark)
    return True

@task(log_prints=True, name="Crear Vistas SQL")
def task_crear_vistas():
    """Tarea para ejecutar el DDL de Vistas SQL post-carga."""
    logger = get_run_logger()
    
    # 1. Definición del SQL para las vistas (usaremos el mismo código de las vistas anteriores)
    sql_views = """
    -- 1. VISTA: Heatmap de Actividad
    CREATE OR REPLACE VIEW view_heatmap_semanal AS
    SELECT 
        CASE 
            WHEN dia_semana = 1 THEN 'Domingo'
            WHEN dia_semana = 2 THEN 'Lunes'
            WHEN dia_semana = 3 THEN 'Martes'
            WHEN dia_semana = 4 THEN 'Miércoles'
            WHEN dia_semana = 5 THEN 'Jueves'
            WHEN dia_semana = 6 THEN 'Viernes'
            WHEN dia_semana = 7 THEN 'Sábado'
        END as dia_nombre,
        dia_semana,
        hora,
        volumen as intensidad_actividad,
        sentiment_promedio as sentimiento
    FROM patrones_horarios;

    -- 2. VISTA: Top Subreddits Clasificados
    CREATE OR REPLACE VIEW view_ranking_subreddits AS
    SELECT 
        subreddit_name,
        total_comentarios,
        sentiment_promedio,
        CASE 
            WHEN sentiment_promedio > 0.2 THEN 'Positivo'
            WHEN sentiment_promedio < -0.2 THEN 'Negativo'
            ELSE 'Neutro'
        END as etiqueta_sentimiento,
        comentarios_por_dia as velocidad_actividad
    FROM comparativa_subreddits
    WHERE total_comentarios > 50 
    ORDER BY total_comentarios DESC;

    -- 3. VISTA: Alertas de Anomalías Recientes
    CREATE OR REPLACE VIEW view_alertas_recientes AS
    SELECT 
        periodo,
        tipo_anomalia,
        volumen,
        sentiment_promedio
    FROM anomalias
    ORDER BY periodo DESC;
    """

    # 2. Ejecutar las sentencias SQL
    try:
        with engine.begin() as connection:
            # Ejecutamos el texto SQL
            connection.execute(text(sql_views))
        logger.info("Vistas SQL creadas/actualizadas exitosamente.")
        return True
    except Exception as e:
        logger.error(f"Fallo al crear las vistas: {e}")
        raise # Forzamos el fallo si las vistas no se crean.