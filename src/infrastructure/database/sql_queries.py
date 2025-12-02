SQL_CREATE_VIEWS = """
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
