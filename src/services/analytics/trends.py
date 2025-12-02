from pyspark.sql import functions as F
from src.utils.text_cleaning import convertir_timestamp

def analizar_tendencia_temporal(df):
    return (df
            .withColumn("fecha", convertir_timestamp("created_utc"))
            .withColumn("periodo", F.date_trunc("day", F.col("fecha")))
            .groupBy("periodo")
            .agg(
                F.avg("sentiment").alias("avg_sentiment"),
                F.count("*").alias("volumen")
            )
            .orderBy("periodo"))

def analizar_patrones_temporales(df):
    return (df
        .withColumn("fecha", convertir_timestamp("created_utc"))
        .withColumn("hora", F.hour("fecha"))
        .withColumn("dia_semana", F.dayofweek("fecha"))
        .withColumn("nombre_dia", 
                   F.when(F.col("dia_semana") == 1, "Domingo")
                    .when(F.col("dia_semana") == 2, "Lunes")
                    .when(F.col("dia_semana") == 3, "Martes")
                    .when(F.col("dia_semana") == 4, "Miércoles")
                    .when(F.col("dia_semana") == 5, "Jueves")
                    .when(F.col("dia_semana") == 6, "Viernes")
                    .otherwise("Sábado"))
        .groupBy("dia_semana", "hora", "nombre_dia")
        .agg(
            F.avg("score").alias("score_promedio"),
            F.count("*").alias("volumen"),
            F.avg("sentiment").alias("sentiment_promedio")
        )
        .orderBy("dia_semana", "hora"))
