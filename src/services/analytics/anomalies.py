from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.text_cleaning import convertir_timestamp

def detectar_picos_actividad(df, ventana_horas=24):
    ventana_movil = Window.orderBy("periodo").rowsBetween(-ventana_horas, 0)
    
    return (df
        .withColumn("fecha", convertir_timestamp("created_utc"))
        .withColumn("periodo", F.date_trunc("hour", "fecha"))
        .groupBy("periodo")
        .agg(
            F.count("*").alias("volumen"),
            F.avg("sentiment").alias("sentiment_promedio")
        )
        .withColumn("media_movil", F.avg("volumen").over(ventana_movil))
        .withColumn("std_movil", F.stddev("volumen").over(ventana_movil))
        .withColumn("desviacion_std", 
                   (F.col("volumen") - F.col("media_movil")) / F.col("std_movil"))
        .withColumn("es_anomalia", 
                   F.when(F.col("desviacion_std") > 3, "Pico Alto")
                    .when(F.col("desviacion_std") < -3, "Caída Brusca")
                    .otherwise("Normal"))
        .filter(F.col("es_anomalia") != "Normal")
        .orderBy(F.desc("periodo")))
