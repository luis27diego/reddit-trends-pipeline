from pyspark.sql import functions as F
from src.utils.text_cleaning import convertir_timestamp

def comparar_subreddits(df):
    return (df
        .groupBy("`subreddit.name`")
        .agg(
            F.count("*").alias("total_comentarios"),
            F.avg("sentiment").alias("sentiment_promedio"),
            F.stddev("sentiment").alias("sentiment_std"),
            F.avg("score").alias("score_promedio"),
            F.sum(F.when(F.col("`subreddit.nsfw`") == True, 1).otherwise(0)).alias("contenido_nsfw"),
            F.min(convertir_timestamp("created_utc")).alias("primer_comentario"),
            F.max(convertir_timestamp("created_utc")).alias("ultimo_comentario")
        )
        .withColumn("dias_activos", 
                   F.datediff(F.col("ultimo_comentario"), F.col("primer_comentario")))
        .withColumn("comentarios_por_dia", 
                   F.col("total_comentarios") / F.greatest(F.col("dias_activos"), F.lit(1)))
        .orderBy(F.desc("total_comentarios")))
