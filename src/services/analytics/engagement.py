from pyspark.sql import functions as F

def analizar_controversia_por_subreddit(df):
    return (df
        .withColumn("tipo_contenido",
                   F.when((F.col("score") < 0) & (F.col("sentiment") < -0.3), "Controversial Negativo")
                    .when((F.col("score") > 20) & (F.col("sentiment") > 0.5), "Viral Positivo")
                    .when(F.abs(F.col("sentiment")) < 0.2, "Neutral")
                    .otherwise("Normal"))
        .groupBy("`subreddit.name`", "tipo_contenido")
        .agg(
            F.count("*").alias("cantidad"),
            F.avg("score").alias("score_promedio"),
            F.avg("sentiment").alias("sentiment_promedio")
        )
        .orderBy(F.desc("cantidad")))
