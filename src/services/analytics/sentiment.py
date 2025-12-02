from pyspark.sql import functions as F

def validar_sentimiento(df):
    return (df
            .withColumn("rango_score", 
                        F.when(F.col("score") > 100, "Alto")
                         .when(F.col("score") < -10, "Bajo")
                         .otherwise("Neutro"))
            .groupBy("rango_score")
            .agg(
                F.avg("sentiment").alias("avg_sentiment_real"),
                F.stddev("sentiment").alias("desviacion_sentiment")
            ))

def analizar_sentiment_vs_score(df):
    return (df
        .withColumn("rango_score",
                   F.when(F.col("score") >= 50, "Muy Alto (50+)")
                    .when(F.col("score") >= 10, "Alto (10-49)")
                    .when(F.col("score") >= 0, "Positivo (0-9)")
                    .when(F.col("score") >= -5, "Bajo (-5 a -1)")
                    .otherwise("Muy Bajo (<-5)"))
        .withColumn("rango_sentiment",
                   F.when(F.col("sentiment") >= 0.5, "Muy Positivo")
                    .when(F.col("sentiment") >= 0.1, "Positivo")
                    .when(F.col("sentiment") >= -0.1, "Neutral")
                    .when(F.col("sentiment") >= -0.5, "Negativo")
                    .otherwise("Muy Negativo"))
        .groupBy("rango_score", "rango_sentiment")
        .agg(F.count("*").alias("cantidad"))
        .orderBy("rango_score", "rango_sentiment"))

def encontrar_comentarios_extremos(df, limite=100):
    return (df
        .withColumn("score_abs", F.abs(F.col("score")))
        .withColumn("sentiment_abs", F.abs(F.col("sentiment")))
        .withColumn("extremo_tipo",
                   F.when((F.col("score") > 100) & (F.col("sentiment") > 0.7), "Viral Positivo")
                    .when((F.col("score") < -20) & (F.col("sentiment") < -0.7), "Controversial Negativo")
                    .when(F.col("score_abs") > 100, "Alto Engagement")
                    .when(F.col("sentiment_abs") > 0.8, "Sentimiento Extremo")
                    .otherwise("Normal"))
        .filter(F.col("extremo_tipo") != "Normal")
        .select(
            "id",
            F.col("`subreddit.name`").alias("subreddit_name"),
            "created_utc",
            "score",
            "sentiment",
            "extremo_tipo",
            F.substring("body", 1, 200).alias("preview_texto")
        )
        .orderBy(F.desc("score_abs"))
        .limit(limite))
