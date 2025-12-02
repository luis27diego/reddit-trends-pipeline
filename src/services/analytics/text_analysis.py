from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.text_cleaning import limpiar_columna_texto

def palabras_por_sentimiento(df, top_n=20):
    return (df
        .filter(F.col("body").isNotNull())
        .withColumn("sentiment_categoria",
                   F.when(F.col("sentiment") > 0.3, "Positivo")
                    .when(F.col("sentiment") < -0.3, "Negativo")
                    .otherwise("Neutral"))
        .withColumn("clean_text", limpiar_columna_texto("body"))
        .withColumn("words", F.split(F.col("clean_text"), " "))
        .withColumn("word", F.explode("words"))
        .filter((F.length("word") > 3) & (F.col("word") != ""))  # Filtrar palabras cortas y vacías
        .groupBy("sentiment_categoria", "word")
        .agg(F.count("*").alias("frecuencia"))
        .withColumn("rank", F.row_number().over(
            Window.partitionBy("sentiment_categoria").orderBy(F.desc("frecuencia"))))
        .filter(F.col("rank") <= top_n)
        .orderBy("sentiment_categoria", "rank"))
