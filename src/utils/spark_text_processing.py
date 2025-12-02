from pyspark.sql.functions import explode, split, lower, regexp_replace, col

def procesar_texto_distribuido(df_with_selftext):
    """Procesa texto usando operaciones distribuidas de Spark."""
    return (df_with_selftext
            .select(
                explode(
                    split(
                        regexp_replace(
                            lower(col("selftext")),
                            r"[^a-záéíóúñü\s]", " "
                        ),
                        r"\s+"
                    )
                ).alias("palabra")
            )
            .where(col("palabra") != "")
            .groupBy("palabra")
            .count()
            .orderBy(col("count").desc()))
