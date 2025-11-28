from pyspark.sql.functions import explode, split, col

def procesar_texto(df):
    """
    Recibe un DataFrame con una columna 'text'
    y retorna un DataFrame con conteo de palabras.
    """
    palabras = (
        df.select(explode(split(col("text"), r"\W+")).alias("palabra"))
        .where(col("palabra") != "")
        .groupBy("palabra")
        .count()
        .orderBy(col("count").desc())
    )
    return palabras
