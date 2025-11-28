from src.config.schemas import REDDIT_SCHEMA

def leer_csv_optimizado(spark, path):
    """
    Lee el CSV usando un schema predefinido para evitar escanear los 12GB 
    antes de empezar (evita inferSchema).
    """
    return (spark.read
            .schema(REDDIT_SCHEMA)  # Schema explícito = lectura inmediata
            .option("header", "true")
            .option("multiLine", "true") # Necesario para saltos de línea en comentarios
            .option("escape", "\"")
            .csv(path))

def guardar_resultado(df, path_salida, formato="parquet"):
    """
    Guarda dataframes. Por defecto usa Parquet (más rápido y ligero que CSV).
    """
    (df.write
       .mode("overwrite")
       .format(formato)
       .save(path_salida))