from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

# Esquema CORREGIDO, siguiendo el orden del CSV original:
REDDIT_SCHEMA_CORREGIDO = StructType([
    StructField("type", StringType(), True),           # 1
    StructField("id", StringType(), True),             # 2
    StructField("subreddit.id", StringType(), True),   # 3
    StructField("subreddit.name", StringType(), True), # 4 (El nombre del Subreddit)
    StructField("subreddit.nsfw", BooleanType(), True),# 5 
    StructField("created_utc", LongType(), True),      # 6 (El campo que buscabas al inicio)
    StructField("permalink", StringType(), True),      # 7
    StructField("body", StringType(), True),           # 8 
    StructField("sentiment", DoubleType(), True),      # 9 
    StructField("score", LongType(), True)             # 10
    # Opcional: El CSV no parece tener 'author' como columna de nivel superior,
    # si falta 'author', revisa cómo se mapeó en tu CSV.
])