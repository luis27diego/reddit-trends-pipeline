from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Ajusta los nombres de columnas según tu CSV real
REDDIT_SCHEMA = StructType([
    StructField("created_utc", LongType(), True),
    StructField("subreddit", StringType(), True),
    StructField("author", StringType(), True),
    StructField("body", StringType(), True),  # O 'selftext' según tu data
    StructField("sentiment", DoubleType(), True),
    StructField("score", LongType(), True)
])