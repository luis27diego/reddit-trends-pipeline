from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

# Esquema para lectura de CSV de Reddit
REDDIT_SCHEMA = StructType([
    StructField("type", StringType(), True),
    StructField("id", StringType(), True),
    StructField("subreddit.id", StringType(), True),
    StructField("subreddit.name", StringType(), True),
    StructField("subreddit.nsfw", BooleanType(), True),
    StructField("created_utc", LongType(), True),
    StructField("permalink", StringType(), True),
    StructField("body", StringType(), True),
    StructField("sentiment", DoubleType(), True),
    StructField("score", LongType(), True)
])
