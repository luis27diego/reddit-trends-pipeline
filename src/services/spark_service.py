from pyspark.sql import SparkSession
from src.config.spark_config import SPARK_MASTER_URL, APP_NAME

def create_spark_session():
    """
    Crea una SparkSession conectada al cluster Docker Spark Standalone.
    """
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .master(SPARK_MASTER_URL)
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .getOrCreate()
    )
    return spark
