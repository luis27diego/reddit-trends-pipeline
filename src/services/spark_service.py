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
        .config("spark.driver.maxResultSize", "2g")    # importante para evitar OOM en collect()
        # ---- Particiones agresivas (clave para baja memoria) ----
        .config("spark.sql.shuffle.partitions", "400")     # muchas particiones = menos memoria por tarea
        .config("spark.default.parallelism", "400")
        .config("spark.sql.files.maxPartitionBytes", "64mb")   # particiones pequeñas
        .config("spark.sql.adaptive.enabled", "true")         # Spark 3+ optimiza solo
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # ---- Optimizaciones para texto grande ----
        .config("spark.rdd.compress", "true")
        .config("spark.speculation", "false")
        .getOrCreate()
    )
    return spark
