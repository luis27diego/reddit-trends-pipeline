import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, lower, regexp_replace, col

def run_spark_job(s3_input_path: str, s3_output_path: str):
    """
    Función principal que inicia la sesión de Spark y ejecuta la lógica de procesamiento.
    
    Parámetros:
    - s3_input_path (str): Ruta completa al archivo de entrada en MinIO/S3.
    - s3_output_path (str): Ruta completa donde se guardará el resultado del conteo en MinIO/S3.
    """
    # 1. Crear la sesión de Spark (Databricks se encarga de las configuraciones)
    print("Iniciando sesión de Spark...")
    spark = (
        SparkSession.builder
        .appName("RedditTrendsProcessing")
        .getOrCreate()
    )
    
    # 2. Leer los datos desde MinIO/S3 (el archivo TXT)
    # Databricks usará las credenciales configuradas en el clúster para acceder a 's3a://'.
    print(f"Leyendo archivo desde S3/MinIO: {s3_input_path}")
    df = spark.read.text(s3_input_path) 
    
    # 3. Lógica de Conteo de Palabras (ejemplo de procesamiento)
    print("Ejecutando Conteo de Palabras...")
    
    # Renombrar la columna a 'text'
    word_df = df.withColumnRenamed("value", "text")
    
    # Limpiar el texto: convertir a minúsculas y remover puntuación
    word_df = word_df.withColumn("text", lower(col("text")))
    word_df = word_df.withColumn("text", regexp_replace(col("text"), "[^a-z\\s]", ""))
    
    # Dividir el texto en palabras y explotar (un-pivot)
    word_df = word_df.withColumn("word", explode(split(col("text"), "\\s+")))
    
    # Eliminar filas vacías después de la limpieza
    word_df = word_df.filter(col("word") != "")
    
    # Contar la frecuencia de cada palabra
    word_counts = (
        word_df.groupBy("word")
               .count()
               .orderBy(col("count").desc())
    )
    
    # 4. Escribir el resultado de vuelta a MinIO/S3
    print(f"Escribiendo resultado en S3/MinIO: {s3_output_path}")
    (
        word_counts.write
                    .mode("overwrite")
                    .format("csv") # Guardamos como CSV para facilitar la revisión
                    .save(s3_output_path)
    )

    print("Procesamiento de Spark finalizado exitosamente.")
    spark.stop()

if __name__ == "__main__":
    # La Jobs API de Databricks pasa los parámetros como argumentos posicionales.
    # En este caso, Databricks pasará los argumentos que definimos en el Flow de Prefect.
    parser = argparse.ArgumentParser(description="Spark Job para procesamiento de datos de Reddit.")
    parser.add_argument("input_path", type=str, help="Ruta de entrada S3/MinIO (ej: s3a://bucket/raw/file.txt)")
    parser.add_argument("output_path", type=str, help="Ruta de salida S3/MinIO (ej: s3a://bucket/processed/result/)")
    
    args = parser.parse_args()
    
    run_spark_job(args.input_path, args.output_path)