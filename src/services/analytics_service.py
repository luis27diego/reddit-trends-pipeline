from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.clustering import LDA
from pyspark.ml import Pipeline
from src.utils.text_cleaning import limpiar_columna_texto, convertir_timestamp

# --- Análisis 1: Tendencias ---
def analizar_tendencia_temporal(df):
    return (df
            .withColumn("fecha", convertir_timestamp("created_utc"))
            .withColumn("periodo", F.date_trunc("day", F.col("fecha")))
            .groupBy("periodo")
            .agg(
                F.avg("sentiment").alias("avg_sentiment"),
                F.count("*").alias("volumen")
            )
            .orderBy("periodo"))

# --- Análisis 2: Topic Modeling (LDA) ---
def extraer_temas_lda(df, num_topics=5, max_iter=10):
    # Limpieza rápida
    df_clean = df.filter(F.col("body").isNotNull()) \
                 .withColumn("clean_text", limpiar_columna_texto("body"))
    
    # Pipeline MLlib
    tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
    # Nota: Deberías cargar stopwords en español reales aquí
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    cv = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=1000, minDF=10.0)
    lda = LDA(k=num_topics, maxIter=max_iter, featuresCol="features")
    
    pipeline = Pipeline(stages=[tokenizer, remover, cv, lda])
    model = pipeline.fit(df_clean)
    
    # Retornamos el modelo para extraer temas, o el DF transformado
    # Para simplificar el guardado, extraemos la descripción de temas ahora
    topics = model.stages[-1].describeTopics(maxTermsPerTopic=10)
    return topics

# --- Análisis 3: Validación ---
def validar_sentimiento(df):
    # Lógica simple de validación: comparar score vs sentimiento
    # Hipótesis: Comentarios con score alto deberían tener sentimiento definido
    return (df
            .withColumn("rango_score", 
                        F.when(F.col("score") > 100, "Alto")
                         .when(F.col("score") < -10, "Bajo")
                         .otherwise("Neutro"))
            .groupBy("rango_score")
            .agg(
                F.avg("sentiment").alias("avg_sentiment_real"),
                F.stddev("sentiment").alias("desviacion_sentiment")
            ))