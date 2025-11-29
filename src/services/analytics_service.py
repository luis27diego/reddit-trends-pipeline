from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.clustering import LDA
from pyspark.ml import Pipeline
from src.utils.text_cleaning import limpiar_columna_texto, convertir_timestamp
from pyspark.sql.types import ArrayType, StringType

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
# 🎯 1. Limpieza inicial: Aplica tu regexp_replace
    df_clean = df.filter(F.col("body").isNotNull()) \
                 .withColumn("clean_text", limpiar_columna_texto("body"))
    
    # --- 2. Pipeline MLlib ---
    
    tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    
    # 🎯 CORRECCIÓN CLAVE: Eliminar tokens vacíos de forma nativa.
    # Usamos array_except() para eliminar cualquier aparición de un array conteniendo el string vacío
    # Necesitamos el token vacío como array para que funcione array_except.
    
    df_tokenized = tokenizer.transform(df_clean)
    df_stopwords_removed = remover.transform(df_tokenized)

    # El token vacío es el string '', lo removemos de la lista de tokens
    df_filtered_tokens = df_stopwords_removed.withColumn(
        "final_filtered",
        F.array_except(F.col("filtered"), F.array(F.lit(""))) 
    )

    # 🎯 CountVectorizer: Usar la columna final_filtered, que ahora NO tiene tokens vacíos
    cv = CountVectorizer(
        inputCol="final_filtered", 
        outputCol="features", 
        vocabSize=3000, 
        minDF=500.0     
    )
    
    lda = LDA(k=num_topics, maxIter=max_iter, featuresCol="features", optimizer="online") 
    
    # Creamos el pipeline con las etapas ya transformadas
    pipeline_final = Pipeline(stages=[cv, lda])
    model = pipeline_final.fit(df_filtered_tokens) # Entrenamos el modelo sobre el DF limpio
    
    # --- 3. Extracción del Vocabulario y Mapeo ---
    # ... (El resto del código de mapeo con vocab y describeTopics) ...
    
    cv_model = model.stages[0] # Ahora es el índice 0 del pipeline (antes era 3)
    vocab = cv_model.vocabulary 
    
    lda_model = model.stages[1] # Ahora es el índice 1 del pipeline (antes era 4)
    topics_df = lda_model.describeTopics(maxTermsPerTopic=20)
    
    # ... (Mapeo con la UDF y return final_topics) ...
    
    # 3.3. Mapeo (UDF) - La UDF es necesaria para el mapeo, no para el filtrado de tokens
    @F.udf("array<string>")
    def map_terms(term_indices):
        return [vocab[int(i)] for i in term_indices]
    
    # Aplicar el UDF
    topics_readable = topics_df.withColumn(
        "terms", 
        map_terms(F.col("termIndices"))
    )
    
    # Seleccionar las columnas relevantes y limpias
    final_topics = topics_readable.select(
        F.col("topic"),
        F.col("terms"), 
        F.col("termWeights")
    )
    
    print(f"\n--- Muestra de Temas LDA Legibles ({num_topics} temas, 20 términos) ---")
    final_topics.show(truncate=80) 
    
    return final_topics
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