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
# --- 1. Preprocesamiento (Asegurando buen rendimiento en datos masivos) ---
    
    # 🎯 Ajustes para Inglés y Grandes Datos
    df_clean = df.filter(F.col("body").isNotNull()) \
                    .withColumn("clean_text", limpiar_columna_texto("body"))
    
    tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    remove_empty_tokens_udf = F.udf(lambda tokens: [t for t in tokens if t], ArrayType(StringType()))
    
  # 🎯 CountVectorizer: Aumentamos vocabulario, minDF (500) y añadimos el filtrado de tokens.
    cv = CountVectorizer(
        inputCol="final_filtered", 
        outputCol="features", 
        vocabSize=3000, # Aumentado a 3000 para mayor detalle
        minDF=500.0     # Asegura la aparición en al menos 500 documentos
    )
    
    # LDA: Optimizador 'online' para masivos y 30 iteraciones para convergencia
    lda = LDA(k=num_topics, maxIter=max_iter, featuresCol="features", optimizer="online")
    
    pipeline = Pipeline(stages=[
            tokenizer, 
            remover, 
            F.withColumn("final_filtered", remove_empty_tokens_udf(F.col("filtered"))), # Nuevo paso
            cv, 
            lda
        ])
    model = pipeline.fit(df_clean)
    
    # --- 2. Extracción del Vocabulario y los Temas ---
    
    # 2.1. Obtener el vocabulario del CountVectorizer
    # El modelo de CountVectorizer está en la tercera posición del pipeline (índice 2)
    #3.1. Obtener el vocabulario del CountVectorizer (índice 3 en el nuevo pipeline)
    cv_model = model.stages[3]
    vocab = cv_model.vocabulary
    
    # 3.2. Obtener los temas del modelo LDA (índice 4 en el nuevo pipeline)
    lda_model = model.stages[4]
    topics_df = lda_model.describeTopics(maxTermsPerTopic=20) # Aumentado a 20 términos
    
    # --- 3. Mapeo de Índices a Palabras (La Clave) ---
    
    # Definir una UDF para mapear los índices a las palabras reales
    @F.udf("array<string>")
    def map_terms(term_indices):
        # Esta UDF toma la lista de índices y la convierte a palabras usando el vocabulario
        return [vocab[int(i)] for i in term_indices]
    
    # Aplicar la UDF al DataFrame de temas
    topics_readable = topics_df.withColumn(
        "terms", 
        map_terms(F.col("termIndices"))
    )
    
    # Seleccionar las columnas relevantes y limpias
    final_topics = topics_readable.select(
        F.col("topic"),
        F.col("terms"),       # <--- ¡La lista de palabras del tema!
        F.col("termWeights")
    )
    
    # 4. Devolver todo el resultado (distribuido y legible)
    
    # Mostrar una muestra de los resultados finales legibles
    print("\n--- Muestra de Temas LDA Legibles ---")
    final_topics.show(truncate=80) 

    # Convertir el DataFrame de Spark completo a JSON para el resultado final
    # Usamos .collect() solo si la tabla de temas es pequeña (que lo es, 10 temas x 15 palabras)
    # y luego convertimos a JSON
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