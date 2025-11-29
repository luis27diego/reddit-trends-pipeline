from pyspark.sql import functions as F
from pyspark.sql.window import Window
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


# --- Análisis 2: Controversia y Engagement por Subreddit ---
def analizar_controversia_por_subreddit(df):
    return (df
        .withColumn("tipo_contenido",
                   F.when((F.col("score") < 0) & (F.col("sentiment") < -0.3), "Controversial Negativo")
                    .when((F.col("score") > 20) & (F.col("sentiment") > 0.5), "Viral Positivo")
                    .when(F.abs(F.col("sentiment")) < 0.2, "Neutral")
                    .otherwise("Normal"))
        .groupBy("subreddit.name", "tipo_contenido")
        .agg(
            F.count("*").alias("cantidad"),
            F.avg("score").alias("score_promedio"),
            F.avg("sentiment").alias("sentiment_promedio")
        )
        .orderBy(F.desc("cantidad")))

# --- Análisis 3: Patrones Temporales (Hora y Día) ---
def analizar_patrones_temporales(df):
    return (df
        .withColumn("fecha", convertir_timestamp("created_utc"))
        .withColumn("hora", F.hour("fecha"))
        .withColumn("dia_semana", F.dayofweek("fecha"))
        .withColumn("nombre_dia", 
                   F.when(F.col("dia_semana") == 1, "Domingo")
                    .when(F.col("dia_semana") == 2, "Lunes")
                    .when(F.col("dia_semana") == 3, "Martes")
                    .when(F.col("dia_semana") == 4, "Miércoles")
                    .when(F.col("dia_semana") == 5, "Jueves")
                    .when(F.col("dia_semana") == 6, "Viernes")
                    .otherwise("Sábado"))
        .groupBy("hora", "nombre_dia")
        .agg(
            F.avg("score").alias("score_promedio"),
            F.count("*").alias("volumen"),
            F.avg("sentiment").alias("sentiment_promedio")
        )
        .orderBy("dia_semana", "hora"))

# --- Análisis 4: Top Palabras por Sentimiento ---
def palabras_por_sentimiento(df, top_n=20):
    return (df
        .filter(F.col("body").isNotNull())
        .withColumn("sentiment_categoria",
                   F.when(F.col("sentiment") > 0.3, "Positivo")
                    .when(F.col("sentiment") < -0.3, "Negativo")
                    .otherwise("Neutral"))
        .withColumn("clean_text", limpiar_columna_texto("body"))
        .withColumn("words", F.split(F.col("clean_text"), " "))
        .withColumn("word", F.explode("words"))
        .filter((F.length("word") > 3) & (F.col("word") != ""))  # Filtrar palabras cortas y vacías
        .groupBy("sentiment_categoria", "word")
        .agg(F.count("*").alias("frecuencia"))
        .withColumn("rank", F.row_number().over(
            Window.partitionBy("sentiment_categoria").orderBy(F.desc("frecuencia"))))
        .filter(F.col("rank") <= top_n)
        .orderBy("sentiment_categoria", "rank"))

# --- Análisis 5: Detección de Picos de Actividad (Anomalías) ---
def detectar_picos_actividad(df, ventana_horas=24):
    ventana_movil = Window.orderBy("periodo").rowsBetween(-ventana_horas, 0)
    
    return (df
        .withColumn("fecha", convertir_timestamp("created_utc"))
        .withColumn("periodo", F.date_trunc("hour", "fecha"))
        .groupBy("periodo")
        .agg(
            F.count("*").alias("volumen"),
            F.avg("sentiment").alias("sentiment_promedio")
        )
        .withColumn("media_movil", F.avg("volumen").over(ventana_movil))
        .withColumn("std_movil", F.stddev("volumen").over(ventana_movil))
        .withColumn("desviacion_std", 
                   (F.col("volumen") - F.col("media_movil")) / F.col("std_movil"))
        .withColumn("es_anomalia", 
                   F.when(F.col("desviacion_std") > 3, "Pico Alto")
                    .when(F.col("desviacion_std") < -3, "Caída Brusca")
                    .otherwise("Normal"))
        .filter(F.col("es_anomalia") != "Normal")
        .orderBy(F.desc("periodo")))

# --- Análisis 6: Comparativa Entre Subreddits ---
def comparar_subreddits(df):
    return (df
        .groupBy("subreddit.name")
        .agg(
            F.count("*").alias("total_comentarios"),
            F.avg("sentiment").alias("sentiment_promedio"),
            F.stddev("sentiment").alias("sentiment_std"),
            F.avg("score").alias("score_promedio"),
            F.sum(F.when(F.col("subreddit.nsfw") == True, 1).otherwise(0)).alias("contenido_nsfw"),
            F.min(convertir_timestamp("created_utc")).alias("primer_comentario"),
            F.max(convertir_timestamp("created_utc")).alias("ultimo_comentario")
        )
        .withColumn("dias_activos", 
                   F.datediff(F.col("ultimo_comentario"), F.col("primer_comentario")))
        .withColumn("comentarios_por_dia", 
                   F.col("total_comentarios") / F.greatest(F.col("dias_activos"), F.lit(1)))
        .orderBy(F.desc("total_comentarios")))

# --- Análisis 7: Distribución de Sentiment vs Score ---
def analizar_sentiment_vs_score(df):
    return (df
        .withColumn("rango_score",
                   F.when(F.col("score") >= 50, "Muy Alto (50+)")
                    .when(F.col("score") >= 10, "Alto (10-49)")
                    .when(F.col("score") >= 0, "Positivo (0-9)")
                    .when(F.col("score") >= -5, "Bajo (-5 a -1)")
                    .otherwise("Muy Bajo (<-5)"))
        .withColumn("rango_sentiment",
                   F.when(F.col("sentiment") >= 0.5, "Muy Positivo")
                    .when(F.col("sentiment") >= 0.1, "Positivo")
                    .when(F.col("sentiment") >= -0.1, "Neutral")
                    .when(F.col("sentiment") >= -0.5, "Negativo")
                    .otherwise("Muy Negativo"))
        .groupBy("rango_score", "rango_sentiment")
        .agg(F.count("*").alias("cantidad"))
        .orderBy("rango_score", "rango_sentiment"))

# --- Análisis 8: Comentarios Extremos (Outliers) ---
def encontrar_comentarios_extremos(df, limite=100):
    return (df
        .withColumn("score_abs", F.abs(F.col("score")))
        .withColumn("sentiment_abs", F.abs(F.col("sentiment")))
        .withColumn("extremo_tipo",
                   F.when((F.col("score") > 100) & (F.col("sentiment") > 0.7), "Viral Positivo")
                    .when((F.col("score") < -20) & (F.col("sentiment") < -0.7), "Controversial Negativo")
                    .when(F.col("score_abs") > 100, "Alto Engagement")
                    .when(F.col("sentiment_abs") > 0.8, "Sentimiento Extremo")
                    .otherwise("Normal"))
        .filter(F.col("extremo_tipo") != "Normal")
        .select(
            "id",
            "subreddit.name",
            "created_utc",
            "score",
            "sentiment",
            "extremo_tipo",
            F.substring("body", 1, 200).alias("preview_texto")
        )
        .orderBy(F.desc("score_abs"))
        .limit(limite))