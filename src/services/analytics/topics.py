from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.clustering import LDA
from pyspark.ml import Pipeline
from src.utils.text_cleaning import limpiar_columna_texto

def extraer_temas_lda(df, num_topics=5, max_iter=10):
    df_clean = df.filter(F.col("body").isNotNull()) \
                 .withColumn("clean_text", limpiar_columna_texto("body"))
    
    tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    
    df_tokenized = tokenizer.transform(df_clean)
    df_stopwords_removed = remover.transform(df_tokenized)

    df_filtered_tokens = df_stopwords_removed.withColumn(
        "final_filtered",
        F.array_except(F.col("filtered"), F.array(F.lit(""))) 
    )

    cv = CountVectorizer(
        inputCol="final_filtered", 
        outputCol="features", 
        vocabSize=3000, 
        minDF=500.0     
    )
    
    lda = LDA(k=num_topics, maxIter=max_iter, featuresCol="features", optimizer="online") 
    
    pipeline_final = Pipeline(stages=[cv, lda])
    model = pipeline_final.fit(df_filtered_tokens)
    
    cv_model = model.stages[0]
    vocab = cv_model.vocabulary 
    
    lda_model = model.stages[1]
    topics_df = lda_model.describeTopics(maxTermsPerTopic=20)
    
    @F.udf("array<string>")
    def map_terms(term_indices):
        return [vocab[int(i)] for i in term_indices]
    
    topics_readable = topics_df.withColumn(
        "terms", 
        map_terms(F.col("termIndices"))
    )
    
    final_topics = topics_readable.select(
        F.col("topic"),
        F.col("terms"), 
        F.col("termWeights")
    )
    
    print(f"\n--- Muestra de Temas LDA Legibles ({num_topics} temas, 20 términos) ---")
    final_topics.show(truncate=80) 
    
    return final_topics
