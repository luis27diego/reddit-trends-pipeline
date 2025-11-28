# src/services/text_processing_service.py  (reemplaza todo)
from pyspark.sql.functions import explode, split, lower, regexp_replace, col

def procesar_texto_distribuido(df_raw):
    """
    Recibe un DataFrame con columna 'value' (una línea por fila)
    y devuelve conteo de palabras 100% distribuido.
    """
    return (df_raw
            .select(
                explode(
                    split(
                        regexp_replace(
                            lower(col("value")), 
                            r"[^a-záéíóúñüA-ZÁÉÍÓÚÑÜ0-9 ]", " "
                        ),
                        r"\s+"
                    )
                ).alias("palabra")
            )
            .where(col("palabra") != "")
            .groupBy("palabra")
            .count()
            .orderBy(col("count").desc()))