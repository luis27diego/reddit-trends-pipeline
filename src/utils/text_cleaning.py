from pyspark.sql import functions as F

def limpiar_columna_texto(col_name):
    """Devuelve la columna transformada: lowercase y sin caracteres especiales"""
    return F.regexp_replace(
        F.lower(F.col(col_name)),
        r"[^a-záéíóúñü0-9\s]", " "
    )

def convertir_timestamp(col_name):
    """Convierte unix timestamp a formato fecha"""
    return F.from_unixtime(F.col(col_name)).cast("timestamp")