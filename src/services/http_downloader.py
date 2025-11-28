import requests
import pandas as pd


def download_file(url: str = None) -> bytes:
    url = url
    # Leer el archivo CSV
    df = pd.read_csv(r"C:\Users\luisd\Downloads\archive\the-reddit-covid-dataset-posts.csv")
    
    # Extraer la columna 'selftext', asegurándonos de que no haya valores nulos
    selftext_column = df['selftext'].dropna()

    # Unir todos los textos en un solo string
    combined_text = "\n".join(selftext_column)

    # Convertir el texto combinado a bytes
    return combined_text.encode('utf-8')
