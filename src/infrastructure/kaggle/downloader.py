import kagglehub
import os
from pathlib import Path
import zipfile
import logging
logger = logging.getLogger(__name__)
def download_dataset(dataset_slug: str, force_download: bool = False) -> Path:
    """
    Descarga un dataset de Kaggle usando kagglehub.
    
    Args:
        dataset_slug: formato "owner/dataset-name"
        force_download: Si True, fuerza re-descarga (ignora caché)
    
    Returns:
        Path: Ruta local al dataset descargado
    """
    logger.info(f"Downloading Kaggle dataset: {dataset_slug}")
    
    # kagglehub.dataset_download returns the path to downloaded files
    dataset_path = kagglehub.dataset_download(dataset_slug)
    
    logger.info(f"Dataset downloaded to: {dataset_path}")
    return Path(dataset_path)
def get_csv_files(dataset_path: Path) -> list[Path]:
    """
    Encuentra todos los archivos CSV en el dataset descargado.
    Maneja archivos comprimidos automáticamente.
    """
    csv_files = []
    
    # Check for ZIP files first
    for zip_file in dataset_path.glob("*.zip"):
        logger.info(f"Extracting {zip_file.name}")
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(dataset_path)
    
    # Collect all CSV files
    csv_files = list(dataset_path.glob("*.csv"))
    
    logger.info(f"Found {len(csv_files)} CSV files")
    return csv_files