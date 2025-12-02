from sqlalchemy import create_engine
from src.config.settings import settings

def get_db_engine():
    return create_engine(settings.DATABASE_URL)

# Instancia global para reutilización (Singleton pattern implícito)
engine = get_db_engine()
