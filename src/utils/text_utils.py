def bytes_to_text(data: bytes) -> str:
    """
    Convierte bytes a string UTF-8 seguro.
    """
    return data.decode("utf-8", errors="ignore")
