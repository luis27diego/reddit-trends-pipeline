import requests

def download_file(url: str) -> bytes:
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()
    return response.content
