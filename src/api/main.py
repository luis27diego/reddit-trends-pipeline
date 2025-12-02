# src/api/main.py
from fastapi import FastAPI

app = FastAPI(title="Reddit Analytics API")

@app.get("/")
def read_root():
    return {"message": "API de Reddit Analytics funcionando 🚀"}