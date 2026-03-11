from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.endpoints.auth       import authRouter
from app.api.v1.endpoints.prediction import predictionRouter
from app.api.v1.endpoints.analytics  import analyticsRouter
from app.api.v1.endpoints.rates      import ratesRouter
from app.api.v1.endpoints.pipeline   import pipelineRouter
from app.db.session import Base, engine
from app.db.init_db import init_db

app = FastAPI(title="SmartStay Intelligence API")

app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["http://localhost:3000", "http://localhost:5173"],
    allow_credentials = True,   # required for cookie auth
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)

# Create tables and seed predictions on startup
try:
    init_db()
except Exception as e:
    print(f"⚠️  DB init failed: {e}")

# Routers
app.include_router(authRouter,       prefix="/api/v1")
app.include_router(predictionRouter, prefix="/api/v1")
app.include_router(analyticsRouter,  prefix="/api/v1")
app.include_router(ratesRouter,      prefix="/api/v1")
app.include_router(pipelineRouter,   prefix="/api/v1")


@app.get("/")
def root():
    return {"message": "SmartStay Intelligence API"}


@app.get("/health")
def health():
    return {"status": "healthy"}