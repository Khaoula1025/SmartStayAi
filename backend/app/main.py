from fastapi import FastAPI
from app.api.v1.endpoints.auth import authRouter
from app.models.user import User
from app.db.session import Base, engine

app = FastAPI(title="Smart Stay Intelligence")
try:
    Base.metadata.create_all(bind=engine)
except Exception as e:
    print(f"⚠️  DB not available at startup: {e}")

app.include_router(authRouter)

@app.get("/")
def root():
    return {"message": "API Smart Stay Intelligence"}

@app.get("/health")
def health():
    return {"status": "healthy"}
