from fastapi import FastAPI

app = FastAPI(title="Smart Stay Intelligence")

@app.get("/")
def root():
    return {"message": "API Smart Stay Intelligence"}

@app.get("/health")
def health():
    return {"status": "healthy"}
