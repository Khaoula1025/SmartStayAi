from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "Smart Stay Intelligence"
    VERSION: str = "1.0.0"
    API_V1_PREFIX: str = "/api/v1"
    SECRET_KEY: str = "change-me"
    DATABASE_URL: str = "postgresql://localhost/smartstay"
    
    class Config:
        env_file = ".env"

settings = Settings()
