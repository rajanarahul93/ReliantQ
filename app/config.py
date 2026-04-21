from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://user:password@localhost:5432/jobs_db"
    REDIS_URL: str = "redis://localhost:6379/0"
    
    MAX_QUEUE_SIZE: int = 5000
    STUCK_JOB_TIMEOUT_MINUTES: int = 10
    
    class Config:
        env_file = ".env"

settings = Settings()
