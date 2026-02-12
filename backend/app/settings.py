from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="POLARIS_", case_sensitive=False)

    database_url: str = "postgresql+psycopg://polaris:polaris@127.0.0.1:5433/polaris"
    redis_url: str = "redis://localhost:6380/0"

    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "staging-txns"


settings = Settings()
