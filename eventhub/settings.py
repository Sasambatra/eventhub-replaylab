from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL

ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ROOT / ".env", extra="ignore", case_sensitive=False)
    app_mode: str = "demo"
    db_host: str = "127.0.0.1"
    db_port: int = 55432
    db_name: str = "eventhub"
    db_user: str = "eventhub"
    db_password: SecretStr
    migration_password: SecretStr = SecretStr("")
    rabbitmq_host: str = "127.0.0.1"
    rabbitmq_port: int = 5673
    rabbitmq_user: str = "eventhub"
    rabbitmq_password: SecretStr
    rabbitmq_vhost: str = "eventhub"
    key_hash_pepper: SecretStr
    secret_encryption_key: SecretStr
    lab_a_token: SecretStr
    lab_b_token: SecretStr
    lab_a_url: str = "http://127.0.0.1:8111"
    lab_b_url: str = "http://127.0.0.1:8112"
    max_event_bytes: int = 65536
    max_case_bytes: int = 1048576
    max_attempts: int = Field(default=5, ge=1, le=20)
    delivery_ttl_seconds: int = Field(default=86400, ge=10)
    http_timeout_seconds: float = Field(default=10, gt=0, le=30)
    lease_seconds: int = Field(default=60, ge=5)
    global_concurrency: int = Field(default=16, ge=1, le=128)
    project_concurrency: int = Field(default=8, ge=1, le=64)
    destination_concurrency: int = Field(default=2, ge=1, le=16)
    session_seconds: int = 3600
    event_retention_days: int = 7
    history_retention_days: int = 30
    otel_exporter_otlp_endpoint: str = ""
    secure_cookie: bool = False

    @model_validator(mode="after")
    def validate_profile(self):
        if self.app_mode != "demo":
            raise ValueError("Only the isolated demo profile is supported; public access is disabled")
        if self.lease_seconds <= self.http_timeout_seconds + 2:
            raise ValueError("Lease must exceed the HTTP request budget")
        if len(self.key_hash_pepper.get_secret_value()) < 32:
            raise ValueError("A private random key pepper is required")
        return self

    def database_url(self, *, migration=False, database=None):
        return URL.create(
            "postgresql+psycopg",
            username="eventhub_migrator" if migration else self.db_user,
            password=(self.migration_password if migration else self.db_password).get_secret_value(),
            host=self.db_host,
            port=self.db_port,
            database=database or self.db_name,
        )

    def for_tests(self):
        return self.model_copy(update={"db_name": "eventhub_test", "rabbitmq_vhost": "eventhub_test"})


@lru_cache
def settings():
    return Settings()
