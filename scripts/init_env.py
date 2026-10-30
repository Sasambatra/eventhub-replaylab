"""Create private local configuration once; never print credentials."""

import base64
import os
from pathlib import Path
import secrets

ROOT = Path(__file__).resolve().parent.parent


def create():
    path = ROOT / ".env"
    if path.exists():
        print("Existing .env preserved.")
        return
    values = {
        "APP_MODE": "demo",
        "LOCAL_UID": str(os.getuid() or 1000),
        "LOCAL_GID": str(os.getgid() or 1000),
        "DB_HOST": "127.0.0.1",
        "DB_PORT": "55432",
        "DB_NAME": "eventhub",
        "DB_USER": "eventhub",
        "DB_PASSWORD": secrets.token_hex(32),
        "MIGRATION_PASSWORD": secrets.token_hex(32),
        "POSTGRES_PASSWORD": secrets.token_hex(32),
        "RABBITMQ_HOST": "127.0.0.1",
        "RABBITMQ_PORT": "5673",
        "RABBITMQ_USER": "eventhub",
        "RABBITMQ_VHOST": "eventhub",
        "RABBITMQ_PASSWORD": secrets.token_hex(32),
        "KEY_HASH_PEPPER": secrets.token_hex(32),
        "SECRET_ENCRYPTION_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
        "LAB_A_TOKEN": secrets.token_hex(32),
        "LAB_B_TOKEN": secrets.token_hex(32),
        "LAB_A_DB_PASSWORD": secrets.token_hex(32),
        "LAB_B_DB_PASSWORD": secrets.token_hex(32),
        "LAB_A_URL": "http://127.0.0.1:8111",
        "LAB_B_URL": "http://127.0.0.1:8112",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "",
        "SECURE_COOKIE": "false",
    }
    with os.fdopen(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "w") as handle:
        handle.write("\n".join(f"{k}={v}" for k, v in values.items()) + "\n")
    local = ROOT / ".local"
    local.mkdir(mode=0o700, exist_ok=True)
    print("Created private .env; credentials were not printed.")


if __name__ == "__main__":
    create()
