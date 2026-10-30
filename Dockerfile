FROM python:3.14-slim@sha256:cd89c30eda26ee911a4611b18680086e0ea42e6f5f8cbe653d07fa7958c9cec8
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.lock /app/
RUN python -m pip install --no-cache-dir --require-hashes --only-binary=:all: -r requirements.lock
COPY eventhub /app/eventhub
COPY lab /app/lab
COPY migrations /app/migrations
COPY alembic.ini /app/
RUN mkdir /app/.local && chown 1000:1000 /app/.local
USER 1000:1000
EXPOSE 8100
CMD ["python", "-m", "eventhub", "api", "--host", "0.0.0.0", "--port", "8100"]
