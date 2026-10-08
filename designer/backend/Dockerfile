FROM python:3.12-slim

WORKDIR /app

COPY designer/backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml /tmp/datamart/pyproject.toml
COPY src /tmp/datamart/src
RUN pip install --no-cache-dir /tmp/datamart
COPY designer/backend/app ./app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
