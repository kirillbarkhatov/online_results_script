FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=2.1.4 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

# Install Poetry and project dependencies strictly by poetry.lock
RUN python -m pip install --upgrade pip && \
    python -m pip install "poetry==${POETRY_VERSION}"

COPY pyproject.toml poetry.lock README.md /app/
RUN poetry install --only main --no-ansi --no-root

COPY online_results /app/online_results
RUN poetry install --only main --no-ansi

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "online_results.api_app:app", "--host", "0.0.0.0", "--port", "8000"]
