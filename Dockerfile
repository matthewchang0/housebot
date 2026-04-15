FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY house /app/house
COPY app.py /app/app.py

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

CMD ["house", "run"]
