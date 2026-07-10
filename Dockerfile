FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./main.py
COPY static ./static
COPY workflows ./workflows

RUN mkdir -p /app/API /app/data/conversations /app/data/canvases /app/output /app/assets/library

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:3000/', timeout=5).read(1)"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3000", "--proxy-headers"]
