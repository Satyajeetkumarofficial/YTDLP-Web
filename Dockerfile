FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
ENV DENO_INSTALL=/usr/local

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg aria2 curl ca-certificates unzip \
    && rm -rf /var/lib/apt/lists/*

# Deno is the recommended JS runtime for yt-dlp EJS.
RUN curl -fsSL https://deno.land/install.sh | sh \
    && mv /usr/local/bin/deno /usr/local/bin/deno.real 2>/dev/null || true
# The installer location differs by image/user; normalize PATH if needed.
ENV PATH="/root/.deno:/usr/local/bin:${PATH}"

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt gunicorn

COPY ytdlp_gui.py .

ENV MAX_CONCURRENT_DOWNLOADS=2
ENV MAX_FILE_GB=4
ENV RATE_LIMIT_PER_MINUTE=30

CMD ["sh","-c","gunicorn --workers 1 --threads 4 --timeout 0 --bind 0.0.0.0:${PORT:-8000} ytdlp_gui:app"]
