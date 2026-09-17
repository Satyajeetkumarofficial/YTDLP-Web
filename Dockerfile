FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt gunicorn
COPY ytdlp_gui.py .
ENV MAX_CONCURRENT_DOWNLOADS=2
ENV MAX_FILE_GB=4
CMD ["sh","-c","gunicorn --workers 1 --threads 4 --timeout 0 --bind 0.0.0.0:${PORT:-8000} ytdlp_gui:app"]
