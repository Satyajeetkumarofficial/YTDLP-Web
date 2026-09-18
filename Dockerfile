FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    ca-certificates \
    unzip \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Deno for yt-dlp YouTube EJS support
RUN curl -fsSL https://deno.land/install.sh | sh

ENV PATH="/root/.deno/bin:$PATH"

# Install YouTube PO Token provider
RUN git clone --depth 1 --branch 2.0.0 \
    https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git \
    /opt/bgutil-ytdlp-pot-provider \
    && cd /opt/bgutil-ytdlp-pot-provider/server \
    && deno install --allow-scripts=npm:canvas --frozen

WORKDIR /app

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application
COPY ytdlp_gui.py .

# Server-side cookies
# cookies.txt must exist in the GitHub repository
COPY cookies.txt* /app/

EXPOSE 8000

# Koyeb
CMD ["sh", "-c", "gunicorn --workers 1 --threads 4 --timeout 0 --bind 0.0.0.0:${PORT:-8000} ytdlp_gui:app"]
