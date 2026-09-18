FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg curl ca-certificates unzip git \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://deno.land/install.sh | sh
ENV PATH="/root/.deno/bin:$PATH"

# YouTube Proof-of-Origin token provider (HTTP server mode).
# HTTP mode avoids spawning a fresh Deno process for every yt-dlp call.
RUN git clone --depth 1 --branch 2.0.0 https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git /opt/bgutil-ytdlp-pot-provider \
    && cd /opt/bgutil-ytdlp-pot-provider/server \
    && deno install --allow-scripts=npm:canvas --frozen

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ytdlp_gui.py .
# Put your real Netscape-format cookies.txt in the same GitHub repository.
# It is copied into the image only if your repository contains it.
COPY cookies.txt* /app/

EXPOSE 8000

CMD ["sh", "-c", "cd /opt/bgutil-ytdlp-pot-provider/server && deno run --allow-env --allow-net --allow-ffi=/opt/bgutil-ytdlp-pot-provider/server/node_modules --allow-read=/opt/bgutil-ytdlp-pot-provider/server/node_modules /opt/bgutil-ytdlp-pot-provider/server/src/main.ts --host 127.0.0.1 --port 4416 & exec gunicorn --workers 1 --threads 4 --timeout 0 --bind 0.0.0.0:${PORT:-8000} ytdlp_gui:app"]
