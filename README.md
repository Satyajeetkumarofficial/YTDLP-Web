# MediaFlow PRO — Koyeb Ready v5

- Koyeb `$PORT` + `0.0.0.0`
- FFmpeg + Deno
- Server-side `cookies.txt` (no user upload UI)
- Dynamic yt-dlp formats
- Size: metadata -> tiny HTTP range probe -> bitrate estimate
- Temporary processing only
- Native browser download (no fetch/blob buffering)
- Signed download tokens
- Real-time percent / speed / ETA
- Automatic cleanup after browser response
- One Gunicorn worker so in-memory jobs/tokens remain consistent

## cookies.txt
Place your real Netscape-format `cookies.txt` in the repository root if required.
Do NOT expose it in frontend code or logs. For a public GitHub repository, use a private repo or preferably a Koyeb secret/mounted secret instead.
