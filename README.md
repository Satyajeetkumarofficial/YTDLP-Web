# MediaFlow PRO — Koyeb Ready v5

- Koyeb `$PORT` + `0.0.0.0`
- FFmpeg + Deno
- Server-side `cookies.txt` (no user upload UI)
- Dynamic yt-dlp formats
- Size: metadata -> tiny HTTP range probe -> bitrate estimate
- Temporary processing only
- Native browser download (no fetch/blob buffering)
- Signed download tokens
- Clean download UI; processing runs in the background until the native browser download starts
- Automatic cleanup after browser response
- One Gunicorn worker so in-memory jobs/tokens remain consistent

## cookies.txt
Place your real Netscape-format `cookies.txt` in the repository root if required.
Do NOT expose it in frontend code or logs. For a public GitHub repository, use a private repo or preferably a Koyeb secret/mounted secret instead.

## v7 fix
The download concurrency slot is now released in a `finally` block after every successful or failed download, preventing the server from becoming permanently busy after the first job.


### v13 YouTube support

This build includes yt-dlp's default EJS dependencies and the bgutil PO-token provider. YouTube still may restrict some videos, clients, or accounts, but the application no longer creates a generic fake format when YouTube extraction fails.
