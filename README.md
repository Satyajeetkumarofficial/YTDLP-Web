# MediaFlow Pro — Koyeb

## Deploy
Use Docker build/deploy or connect this repository to Koyeb.

Recommended environment variables:
- `MEDIAFLOW_SECRET` = long random secret (recommended)
- `MAX_CONCURRENT_DOWNLOADS` = `2`
- `MAX_FILE_GB` = `4`

Health endpoint:
`/health`

## Important behavior
The app does not maintain a permanent downloads directory. Each requested download is written to a temporary job directory and removed after the browser response closes.

The browser starts a normal attachment download when the user clicks a format button.

## Notes
- `yt-dlp` support changes as websites change; no downloader can guarantee every website.
- Only download content you have permission to download.
- Server-side temporary disk space is still required while a file is being processed.
