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


## Logging
All important requests and lifecycle events are printed to the Koyeb service logs:
- health checks
- format analysis requests
- format-analysis failures
- download requests
- completed output size
- browser response start
- temporary cleanup
- download errors

## Users / protection
There is **no hardcoded total-user limit**. Multiple users can use the service subject to Koyeb CPU/RAM/bandwidth and the configured concurrent-download semaphore.

Protection is token-based for downloads: each detected format receives a signed token. Tokens are validated before a download starts and jobs expire automatically.

A small per-IP request rate limit remains enabled as abuse protection. It is not a user-count limit and can be adjusted with:
`RATE_LIMIT_PER_MINUTE` (default 30).


## Detailed Koyeb analysis logs
For each URL analysis, the Koyeb logs now include a safe summary of:
- extractor/site name
- media title
- uploader/channel when available
- duration when available
- number of detected download options
- source webpage URL
- each returned format's type, label, detail, estimated size and yt-dlp selector
- generated job ID
- final analysis response count
- selected format when a download starts

Cookies, secret tokens and uploaded cookie-file contents are not printed.


## Full media engine upgrade
This build adds:
- `yt-dlp[default]` including the companion `yt-dlp-ejs` package
- Deno JavaScript runtime for current YouTube challenge solving
- FFmpeg + FFprobe
- `curl-cffi` for sites that benefit from browser-like HTTP/TLS impersonation
- `aria2c` as an external downloader/accelerator for direct HTTP/HTTPS transfers
- native yt-dlp handling remains available for HLS/DASH and other extractor-managed streams

The official yt-dlp documentation currently recommends FFmpeg/FFprobe, yt-dlp-ejs and a supported JS runtime; Deno is the recommended runtime. See the project documentation before changing runtime versions.
