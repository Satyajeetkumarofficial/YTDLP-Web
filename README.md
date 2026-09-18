# YTDLP Web / MediaFlow PRO v8

Koyeb-ready Flask + yt-dlp downloader with native browser streaming.

## Improvements in v8
- `yt-dlp[default,curl-cffi]` for current extractors, EJS and browser-impersonation support.
- Deno JavaScript runtime for modern yt-dlp extraction.
- Normal yt-dlp extraction first; Chrome impersonation is used only as a fallback, not globally.
- Generic embedded-media fallback for pages exposing MP4/WebM/HLS URLs in HTML/OpenGraph/video/source markup.
- HLS fallback is piped through FFmpeg directly to the browser.
- No server-side media file storage.
- Real client IP handling behind Koyeb proxy for rate limiting.
- Tiny range probe before bitrate estimate for format-size display.
- Native browser download; no JS Blob buffering.

## Limits
No downloader can guarantee every site. Anti-bot, DRM, login requirements, geo restrictions, expiring URLs and site changes can still prevent extraction.

## Cookies
Put a Netscape-format `cookies.txt` in the project if a site needs authenticated cookies. Keep the repository private when cookies are real credentials.
