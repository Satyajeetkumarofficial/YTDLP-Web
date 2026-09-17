# MediaFlow PRO — Koyeb Ready

- Analyze URL before download.
- Shows available video/audio formats.
- Size: yt-dlp metadata -> bitrate estimate -> tiny Range probe.
- Koyeb uses only temporary processing storage.
- Temporary files are deleted after browser response closes.
- Stable HMAC download tokens fix the previous 404.
- Progress endpoint shows percentage, bytes, speed and ETA.
- cookies.txt upload supported; cookie contents are not logged.
- One Gunicorn worker is intentional because jobs are in memory.
- Set MEDIAFLOW_SECRET in Koyeb.
- Exact size may remain unavailable when the origin hides total length.
