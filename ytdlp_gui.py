import os, re, time, uuid, hmac, json, socket, hashlib, tempfile, threading, ipaddress, shutil, logging
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify, render_template_string, send_file
import yt_dlp

try:
    import requests
except ImportError:
    requests = None

app = Flask(__name__)
PORT = int(os.getenv("PORT", "8000"))
SECRET = os.getenv("MEDIAFLOW_SECRET") or uuid.uuid4().hex + uuid.uuid4().hex
JOB_TTL = int(os.getenv("JOB_TTL", "1800"))
RATE_LIMIT = int(os.getenv("RATE_LIMIT_PER_MINUTE", "30"))
MAX_FILE_GB = float(os.getenv("MAX_FILE_GB", "4"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "2"))
MAX_TOTAL_STORAGE_GB = float(os.getenv("MAX_TOTAL_STORAGE_GB", "6"))
REAP_INTERVAL = int(os.getenv("REAP_INTERVAL_SECONDS", "45"))
PROBE_TIMEOUT = float(os.getenv("PROBE_TIMEOUT_SECONDS", "4"))
PROBE_WORKERS = int(os.getenv("PROBE_WORKERS", "8"))
COOKIES_FILE = os.getenv("COOKIES_FILE", "cookies.txt")
TMP_ROOT = os.path.join(tempfile.gettempdir(), "mediaflow")
os.makedirs(TMP_ROOT, exist_ok=True)

JOBS, RATE = {}, {}
LOCK = threading.RLock()
SEM = threading.BoundedSemaphore(MAX_CONCURRENT)
PROBE_POOL = ThreadPoolExecutor(max_workers=PROBE_WORKERS)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("mediaflow")


def hs(n):
    if n is None:
        return "Size unavailable"
    n = float(n)
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024
        i += 1
    return f"{n:.1f} {units[i]}" if i else f"{int(n)} B"


def hspeed(n):
    return hs(n) + "/s" if n else "—"


def heta(n):
    if n is None:
        return "—"
    n = int(n)
    m, s = divmod(n, 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")


def hdur(n):
    if not n:
        return None
    n = int(n)
    m, s = divmod(n, 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def safe(name):
    return (re.sub(r'[\\/:*?"<>|]+', "_", name or "download")[:180].strip(" .") or "download")


def client_ip():
    return request.remote_addr or "unknown"


def rate_ok():
    now = time.time()
    key = client_ip()
    with LOCK:
        arr = [x for x in RATE.get(key, []) if now - x < 60]
        if len(arr) >= RATE_LIMIT:
            RATE[key] = arr
            return False
        arr.append(now)
        RATE[key] = arr
    return True


def valid_url(url):
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https") or not p.hostname:
            return False
        host = p.hostname.lower().rstrip(".")
        if host == "localhost" or host.endswith(".local"):
            return False
        for item in socket.getaddrinfo(host, None):
            addr = ipaddress.ip_address(item[4][0])
            if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved:
                return False
        return True
    except Exception:
        return False


def make_token(job_id, fmt_index):
    raw = json.dumps({"job": job_id, "fmt": fmt_index}, separators=(",", ":"), sort_keys=True).encode()
    sig = hmac.new(SECRET.encode(), raw, hashlib.sha256).hexdigest().encode()
    import base64
    return base64.urlsafe_b64encode(raw + b"." + sig).decode().rstrip("=")


def check_token(token):
    try:
        import base64
        blob = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
        raw, sig = blob.rsplit(b".", 1)
        expected = hmac.new(SECRET.encode(), raw, hashlib.sha256).hexdigest().encode()
        if not hmac.compare_digest(sig, expected):
            return None
        return json.loads(raw.decode())
    except Exception:
        return None


def cookie_path():
    p = Path(COOKIES_FILE)
    return str(p) if p.is_file() and p.stat().st_size > 0 else None


# ---------------------------------------------------------------------------
# Storage accounting — keeps MediaFlow inside a small hosting disk (Koyeb etc.)
# ---------------------------------------------------------------------------

def dir_size_bytes(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


def storage_ok_for(extra_gb):
    limit = MAX_TOTAL_STORAGE_GB * 1024**3
    used = dir_size_bytes(TMP_ROOT)
    return (used + extra_gb * 1024**3) <= limit


def reaper():
    """Background sweep: removes stale job workdirs so temp storage never
    grows unbounded, even if a browser never comes back to claim a file."""
    while True:
        time.sleep(REAP_INTERVAL)
        now = time.time()
        stale = []
        with LOCK:
            for job_id, job in list(JOBS.items()):
                status = job.get("status")
                age = now - job.get("created", now)
                if status in ("downloading", "starting"):
                    continue
                if status in ("ready", "error") and age > JOB_TTL:
                    stale.append((job_id, job.get("workdir")))
                elif status == "analyzed" and age > JOB_TTL * 2:
                    stale.append((job_id, None))
        for job_id, workdir in stale:
            cleanup_job(job_id, workdir)
        if stale:
            log.info("REAPER cleaned %d stale job(s)", len(stale))


threading.Thread(target=reaper, daemon=True).start()


# ---------------------------------------------------------------------------
# Format discovery — sized concurrently and deduplicated before any network
# probing happens, so "Analyze" stays fast even on sources with 30+ streams.
# ---------------------------------------------------------------------------

def tiny_probe(url):
    """1-byte range probe, bounded by PROBE_TIMEOUT. Never downloads the body."""
    if not requests or not url:
        return None
    try:
        r = requests.get(
            url,
            headers={"Range": "bytes=0-0", "User-Agent": "Mozilla/5.0"},
            stream=True,
            timeout=PROBE_TIMEOUT,
            allow_redirects=True,
        )
        cr = r.headers.get("Content-Range", "")
        cl = r.headers.get("Content-Length")
        m = re.search(r"/(\d+)$", cr)
        if m:
            size = int(m.group(1))
        elif r.status_code == 200 and cl and cl.isdigit():
            size = int(cl)
        else:
            size = None
        r.close()
        return size
    except Exception:
        return None


def bitrate_estimate(fmt, duration):
    if duration and fmt.get("tbr"):
        try:
            return int(float(fmt["tbr"]) * 1000 / 8 * float(duration))
        except Exception:
            pass
    return None


def resolve_size(fmt, duration):
    """Returns (bytes, exact). Only probes the network as a last resort, and
    only for the small, already-deduplicated set of formats we plan to show."""
    for key in ("filesize", "filesize_approx"):
        if fmt.get(key):
            return int(fmt[key]), True

    estimate = bitrate_estimate(fmt, duration)
    probed = tiny_probe(fmt.get("url"))
    if probed is not None:
        return probed, True
    if estimate is not None:
        return estimate, False
    return None, False


def size_label(bytes_, exact):
    if bytes_ is None:
        return "Size unavailable"
    return hs(bytes_) if exact else "≈ " + hs(bytes_)


def reslabel(fmt):
    w, h = fmt.get("width"), fmt.get("height")
    if not h:
        return "Audio"
    if w:
        x = max(int(w), int(h))
        known = {2160: "2160p", 1440: "1440p", 1080: "1080p", 720: "720p",
                 480: "480p", 360: "360p", 240: "240p", 144: "144p"}
        return known.get(x, f"{w}×{h}")
    return f"{h}p"


def pick_container(video, audio):
    """One sane container per resolution instead of always offering both
    MP4 and MKV — halves the list size, which is most of what made the
    results feel sluggish to render and probe."""
    vext = (video.get("ext") or "").lower()
    aext = (audio.get("ext") or "").lower() if audio else ""
    if vext in ("mp4", "m4v") and (not audio or aext in ("m4a", "mp4", "aac")):
        return "mp4"
    return "mkv"


def make_formats(info):
    duration = info.get("duration")
    videos, audios = [], []

    for raw in info.get("formats") or []:
        if not raw.get("format_id"):
            continue
        if raw.get("vcodec") not in (None, "none"):
            videos.append(raw)
        elif raw.get("acodec") not in (None, "none"):
            audios.append(raw)

    # Rank first, size later — sizing is the expensive part.
    audios.sort(key=lambda x: x.get("abr") or 0, reverse=True)
    best_audio = audios[0] if audios else None

    videos.sort(
        key=lambda x: (x.get("height") or 0, x.get("width") or 0, x.get("tbr") or 0),
        reverse=True,
    )
    deduped_videos, seen = [], set()
    for video in videos:
        key = (video.get("width"), video.get("height"))
        if key in seen:
            continue
        seen.add(key)
        deduped_videos.append(video)

    top_audios = audios[:8]

    # Size every format we might actually show, concurrently, once each.
    to_size = list(deduped_videos) + list(top_audios)
    if best_audio is not None and best_audio not in to_size:
        to_size.append(best_audio)

    sizes = {}
    futures = {
        id(fmt): PROBE_POOL.submit(resolve_size, fmt, duration)
        for fmt in to_size
    }
    for fmt in to_size:
        try:
            sizes[id(fmt)] = futures[id(fmt)].result(timeout=PROBE_TIMEOUT + 2)
        except Exception:
            sizes[id(fmt)] = (bitrate_estimate(fmt, duration), False)

    audio_size = sizes.get(id(best_audio)) if best_audio else (None, False)

    out = []
    for video in deduped_videos:
        vbytes, vexact = sizes.get(id(video), (None, False))
        total_bytes, total_exact = vbytes, vexact
        if best_audio and vbytes is not None and audio_size[0] is not None:
            total_bytes = vbytes + audio_size[0]
            total_exact = vexact and audio_size[1]

        container = pick_container(video, best_audio)
        selector = video["format_id"] + (f"+{best_audio['format_id']}" if best_audio else "")
        out.append({
            "kind": "video",
            "label": f"{reslabel(video)} • {container.upper()}",
            "detail": "Video + best audio",
            "size": size_label(total_bytes, total_exact),
            "size_bytes": total_bytes,
            "selector": selector,
            "container": container,
        })

    for audio in top_audios:
        ext = (audio.get("ext") or "m4a").lower()
        abr = audio.get("abr")
        abytes, aexact = sizes.get(id(audio), (None, False))
        out.append({
            "kind": "audio",
            "label": f"{ext.upper()} • {int(abr) if abr else '?'} kbps",
            "detail": "Original audio",
            "size": size_label(abytes, aexact),
            "size_bytes": abytes,
            "selector": audio["format_id"],
            "container": ext,
        })

    return out[:40]


def cleanup_job(job_id, workdir=None):
    if workdir:
        shutil.rmtree(workdir, ignore_errors=True)
    with LOCK:
        JOBS.pop(job_id, None)
    log.info("CLEANUP job=%s temporary files removed", job_id)


@app.after_request
def security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/health")
def health():
    return jsonify(ok=True, service="MediaFlow PRO")


@app.get("/")
def index():
    return render_template_string(PAGE)


@app.post("/api/formats")
def analyze():
    if not rate_ok():
        return jsonify(error="Too many requests. Please wait a moment."), 429

    url = (request.form.get("url") or "").strip()
    if not valid_url(url):
        return jsonify(error="Invalid or blocked URL"), 400

    job_id = uuid.uuid4().hex
    cp = cookie_path()
    t0 = time.time()

    try:
        opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "noplaylist": True,
            "retries": 3,
            "fragment_retries": 3,
            "socket_timeout": 20,
            "js_runtimes": {"deno": {}},
        }
        if cp:
            opts["cookiefile"] = cp
            log.info("Using server cookies file: %s", cp)

        log.info("ANALYZE request ip=%s url=%s", client_ip(), url)

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)

        formats = make_formats(info)
        JOBS[job_id] = {
            "created": time.time(),
            "url": url,
            "title": info.get("title") or url,
            "formats": formats,
            "status": "analyzed",
            "progress": {},
        }

        log.info(
            "ANALYSIS RESULT | extractor=%s | title=%s | duration=%s | formats=%d | took=%.2fs",
            info.get("extractor"), info.get("title"), info.get("duration"),
            len(formats), time.time() - t0,
        )

        result = []
        for i, fmt in enumerate(formats):
            item = dict(fmt, token=make_token(job_id, i))
            item.pop("selector", None)
            item.pop("size_bytes", None)
            result.append(item)

        return jsonify(
            job_id=job_id,
            title=JOBS[job_id]["title"],
            thumbnail=info.get("thumbnail"),
            uploader=info.get("uploader"),
            duration=hdur(info.get("duration")),
            formats=result,
        )

    except Exception as exc:
        log.exception("ANALYZE failed")
        return jsonify(error=str(exc)), 500


def worker(job_id, fmt, workdir):
    job = JOBS[job_id]
    st = {
        "percent": 0,
        "downloaded": "0 B",
        "total": fmt["size"],
        "speed": "—",
        "eta": "—",
    }
    job["progress"] = st
    job["status"] = "downloading"

    try:
        cp = cookie_path()

        def hook(data):
            if data.get("status") == "downloading":
                total = data.get("total_bytes") or data.get("total_bytes_estimate") or 0
                done = data.get("downloaded_bytes") or 0
                st.update(
                    percent=round(done * 100 / total, 1) if total else 0,
                    downloaded=hs(done),
                    total=hs(total) if total else fmt["size"],
                    speed=hspeed(data.get("speed")),
                    eta=heta(data.get("eta")),
                )
            elif data.get("status") == "finished":
                st["percent"] = 100

        opts = {
            "format": fmt["selector"],
            "outtmpl": os.path.join(workdir, "%(title)s [%(id)s].%(ext)s"),
            "noplaylist": True,
            "restrictfilenames": True,
            "quiet": True,
            "no_warnings": True,
            "retries": 5,
            "fragment_retries": 5,
            "concurrent_fragment_downloads": 4,
            "socket_timeout": 30,
            "max_filesize": int(MAX_FILE_GB * 1024**3),
            "merge_output_format": fmt.get("container"),
            "progress_hooks": [hook],
            "js_runtimes": {"deno": {}},
            "paths": {"home": workdir, "temp": workdir},
        }
        if cp:
            opts["cookiefile"] = cp

        log.info("DOWNLOAD START | job=%s | label=%s | selector=%s", job_id, fmt["label"], fmt["selector"])

        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([job["url"]])

        files = [
            p for p in Path(workdir).iterdir()
            if p.is_file() and p.name != os.path.basename(COOKIES_FILE)
        ]
        if not files:
            raise RuntimeError("No output file was produced")

        output = max(files, key=lambda p: p.stat().st_size)
        job.update(
            status="ready",
            result=str(output),
            filename=safe(output.name),
        )
        st["percent"] = 100
        st["total"] = hs(output.stat().st_size)
        st["downloaded"] = hs(output.stat().st_size)

        log.info("DOWNLOAD READY | job=%s | file=%s | size=%s", job_id, output.name, hs(output.stat().st_size))

    except Exception as exc:
        job.update(status="error", error=str(exc))
        log.exception("DOWNLOAD failed job=%s", job_id)
        shutil.rmtree(workdir, ignore_errors=True)
    finally:
        try:
            SEM.release()
        except Exception:
            log.exception("Failed to release download slot | job=%s", job_id)


@app.post("/api/start/<token>")
def start_download(token):
    payload = check_token(token)
    if not payload:
        return jsonify(error="Invalid or expired download token"), 404

    job_id = payload["job"]
    job = JOBS.get(job_id)
    if not job:
        return jsonify(error="Download job expired or not found"), 404

    if time.time() - job["created"] > JOB_TTL:
        JOBS.pop(job_id, None)
        return jsonify(error="Download link expired"), 410

    try:
        fmt = job["formats"][int(payload["fmt"])]
    except (ValueError, TypeError, KeyError, IndexError):
        return jsonify(error="Format not found"), 404

    with LOCK:
        if job.get("status") == "ready" and job.get("result"):
            return jsonify(status="ready", download_url=f"/download/{token}")
        if job.get("status") == "downloading":
            return jsonify(status="downloading")
        if job.get("status") == "error":
            return jsonify(status="error", error=job.get("error"))

        if not storage_ok_for(MAX_FILE_GB):
            return jsonify(error="Server storage is nearly full. Please try again shortly."), 503

        if not SEM.acquire(blocking=False):
            return jsonify(error="All download slots are currently in use. Please try again in a few seconds."), 503

        workdir = tempfile.mkdtemp(prefix="mediaflow-", dir=TMP_ROOT)
        job["workdir"] = workdir
        job["status"] = "starting"

    threading.Thread(target=worker, args=(job_id, fmt, workdir), daemon=True).start()
    return jsonify(status="starting")


@app.get("/api/progress/<token>")
def progress(token):
    payload = check_token(token)
    if not payload or payload["job"] not in JOBS:
        return jsonify(error="expired"), 404

    job = JOBS[payload["job"]]
    return jsonify(
        status=job.get("status"),
        progress=job.get("progress", {}),
        error=job.get("error"),
        download_url=f"/download/{token}" if job.get("status") == "ready" else None,
    )


@app.get("/download/<token>")
def download(token):
    payload = check_token(token)
    if not payload:
        return "Invalid or expired download link", 404

    job_id = payload["job"]
    job = JOBS.get(job_id)
    if not job:
        return "Download job expired or not found", 404

    if time.time() - job["created"] > JOB_TTL:
        return "Download link expired", 410

    if job.get("status") != "ready" or not job.get("result"):
        return "File is not ready yet. Please wait for processing to finish.", 409

    result = job["result"]
    if not os.path.isfile(result):
        return "Temporary file is no longer available. Please start the download again.", 410

    response = send_file(
        result,
        as_attachment=True,
        download_name=job.get("filename") or "download",
        max_age=0,
        conditional=True,
    )

    @response.call_on_close
    def cleanup_response():
        cleanup_job(job_id, job.get("workdir"))

    return response


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>MediaFlow PRO</title>
<style>
:root{
  --bg:#070a10;--panel:rgba(22,29,43,.72);--panel-solid:#131a27;--line:rgba(255,255,255,.08);
  --text:#f5f7fb;--muted:#8d9bb0;--blue:#5aa8ff;--blue2:#8f6bff;--green:#33d19a;--danger:#ff6b78;
  --radius:18px;
}
*{box-sizing:border-box}
html,body{height:100%}
body{
  margin:0;color:var(--text);
  font:15px/1.5 "Segoe UI",system-ui,-apple-system,BlinkMacSystemFont,sans-serif;
  background:
    radial-gradient(1100px 600px at 12% -10%, rgba(90,168,255,.16), transparent 60%),
    radial-gradient(900px 500px at 110% 10%, rgba(143,107,255,.14), transparent 55%),
    var(--bg);
  min-height:100%;
}
.wrap{max-width:860px;margin:auto;padding:42px 18px 80px}
.header{margin-bottom:26px;display:flex;align-items:center;gap:14px}
.logo{width:44px;height:44px;border-radius:13px;flex:none;
  background:linear-gradient(135deg,var(--blue),var(--blue2));
  display:flex;align-items:center;justify-content:center;font-weight:900;font-size:18px;color:#07111e;
  box-shadow:0 8px 24px rgba(90,168,255,.35)}
.brand{font-size:26px;font-weight:800;letter-spacing:-.5px}
.sub{color:var(--muted);margin-top:2px;font-size:13.5px}

.panel{
  background:var(--panel);backdrop-filter:blur(18px);-webkit-backdrop-filter:blur(18px);
  border:1px solid var(--line);border-radius:var(--radius);padding:18px;
  box-shadow:0 20px 50px rgba(0,0,0,.35);
}
.inputrow{display:flex;gap:10px}
.url{width:100%;min-width:0;background:#0b1119;color:#fff;border:1px solid #263248;border-radius:13px;
  padding:14px 15px;font-size:15px;outline:none;transition:border-color .15s,box-shadow .15s}
.url:focus{border-color:var(--blue);box-shadow:0 0 0 4px rgba(90,168,255,.14)}
.url::placeholder{color:#5b6a80}
.btn{border:0;border-radius:13px;padding:13px 22px;font-weight:800;cursor:pointer;
  background:linear-gradient(135deg,var(--blue),#4a8fe0);color:#07111e;white-space:nowrap;
  transition:transform .12s ease,filter .12s ease,opacity .12s ease}
.btn:hover:not(:disabled){filter:brightness(1.08);transform:translateY(-1px)}
.btn:active:not(:disabled){transform:translateY(0)}
.btn:disabled{opacity:.5;cursor:not-allowed;transform:none}
.msg{min-height:20px;color:var(--muted);margin-top:12px;font-size:13.5px;transition:color .15s}
.note{color:#5b6a80;font-size:12.5px;margin-top:10px}
.error{color:var(--danger)}

.meta{display:flex;gap:14px;margin-top:16px;align-items:center}
.thumb{width:120px;height:68px;border-radius:11px;object-fit:cover;flex:none;
  background:#0b1119;border:1px solid var(--line)}
.meta-text{min-width:0}
.meta-title{font-weight:750;font-size:15px;line-height:1.3;
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.meta-sub{color:var(--muted);font-size:12.5px;margin-top:4px}

.section-title{font-size:15px;font-weight:800;letter-spacing:.3px;text-transform:uppercase;
  color:var(--muted);margin:26px 4px 12px;display:flex;align-items:center;gap:8px}

.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
@media(max-width:650px){.grid{grid-template-columns:1fr}}

.format{
  background:var(--panel-solid);border:1px solid var(--line);border-radius:15px;padding:15px;
  animation:rise .28s ease both;transition:border-color .15s,transform .12s;
}
.format:hover{border-color:rgba(90,168,255,.35)}
@keyframes rise{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
.frow{display:flex;align-items:center;gap:12px}
.icon{width:34px;height:34px;border-radius:10px;flex:none;display:flex;align-items:center;justify-content:center;
  background:rgba(90,168,255,.12);font-size:16px}
.icon.audio{background:rgba(51,209,154,.12)}
.info{min-width:0;flex:1}
.label{font-size:15.5px;font-weight:800}
.detail{color:var(--muted);margin-top:2px;font-size:12.5px}
.size{color:#cfe0f5;font-weight:750}

.download{width:100%;margin-top:12px;background:linear-gradient(135deg,var(--green),#26b586);color:#05140f}
.progress{height:7px;background:#0a0f17;border-radius:99px;overflow:hidden;margin-top:12px}
.progress i{display:block;height:100%;width:0;background:linear-gradient(90deg,var(--blue),var(--blue2));
  transition:width .25s ease}
.status{color:var(--muted);font-size:12.5px;margin-top:7px;min-height:18px;display:flex;justify-content:space-between}

.skeleton{background:var(--panel-solid);border:1px solid var(--line);border-radius:15px;
  padding:15px;margin-bottom:12px;overflow:hidden;position:relative}
.skeleton::after{content:"";position:absolute;inset:0;
  background:linear-gradient(90deg,transparent,rgba(255,255,255,.06),transparent);
  animation:shimmer 1.2s infinite}
@keyframes shimmer{from{transform:translateX(-100%)}to{transform:translateX(100%)}}
.bar{height:12px;border-radius:6px;background:rgba(255,255,255,.07)}
.bar.w60{width:60%}.bar.w40{width:40%;margin-top:8px}

@media(max-width:650px){
  .wrap{padding:26px 14px 60px}
  .brand{font-size:22px}
  .inputrow{flex-direction:column}
  .inputrow .btn{width:100%}
  .meta{flex-direction:row}
  .thumb{width:96px;height:56px}
}
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <div class="logo">M</div>
    <div>
      <div class="brand">MediaFlow PRO</div>
      <div class="sub">Fast analysis · temporary processing · automatic cleanup</div>
    </div>
  </div>

  <div class="panel">
    <div class="inputrow">
      <input class="url" id="url" placeholder="Paste video URL" autocomplete="off">
      <button class="btn" id="go">Analyze</button>
    </div>
    <div class="msg" id="msg"></div>
    <div class="meta" id="meta" style="display:none"></div>
    <div class="note">Server-side cookies are used automatically when configured.</div>
  </div>

  <div id="out"></div>
</div>

<script>
const $=s=>document.querySelector(s);
const esc=x=>String(x??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));

$('#go').onclick=analyze;
$('#url').addEventListener('keydown',e=>{if(e.key==='Enter')analyze()});

function skeletons(n){
  const out=$('#out'); out.innerHTML='';
  const title=document.createElement('div');
  title.className='section-title'; title.textContent='Reading available formats…';
  out.appendChild(title);
  for(let i=0;i<n;i++){
    const s=document.createElement('div');
    s.className='skeleton';
    s.innerHTML='<div class="bar w60"></div><div class="bar w40"></div>';
    out.appendChild(s);
  }
}

async function analyze(){
  const url=$('#url').value.trim();
  if(!url){$('#msg').textContent='Please paste a URL.';return}
  const btn=$('#go'); btn.disabled=true; btn.textContent='Analyzing…';
  $('#msg').textContent='Reading source and available formats…';
  $('#meta').style.display='none';
  skeletons(4);

  const fd=new FormData(); fd.append('url',url);
  try{
    const r=await fetch('/api/formats',{method:'POST',body:fd});
    const d=await r.json();
    if(!r.ok) throw Error(d.error||'Analysis failed');

    $('#msg').textContent=`Found ${d.formats.length} format${d.formats.length===1?'':'s'}`;

    if(d.title){
      const meta=$('#meta');
      meta.style.display='flex';
      meta.innerHTML=`
        ${d.thumbnail?`<img class="thumb" src="${esc(d.thumbnail)}" alt="">`:''}
        <div class="meta-text">
          <div class="meta-title">${esc(d.title)}</div>
          <div class="meta-sub">${[d.uploader,d.duration].filter(Boolean).map(esc).join(' · ')}</div>
        </div>`;
    }

    const out=$('#out'); out.innerHTML='';
    const groups=[['video','Video'],['audio','Audio only']];
    for(const [kind,label] of groups){
      const items=d.formats.filter(f=>f.kind===kind);
      if(!items.length) continue;
      const title=document.createElement('div');
      title.className='section-title';
      title.textContent=label;
      out.appendChild(title);
      const grid=document.createElement('div');
      grid.className='grid';
      items.forEach((f,idx)=>{
        const card=document.createElement('div');
        card.className='format';
        card.style.animationDelay=(idx*25)+'ms';
        card.innerHTML=`
          <div class="frow">
            <div class="icon ${kind==='audio'?'audio':''}">${kind==='audio'?'♪':'▶'}</div>
            <div class="info">
              <div class="label">${esc(f.label)}</div>
              <div class="detail">${esc(f.detail)} · <span class="size">${esc(f.size)}</span></div>
            </div>
          </div>
          <button class="btn download">Download</button>
          `;
        card.querySelector('button').onclick=()=>startDownload(f.token,card);
        grid.appendChild(card);
      });
      out.appendChild(grid);
    }
  }catch(e){
    $('#out').innerHTML='';
    $('#msg').innerHTML='<span class="error">Error: '+esc(e.message)+'</span>';
  }finally{
    btn.disabled=false;btn.textContent='Analyze';
  }
}

async function startDownload(token,card){
  const btn=card.querySelector('button');
  btn.disabled=true;
  btn.textContent='Preparing…';

  let bar=card.querySelector('.progress');
  if(!bar){
    card.insertAdjacentHTML('beforeend','<div class="progress"><i></i></div><div class="status"><span class="s-left"></span><span class="s-right"></span></div>');
    bar=card.querySelector('.progress');
  }
  const fill=bar.querySelector('i');
  const sLeft=card.querySelector('.s-left');
  const sRight=card.querySelector('.s-right');

  try{
    const start=await fetch('/api/start/'+encodeURIComponent(token),{method:'POST'});
    const sd=await start.json();
    if(!start.ok) throw Error(sd.error||'Unable to start download');
    if(sd.status==='error') throw Error(sd.error||'Download failed');

    if(sd.status==='ready'){
      window.location.href=sd.download_url||('/download/'+encodeURIComponent(token));
      return;
    }

    const timer=setInterval(async()=>{
      try{
        const r=await fetch('/api/progress/'+encodeURIComponent(token),{cache:'no-store'});
        const d=await r.json();

        if(!r.ok){
          clearInterval(timer);
          btn.disabled=false;
          btn.textContent='Download';
          return;
        }

        const p=d.progress||{};
        if(fill) fill.style.width=(p.percent||0)+'%';
        if(sLeft) sLeft.textContent=`${p.downloaded||''} / ${p.total||''}`;
        if(sRight) sRight.textContent=p.speed?`${p.speed} · ETA ${p.eta}`:'';
        btn.textContent=(p.percent||0)+'% …';

        if(d.status==='ready'){
          clearInterval(timer);
          btn.textContent='Downloading…';
          window.location.href=d.download_url||('/download/'+encodeURIComponent(token));
        }else if(d.status==='error'){
          clearInterval(timer);
          btn.disabled=false;
          btn.textContent='Download';
          alert(d.error||'Download failed');
        }
      }catch(e){
        clearInterval(timer);
        btn.disabled=false;
        btn.textContent='Download';
      }
    },700);

  }catch(e){
    btn.disabled=false;
    btn.textContent='Download';
    alert(e.message||'Download failed');
  }
}
</script>
</body>
</html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
