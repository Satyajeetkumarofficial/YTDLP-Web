import os, re, time, uuid, hmac, json, socket, hashlib, threading, ipaddress, subprocess, logging
from pathlib import Path
from urllib.parse import urlparse, quote
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify, render_template_string, Response, stream_with_context
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
MAX_CONCURRENT_STREAMS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "3"))
PROBE_TIMEOUT = float(os.getenv("PROBE_TIMEOUT_SECONDS", "4"))
PROBE_WORKERS = int(os.getenv("PROBE_WORKERS", "8"))
CHUNK = 256 * 1024
COOKIES_FILE = os.getenv("COOKIES_FILE", "cookies.txt")

JOBS, RATE = {}, {}
LOCK = threading.RLock()
SEM = threading.BoundedSemaphore(MAX_CONCURRENT_STREAMS)
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


def hdur(n):
    if not n:
        return None
    n = int(n)
    m, s = divmod(n, 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def safe(name):
    return (re.sub(r'[\\/:*?"<>|]+', "_", name or "download")[:180].strip(" .") or "download")


def content_disposition(filename):
    """HTTP headers must be Latin-1/ASCII — a raw emoji or curly-quote title
    (common on Facebook/Instagram captions) will make the server reject the
    response outright. Send a plain-ASCII fallback plus an RFC 5987
    UTF-8 filename* so browsers still show the real name."""
    ascii_name = safe(name.encode("ascii", "ignore").decode("ascii")) or "download"
    encoded = quote(name, safe="")
    return f'attachment; filename="{ascii_name}"; filename*=UTF-8\'\'{encoded}'


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


def reap_jobs():
    while True:
        time.sleep(120)
        now = time.time()
        with LOCK:
            for job_id in [j for j, job in JOBS.items() if now - job.get("created", now) > JOB_TTL]:
                JOBS.pop(job_id, None)


threading.Thread(target=reap_jobs, daemon=True).start()


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
            stream=True, timeout=PROBE_TIMEOUT, allow_redirects=True,
        )
        cr = r.headers.get("Content-Range", "")
        cl = r.headers.get("Content-Length")
        m = re.search(r"/(\d+)$", cr)
        size = int(m.group(1)) if m else (int(cl) if r.status_code == 200 and cl and cl.isdigit() else None)
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

    audios.sort(key=lambda x: x.get("abr") or 0, reverse=True)
    best_audio = audios[0] if audios else None

    videos.sort(key=lambda x: (x.get("height") or 0, x.get("width") or 0, x.get("tbr") or 0), reverse=True)
    deduped_videos, seen = [], set()
    for video in videos:
        key = (video.get("width"), video.get("height"))
        if key in seen:
            continue
        seen.add(key)
        deduped_videos.append(video)

    top_audios = audios[:8]

    to_size = list(deduped_videos) + list(top_audios)
    if best_audio is not None and best_audio not in to_size:
        to_size.append(best_audio)

    futures = {id(fmt): PROBE_POOL.submit(resolve_size, fmt, duration) for fmt in to_size}
    sizes = {}
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
            "quiet": True, "no_warnings": True, "skip_download": True,
            "noplaylist": True, "retries": 3, "fragment_retries": 3,
            "socket_timeout": 20, "js_runtimes": {"deno": {}},
        }
        if cp:
            opts["cookiefile"] = cp

        log.info("ANALYZE request ip=%s url=%s", client_ip(), url)

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)

        formats = make_formats(info)
        JOBS[job_id] = {"created": time.time(), "url": url, "title": info.get("title") or url, "formats": formats}

        log.info(
            "ANALYSIS RESULT | extractor=%s | title=%s | duration=%s | formats=%d | took=%.2fs",
            info.get("extractor"), info.get("title"), info.get("duration"), len(formats), time.time() - t0,
        )

        result = []
        for i, fmt in enumerate(formats):
            item = dict(fmt, token=make_token(job_id, i))
            item.pop("selector", None)
            item.pop("size_bytes", None)
            result.append(item)

        return jsonify(
            job_id=job_id, title=JOBS[job_id]["title"], thumbnail=info.get("thumbnail"),
            uploader=info.get("uploader"), duration=hdur(info.get("duration")), formats=result,
        )

    except Exception as exc:
        log.exception("ANALYZE failed")
        return jsonify(error=str(exc)), 500


# ---------------------------------------------------------------------------
# Streaming download — resolved formats are piped straight to the browser.
# Nothing is ever written to disk on the server, so hosts with small/ephemeral
# storage (Koyeb etc.) never fill up regardless of file size or traffic.
# ---------------------------------------------------------------------------

def header_block(fmt):
    headers = fmt.get("http_headers") or {}
    return "".join(f"{k}: {v}\r\n" for k, v in headers.items())


@app.get("/stream/<token>")
def stream(token):
    payload = check_token(token)
    if not payload:
        return "Invalid or expired link", 404

    job = JOBS.get(payload["job"])
    if not job or time.time() - job["created"] > JOB_TTL:
        JOBS.pop(payload.get("job"), None)
        return "Link expired. Please analyze the URL again.", 410

    try:
        fmt = job["formats"][int(payload["fmt"])]
    except (ValueError, TypeError, KeyError, IndexError):
        return "Format not found", 404

    if not SEM.acquire(blocking=False):
        return "Server is busy streaming other downloads. Please try again shortly.", 503

    released = threading.Event()

    def release_once():
        if not released.is_set():
            released.set()
            try:
                SEM.release()
            except Exception:
                pass

    try:
        cp = cookie_path()
        opts = {
            "quiet": True, "no_warnings": True, "skip_download": True, "noplaylist": True,
            "format": fmt["selector"], "socket_timeout": 20, "js_runtimes": {"deno": {}},
        }
        if cp:
            opts["cookiefile"] = cp
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(job["url"], download=False)
    except Exception as exc:
        release_once()
        log.exception("STREAM resolve failed")
        return f"Could not resolve the stream: {exc}", 502

    requested = info.get("requested_formats") or [info]
    filename = safe(f"{info.get('title') or job['title']}.{fmt['container']}")
    log.info("STREAM START | label=%s | parts=%d | file=%s", fmt["label"], len(requested), filename)

    if len(requested) >= 2:
        video, audio = requested[0], requested[1]
        is_mp4 = fmt["container"] == "mp4"
        args = ["ffmpeg", "-loglevel", "error"]
        for src in (video, audio):
            hb = header_block(src)
            if hb:
                args += ["-headers", hb]
            args += ["-i", src["url"]]
        args += ["-map", "0:v:0", "-map", "1:a:0", "-c", "copy"]
        if is_mp4:
            args += ["-movflags", "frag_keyframe+empty_moov+faststart"]
        args += ["-f", "mp4" if is_mp4 else "matroska", "pipe:1"]

        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

        def generate():
            try:
                while True:
                    chunk = proc.stdout.read(CHUNK)
                    if not chunk:
                        break
                    yield chunk
            except GeneratorExit:
                pass
            finally:
                try:
                    proc.kill()
                except Exception:
                    pass
                release_once()
                log.info("STREAM END | file=%s", filename)

        resp = Response(stream_with_context(generate()),
                         mimetype="video/mp4" if is_mp4 else "video/x-matroska")
        resp.headers["Content-Disposition"] = content_disposition(filename)
        return resp

    src = requested[0]
    headers = dict(src.get("http_headers") or {})
    range_header = request.headers.get("Range")
    if range_header:
        headers["Range"] = range_header
    try:
        upstream = requests.get(src["url"], headers=headers, stream=True, timeout=30)
    except Exception as exc:
        release_once()
        return f"Could not reach source: {exc}", 502

    def generate_single():
        try:
            for chunk in upstream.iter_content(chunk_size=CHUNK):
                if chunk:
                    yield chunk
        except GeneratorExit:
            pass
        finally:
            upstream.close()
            release_once()
            log.info("STREAM END | file=%s", filename)

    resp = Response(stream_with_context(generate_single()), status=upstream.status_code)
    for h in ("Content-Length", "Content-Range", "Accept-Ranges", "Content-Type"):
        if h in upstream.headers:
            resp.headers[h] = upstream.headers[h]
    resp.headers["Content-Disposition"] = content_disposition(filename)
    return resp


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
.status{color:var(--muted);font-size:12.5px;margin-top:8px;text-align:center}

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
      <div class="sub">Fast analysis · streamed straight to your device</div>
    </div>
  </div>

  <div class="panel">
    <div class="inputrow">
      <input class="url" id="url" placeholder="Paste video URL" autocomplete="off">
      <button class="btn" id="go">Analyze</button>
    </div>
    <div class="msg" id="msg"></div>
    <div class="meta" id="meta" style="display:none"></div>
    <div class="note">Downloads are streamed live and never stored on the server.</div>
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
          <div class="status"></div>
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

function startDownload(token,card){
  const btn=card.querySelector('button');
  const status=card.querySelector('.status');
  btn.disabled=true;
  btn.textContent='Starting…';
  status.textContent="Streaming from source — check your browser's downloads.";
  // Direct navigation lets the browser handle the transfer natively:
  // nothing is buffered on the server, so it starts (and shows progress) immediately.
  window.location.href='/stream/'+encodeURIComponent(token);
  setTimeout(()=>{ btn.disabled=false; btn.textContent='Download'; }, 2500);
}
</script>
</body>
</html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
