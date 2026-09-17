import os, re, time, uuid, hmac, json, socket, hashlib, tempfile, threading, ipaddress, shutil, logging
from pathlib import Path
from urllib.parse import urlparse
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
COOKIES_FILE = os.getenv("COOKIES_FILE", "cookies.txt")
TMP_ROOT = os.path.join(tempfile.gettempdir(), "mediaflow")
os.makedirs(TMP_ROOT, exist_ok=True)

JOBS, RATE = {}, {}
LOCK = threading.RLock()
SEM = threading.BoundedSemaphore(MAX_CONCURRENT)

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


def tiny_probe(url):
    """Try a 1-byte range probe. Never intentionally downloads the whole object."""
    if not requests or not url:
        return None
    try:
        r = requests.get(
            url,
            headers={"Range": "bytes=0-0", "User-Agent": "Mozilla/5.0"},
            stream=True,
            timeout=10,
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


def fsize(fmt, duration):
    for key in ("filesize", "filesize_approx"):
        if fmt.get(key):
            return int(fmt[key])

    # User requirement: probe the source when metadata does not expose size.
    probed = tiny_probe(fmt.get("url"))
    if probed is not None:
        return probed

    if duration and fmt.get("tbr"):
        try:
            return int(float(fmt["tbr"]) * 1000 / 8 * float(duration))
        except Exception:
            pass
    return None


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


def make_formats(info):
    duration = info.get("duration")
    videos, audios = [], []

    for raw in info.get("formats") or []:
        if not raw.get("format_id"):
            continue
        fmt = dict(raw)
        fmt["_size"] = fsize(fmt, duration)
        if fmt.get("vcodec") not in (None, "none"):
            videos.append(fmt)
        elif fmt.get("acodec") not in (None, "none"):
            audios.append(fmt)

    best_audio = max(
        audios,
        key=lambda x: (x.get("abr") or 0, x.get("_size") or 0),
        default=None,
    )

    out, seen = [], set()
    videos.sort(
        key=lambda x: (
            x.get("height") or 0,
            x.get("width") or 0,
            x.get("tbr") or 0,
        ),
        reverse=True,
    )

    for video in videos:
        key = (video.get("width"), video.get("height"))
        if key in seen:
            continue
        seen.add(key)

        size = video.get("_size")
        if best_audio and size is not None and best_audio.get("_size") is not None:
            size += best_audio["_size"]

        selector = video["format_id"] + (f"+{best_audio['format_id']}" if best_audio else "")

        for container in ("mp4", "mkv"):
            out.append({
                "kind": "video",
                "label": f"{reslabel(video)} • {container.upper()}",
                "detail": "Video + best audio",
                "size": hs(size),
                "size_bytes": size,
                "selector": selector,
                "container": container,
            })

    for audio in sorted(audios, key=lambda x: x.get("abr") or 0, reverse=True)[:12]:
        ext = (audio.get("ext") or "m4a").lower()
        abr = audio.get("abr")
        out.append({
            "kind": "audio",
            "label": f"{ext.upper()} • {int(abr) if abr else '?'} kbps",
            "detail": "Original audio",
            "size": hs(audio.get("_size")),
            "size_bytes": audio.get("_size"),
            "selector": audio["format_id"],
            "container": ext,
        })

    return out[:80]


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
            "ANALYSIS RESULT | extractor=%s | title=%s | uploader=%s | duration=%s | formats=%d",
            info.get("extractor"),
            info.get("title"),
            info.get("uploader"),
            info.get("duration"),
            len(formats),
        )

        result = []
        for i, fmt in enumerate(formats):
            item = dict(fmt, token=make_token(job_id, i))
            item.pop("selector", None)
            item.pop("size_bytes", None)
            result.append(item)
            log.info(
                "FORMAT | index=%d | kind=%s | label=%s | size=%s | selector=%s",
                i, fmt["kind"], fmt["label"], fmt["size"], fmt["selector"]
            )

        return jsonify(
            job_id=job_id,
            title=JOBS[job_id]["title"],
            thumbnail=info.get("thumbnail"),
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
                log.info(
                    "PROGRESS job=%s %.1f%% %s/%s speed=%s ETA=%s",
                    job_id, st["percent"], st["downloaded"], st["total"],
                    st["speed"], st["eta"],
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

        log.info(
            "DOWNLOAD START | job=%s | label=%s | selector=%s | workdir=%s",
            job_id, fmt["label"], fmt["selector"], workdir
        )

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

        if not SEM.acquire(blocking=False):
            return jsonify(error="Server is busy. Try again shortly."), 503

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
  --bg:#080b11;--panel:#111722;--panel2:#0d131d;--line:#273246;
  --text:#f5f7fb;--muted:#8f9db0;--blue:#5aa8ff;--green:#35c995;--danger:#ff6b78;
}
*{box-sizing:border-box}
body{margin:0;background:linear-gradient(180deg,#080b11 0%,#0b1018 100%);color:var(--text);
font:15px/1.45 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
.wrap{max-width:900px;margin:auto;padding:38px 18px 70px}
.header{margin-bottom:24px}
.brand{font-size:32px;font-weight:850;letter-spacing:-.8px}
.sub{color:var(--muted);margin-top:4px}
.panel{background:rgba(17,23,34,.92);border:1px solid var(--line);border-radius:18px;padding:18px;
box-shadow:0 14px 40px rgba(0,0,0,.22)}
.inputrow{display:flex;gap:10px}
.url{width:100%;min-width:0;background:#0a1019;color:#fff;border:1px solid #2a374b;border-radius:12px;
padding:14px 15px;font-size:15px;outline:none}
.url:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(90,168,255,.12)}
.btn{border:0;border-radius:12px;padding:13px 20px;font-weight:800;cursor:pointer;
background:var(--blue);color:#07111e;white-space:nowrap}
.btn:disabled{opacity:.55;cursor:not-allowed}
.msg{min-height:22px;color:var(--muted);margin-top:12px}
.section-title{font-size:18px;font-weight:800;margin:22px 2px 10px}
.format{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:16px;margin:10px 0}
.frow{display:flex;align-items:center;gap:14px}
.info{min-width:0;flex:1}
.label{font-size:17px;font-weight:800}
.detail{color:var(--muted);margin-top:2px}
.size{color:#cbd6e5;font-weight:750}
.action{min-width:120px}
.download{width:100%;background:var(--green);color:#06150f}
.progress{height:7px;background:#080d14;border-radius:99px;overflow:hidden;margin-top:12px}
.progress i{display:block;height:100%;width:0;background:var(--blue);transition:width .2s}
.status{color:var(--muted);font-size:13px;margin-top:7px;min-height:19px}
.note{color:var(--muted);font-size:13px;margin-top:10px}
.error{color:var(--danger)}
@media(max-width:650px){
  .wrap{padding:28px 14px 60px}
  .brand{font-size:29px}
  .inputrow{flex-direction:column}
  .inputrow .btn{width:100%}
  .frow{align-items:stretch;flex-direction:column}
  .action{min-width:0}
}
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <div class="brand">MediaFlow PRO</div>
    <div class="sub">Fast analysis • temporary processing • automatic cleanup</div>
  </div>

  <div class="panel">
    <div class="inputrow">
      <input class="url" id="url" placeholder="Paste video URL" autocomplete="off">
      <button class="btn" id="go">Analyze</button>
    </div>
    <div class="msg" id="msg"></div>
    <div class="note">Server-side cookies are configured automatically when <b>cookies.txt</b> is present in the project.</div>
  </div>

  <div id="out"></div>
</div>

<script>
const $=s=>document.querySelector(s);
const esc=x=>String(x??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));

$('#go').onclick=analyze;
$('#url').addEventListener('keydown',e=>{if(e.key==='Enter')analyze()});

async function analyze(){
  const url=$('#url').value.trim();
  if(!url){$('#msg').textContent='Please paste a URL.';return}
  const btn=$('#go'); btn.disabled=true; btn.textContent='Analyzing…';
  $('#msg').textContent='Analyzing URL and reading available formats…';
  $('#out').innerHTML='';

  const fd=new FormData(); fd.append('url',url);
  try{
    const r=await fetch('/api/formats',{method:'POST',body:fd});
    const d=await r.json();
    if(!r.ok) throw Error(d.error||'Analysis failed');

    $('#msg').textContent=d.title||'Available formats';

    const title=document.createElement('div');
    title.className='section-title';
    title.textContent='Available Formats';
    $('#out').appendChild(title);

    d.formats.forEach(f=>{
      const card=document.createElement('div');
      card.className='format';
      card.innerHTML=`
        <div class="frow">
          <div class="info">
            <div class="label">${esc(f.label)}</div>
            <div class="detail">${esc(f.detail)} · <span class="size">${esc(f.size)}</span></div>
          </div>
          <div class="action"><button class="btn download">Download</button></div>
        </div>
        <div class="progress"><i></i></div>
        <div class="status"></div>`;
      card.querySelector('button').onclick=()=>startDownload(f.token,card);
      $('#out').appendChild(card);
    });
  }catch(e){
    $('#msg').innerHTML='<span class="error">Error: '+esc(e.message)+'</span>';
  }finally{
    btn.disabled=false;btn.textContent='Analyze';
  }
}

async function startDownload(token,card){
  const btn=card.querySelector('button'), bar=card.querySelector('i'), status=card.querySelector('.status');
  btn.disabled=true; btn.textContent='Preparing…'; status.textContent='Starting…';

  try{
    const start=await fetch('/api/start/'+encodeURIComponent(token),{method:'POST'});
    const sd=await start.json();
    if(!start.ok) throw Error(sd.error||'Unable to start download');

    if(sd.status==='error') throw Error(sd.error||'Download failed');

    const timer=setInterval(async()=>{
      try{
        const r=await fetch('/api/progress/'+encodeURIComponent(token),{cache:'no-store'});
        const d=await r.json();
        if(!r.ok){clearInterval(timer);throw Error(d.error||'Progress expired')}

        const p=d.progress||{};
        bar.style.width=(p.percent||0)+'%';
        status.textContent=(p.percent||0)+'% · '+(p.downloaded||'0 B')+' / '+(p.total||'unknown')+' · '+(p.speed||'—')+' · ETA '+(p.eta||'—');

        if(d.status==='ready'){
          clearInterval(timer);
          bar.style.width='100%';
          status.textContent='Ready — starting browser download…';
          btn.textContent='Downloading…';
          window.location.href=d.download_url||('/download/'+encodeURIComponent(token));
        }else if(d.status==='error'){
          clearInterval(timer);
          throw Error(d.error||'Download failed');
        }
      }catch(e){
        clearInterval(timer);
        status.innerHTML='<span class="error">Error: '+esc(e.message)+'</span>';
        btn.disabled=false; btn.textContent='Download';
      }
    },700);

  }catch(e){
    status.innerHTML='<span class="error">Error: '+esc(e.message)+'</span>';
    btn.disabled=false; btn.textContent='Download';
  }
}
</script>
</body>
</html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
