#!/usr/bin/env python3
"""
MediaFlow Pro — Koyeb-ready yt-dlp web downloader.

Design:
- Koyeb serves only the web app.
- Downloads are temporary and are deleted after the response closes.
- Dynamic formats are detected per URL.
- Each option has its own browser Download button.
- Includes basic abuse protection, signed one-time download tokens,
  rate limiting, size limits, security headers and bounded concurrency.
"""
import os, re, uuid, time, hmac, hashlib, ipaddress, socket, tempfile, shutil, threading
from collections import defaultdict, deque
from urllib.parse import urlparse
from flask import Flask, request, jsonify, render_template_string, send_file, abort, make_response
import yt_dlp
import logging
try:
    from curl_cffi import requests as cf_requests
except Exception:
    cf_requests = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("mediaflow")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 6 * 1024 * 1024

PORT = int(os.environ.get("PORT", "8000"))
SECRET = os.environ.get("MEDIAFLOW_SECRET") or uuid.uuid4().hex + uuid.uuid4().hex
TMP_ROOT = os.path.join(tempfile.gettempdir(), "mediaflow_pro")
os.makedirs(TMP_ROOT, exist_ok=True)

JOBS = {}
LOCK = threading.Lock()
RATE = defaultdict(deque)
MAX_JOBS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "2"))
DOWNLOAD_SEM = threading.BoundedSemaphore(MAX_JOBS)
MAX_FILE_BYTES = int(os.environ.get("MAX_FILE_GB", "4")) * 1024**3
MAX_FORMATS = 80
JOB_TTL = 30 * 60
RATE_LIMIT = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "30"))
RATE_WINDOW = 60

def client_ip():
    # Do not trust arbitrary X-Forwarded-For headers for authorization.
    return request.remote_addr or "unknown"

def allowed_url(url):
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https") or not p.hostname:
            return False
        host = p.hostname.strip(".").lower()
        if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
            return False
        try:
            infos = socket.getaddrinfo(host, None)
            for item in infos:
                ip = ipaddress.ip_address(item[4][0])
                if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                    return False
        except socket.gaierror:
            return False
        return True
    except Exception:
        return False

def limited():
    now = time.time()
    q = RATE[client_ip()]
    while q and now - q[0] > RATE_WINDOW:
        q.popleft()
    if len(q) >= RATE_LIMIT:
        return False
    q.append(now)
    return True

def sign(token):
    return hmac.new(SECRET.encode(), token.encode(), hashlib.sha256).hexdigest()[:32]

def make_token(job, fmt):
    raw = f"{job}:{fmt}:{uuid.uuid4().hex}"
    return raw + "." + sign(raw)

def verify_token(token):
    try:
        raw, sig = token.rsplit(".", 1)
        if not hmac.compare_digest(sig, sign(raw)):
            return None
        parts = raw.split(":")
        if len(parts) < 3:
            return None
        return parts[0], parts[1]
    except Exception:
        return None

def cleanup(path):
    shutil.rmtree(path, ignore_errors=True)

def cleanup_old():
    cutoff = time.time() - JOB_TTL
    with LOCK:
        for k, v in list(JOBS.items()):
            if v.get("created", 0) < cutoff:
                JOBS.pop(k, None)

def log_format_summary(info, formats):
    title = info.get("title") or "Unknown"
    extractor = info.get("extractor_key") or info.get("extractor") or "unknown"
    webpage = info.get("webpage_url") or ""
    duration = info.get("duration")
    uploader = info.get("uploader") or info.get("channel") or ""
    log.info("ANALYSIS RESULT | extractor=%s | title=%s | uploader=%s | duration=%s | formats=%d | url=%s",
             extractor, title[:180], uploader[:120], duration, len(formats), webpage[:300])
    for x in formats:
        log.info("FORMAT | kind=%s | label=%s | detail=%s | size=%s | selector=%s",
                 x.get("kind"), x.get("label"), x.get("detail"),
                 human_size(x.get("size")) if x.get("size") else "unavailable",
                 x.get("format") or "")


def validate_cookie_file(path):
    """Validate a Netscape-format cookie.txt without logging cookie values."""
    try:
        raw = Path(path).read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False
    lines = [x.strip() for x in raw.splitlines() if x.strip() and not x.lstrip().startswith("#")]
    if not lines:
        return False
    # Netscape cookie files normally have 7 tab-separated columns.
    good = sum(1 for line in lines if len(line.split("\t")) >= 7)
    return good > 0


def human_size(n):
    if not n:
        return "Size unavailable"
    n = float(n)
    units = ["B", "KB", "MB", "GB", "TB"]
    for u in units:
        if n < 1024 or u == units[-1]:
            return f"{n:.1f} {u}" if u != "B" else f"{int(n)} B"
        n /= 1024
    return "Size unavailable"

def safe_filename(name):
    name = re.sub(r'[\\/:*?"<>|]+', "_", name or "download")
    return name[:180].strip(" .") or "download"

def _fmt_size(f, duration=0):
    size = f.get("filesize") or f.get("filesize_approx")
    if size:
        return int(size)
    bitrate = f.get("tbr") or f.get("abr")
    if duration and bitrate:
        return int(float(duration) * float(bitrate) * 1000 / 8)
    return None

def build_formats(info):
    src = info.get("formats") or []
    videos, audios = [], []
    heights = set()
    duration = info.get("duration") or 0
    for f in src:
        fid = str(f.get("format_id", ""))
        ext = (f.get("ext") or "").lower()
        h = int(f.get("height") or 0)
        vc, ac = f.get("vcodec"), f.get("acodec")
        if not fid:
            continue
        if vc and vc != "none":
            if h > 0: heights.add(h)
            videos.append((h, f))
        elif ac and ac != "none":
            audios.append(f)

    result=[]
    for h in sorted(heights, reverse=True):
        candidates=[x for x in videos if x[0]==h]
        srcf=max(candidates,key=lambda z:(1 if z[1].get("acodec") not in (None,"none") else 0,float(z[1].get("tbr") or 0)))[1]
        size=_fmt_size(srcf,duration)
        # If video-only, also account for the best available audio stream.
        if srcf.get("acodec") in (None,"none"):
            audio_candidates=[a for a in audios if a.get("url")]
            if audio_candidates:
                best_audio=max(audio_candidates,key=lambda a:float(a.get("abr") or a.get("tbr") or 0))
                a_size=_fmt_size(best_audio,duration)
                if size and a_size: size += a_size
                elif a_size: size=a_size
        fmt=f"bestvideo[height={h}]+bestaudio/best[height={h}]"
        common={"format":fmt,"size":size,"size_pending":False}
        result.append({"id":f"q:{h}","kind":"video","label":f"{h}p • MP4","detail":"Video + Audio","container":"mp4",**common})
        result.append({"id":f"q:{h}:mkv","kind":"video","label":f"{h}p • MKV","detail":"Video + Audio","container":"mkv",**common})
        if (srcf.get("ext") or "").lower()=="webm":
            result.append({"id":f"q:{h}:webm","kind":"video","label":f"{h}p • WebM","detail":"WebM stream","container":"webm",**common})

    seen=set()
    for f in sorted(audios,key=lambda x:float(x.get("abr") or 0),reverse=True):
        ext=(f.get("ext") or "audio").lower(); abr=int(float(f.get("abr") or 0)) if f.get("abr") else 0
        key=(ext,abr//32)
        if key in seen: continue
        seen.add(key)
        result.append({"id":f"a:{f.get('format_id')}","kind":"audio","label":f"{ext.upper()} • {abr}kbps" if abr else ext.upper(),"detail":"Original audio","format":str(f.get("format_id")),"container":ext,"size":_fmt_size(f,duration),"size_pending":False})
        if len(seen)>=10: break
    return result[:MAX_FORMATS]

def _probe_url_size(url, headers=None, timeout=8):
    if not url: return None
    try:
        if cf_requests:
            r=cf_requests.head(url,headers=headers or {},timeout=timeout,allow_redirects=True)
            cl=r.headers.get("content-length")
            if cl and cl.isdigit(): return int(cl)
            cr=r.headers.get("content-range","")
            if "/" in cr and cr.rsplit("/",1)[1].isdigit(): return int(cr.rsplit("/",1)[1])
            r=cf_requests.get(url,headers={**(headers or {}),"Range":"bytes=0-0"},timeout=timeout,allow_redirects=True,stream=True)
            cr=r.headers.get("content-range","")
            if "/" in cr and cr.rsplit("/",1)[1].isdigit(): return int(cr.rsplit("/",1)[1])
            cl=r.headers.get("content-length")
            return int(cl) if cl and cl.isdigit() and r.status_code==200 else None
        return None
    except Exception:
        return None

def background_probe_sizes(job_id, info, cookie_path=None):
    """Best-effort remote size probing. Never logs URLs/cookie contents."""
    try:
        raw=info.get("formats") or []
        duration=info.get("duration") or 0
        by_height={}
        aud={}
        for f in raw:
            fid=str(f.get("format_id","")); h=int(f.get("height") or 0)
            size=_fmt_size(f,duration)
            if size is None and f.get("url"):
                size=_probe_url_size(f.get("url"), f.get("http_headers") or {})
            if f.get("vcodec") not in (None,"none") and h:
                by_height.setdefault(h,[]).append((f,size))
            elif f.get("acodec") not in (None,"none") and fid:
                aud[fid]=size
        with LOCK:
            job=JOBS.get(job_id)
            if not job: return
            for x in job["formats"].values():
                if x.get("kind")=="audio":
                    if x.get("size") is None:
                        x["size"]=aud.get(str(x.get("format")))
                elif x.get("size") is None:
                    h=int(x["id"].split(":")[1])
                    candidates=by_height.get(h,[])
                    if candidates:
                        best=max(candidates,key=lambda z:float(z[0].get("tbr") or 0))
                        x["size"]=best[1]
            job["size_ready"]=True
    except Exception:
        log.exception("SIZE PROBE failed job=%s", job_id)
        with LOCK:
            if job_id in JOBS: JOBS[job_id]["size_ready"]=True

PAGE = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#080b12">
<title>MediaFlow Pro</title>
<style>
:root{--bg:#070a10;--surface:#0e141e;--surface2:#111a27;--line:#202c3d;--txt:#f3f7ff;--muted:#8e9bb0;--a:#6d9cff;--b:#8d6bff;--good:#53d28b;--danger:#ff6574;--shadow:0 25px 70px #0008}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(800px 420px at 50% -80px,#20325855,transparent 65%),var(--bg);color:var(--txt);font:15px Inter,system-ui,-apple-system,Segoe UI,sans-serif}
.wrap{max-width:1000px;margin:auto;padding:28px 18px 70px}.top{display:flex;justify-content:space-between;align-items:center;margin-bottom:30px}
.logo{font-weight:900;font-size:26px;letter-spacing:-.8px}.logo i{font-style:normal;color:#7fa7ff}.badge{font-size:11px;color:#aebbd0;border:1px solid var(--line);padding:6px 9px;border-radius:999px}
.hero{text-align:center;margin:25px 0 24px}.hero h1{font-size:clamp(30px,6vw,54px);margin:0;letter-spacing:-2px}.hero p{color:var(--muted);margin:10px auto 26px;max-width:620px}
.search{display:flex;gap:10px;background:#0d131d;border:1px solid var(--line);padding:9px;border-radius:18px;box-shadow:var(--shadow)}input{min-width:0;flex:1;background:#080d15;border:0;color:var(--txt);padding:15px;border-radius:12px;font-size:15px;outline:0}.primary{border:0;border-radius:12px;padding:0 24px;background:linear-gradient(135deg,var(--a),var(--b));color:#fff;font-weight:800;cursor:pointer}.primary:disabled{opacity:.55}
#msg{min-height:42px;padding:15px 2px;color:var(--muted)}.title{font-size:18px;font-weight:800;margin:5px 0 13px}.section{margin-top:18px}.section h2{font-size:12px;color:var(--muted);letter-spacing:.13em;text-transform:uppercase;margin:0 0 10px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(270px,1fr));gap:10px}.card{background:linear-gradient(145deg,var(--surface),var(--surface2));border:1px solid var(--line);border-radius:15px;padding:13px;display:flex;align-items:center;justify-content:space-between;gap:12px;transition:.16s}.card:hover{transform:translateY(-1px);border-color:#385174}.info b{font-size:15px}.info small{display:block;color:var(--muted);margin-top:4px}.info small:last-child{color:#a9bce0;font-size:11px}.dl{border:1px solid #304768;background:#142238;color:#e9f1ff;border-radius:10px;padding:10px 13px;font-weight:800;cursor:pointer}.dl:hover{background:#1a2d48}.progress{height:5px;background:#0a0f17;border-radius:99px;margin-top:16px;overflow:hidden}.progress div{height:100%;width:0;background:linear-gradient(90deg,var(--a),var(--b));transition:width .2s}.status{font-size:12px;color:var(--muted);margin-top:8px}.foot{text-align:center;color:#657289;font-size:11px;margin-top:30px}
@media(max-width:620px){.top{margin-bottom:18px}.search{flex-direction:column}.primary{height:48px}.card{align-items:flex-start}.dl{padding:9px 10px}}
.cookie-box{margin-top:12px;padding:14px 16px;border:1px solid rgba(255,255,255,.12);border-radius:14px;background:rgba(255,255,255,.04)}
.cookie-title{font-weight:700;margin-bottom:5px}.cookie-help{font-size:12px;opacity:.72;margin-bottom:9px}
.cookie-box input{max-width:100%}
</style></head>
<body><main class="wrap">
<div class="top"><div class="logo">⚡ MediaFlow <i>PRO</i></div><div class="badge">Koyeb Ready</div></div>
<section class="hero"><h1>Download Media. Your Way.</h1><p>Detect available qualities and audio streams, then choose exactly what you want. The browser receives the requested download.</p>
<div class="search"><input id="url" placeholder="Paste a media URL…" autocomplete="off"><button id="go" class="primary" onclick="analyze()">Analyze</button></div><div class="cookie-box"><div class="cookie-title">🍪 Optional cookie.txt</div><div class="cookie-help">Upload a Netscape-format cookie.txt only when the site requires your logged-in session.</div><input id="cookieFile" type="file" accept=".txt,text/plain"></div></section>
<div id="msg"></div><div id="results"></div><div class="foot">Temporary processing only • No permanent download library</div>
</main>
<script>
const $=id=>document.getElementById(id);
function esc(s){return String(s).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
async function analyze(){
 let url=$('url').value.trim(); if(!url)return;
 $('go').disabled=true;$('go').textContent='Analyzing…';$('results').innerHTML='';$('msg').textContent='🔎 Analyzing formats…';
 try{let fd=new FormData();fd.append('url',url);let cf=$('cookieFile').files[0];if(cf)fd.append('cookie_file',cf);
 let r=await fetch('/api/formats',{method:'POST',body:fd});let d=await r.json();if(!r.ok)throw Error(d.error||'Analysis failed');render(d);if(d.job&&!d.size_ready)pollSize(d.job)}
 catch(e){$('msg').textContent='❌ '+e.message}
 finally{$('go').disabled=false;$('go').textContent='Analyze'}
}
async function pollSize(job){for(let i=0;i<20;i++){await new Promise(r=>setTimeout(r,500));try{let r=await fetch('/api/formats/'+encodeURIComponent(job));if(!r.ok)return;let d=await r.json();if(d.formats)render(d);if(d.size_ready)return}catch(e){return}}}

function render(d){
 $('msg').innerHTML='<div class="title">'+esc(d.title)+'</div>'+d.formats.length+' download options detected';
 let v=d.formats.filter(x=>x.kind==='video'),a=d.formats.filter(x=>x.kind==='audio'),h='';
 if(v.length)h+='<section class="section"><h2>🎬 Video</h2><div class="grid">'+v.map(card).join('')+'</div></section>';
 if(a.length)h+='<section class="section"><h2>🎵 Audio</h2><div class="grid">'+a.map(card).join('')+'</div></section>';
 $('results').innerHTML=h||'No formats found.';
}
function card(x){return `<div class="card"><div class="info"><b>${esc(x.label)}</b><small>${esc(x.detail)}</small><small>📦 ${esc(x.size||"Size unavailable")}</small></div><button class="dl" onclick="start('${x.token}')">Download</button></div>`}
async function start(token){
 const old=document.activeElement; if(old)old.disabled=true;
 const url='/download/'+encodeURIComponent(token);
 // Navigation is a normal browser download response, not a Koyeb-side saved library.
 window.location.assign(url);
}
$('url').addEventListener('keydown',e=>{if(e.key==='Enter')analyze()});
</script></body></html>
"""

@app.after_request
def security(resp):
    resp.headers["X-Content-Type-Options"]="nosniff"
    resp.headers["X-Frame-Options"]="DENY"
    resp.headers["Referrer-Policy"]="no-referrer"
    resp.headers["Permissions-Policy"]="camera=(),microphone=(),geolocation=()"
    resp.headers["Content-Security-Policy"]="default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; frame-ancestors 'none'"
    return resp

@app.get("/")
def index():
    return render_template_string(PAGE)

@app.get("/health")
def health():
    log.info("HEALTH check ip=%s", client_ip())
    return jsonify(ok=True, service="mediaflow-pro")

@app.post("/api/formats")
def formats_api():
    cleanup_old()
    if not limited(): return jsonify(error="Too many requests. Please wait a minute."),429
    data=request.form or {}
    url=(data.get("url") or "").strip()
    cookie_upload=request.files.get("cookie_file")
    log.info("ANALYZE request ip=%s url=%s",client_ip(),url[:180])
    if len(url)>4096 or not allowed_url(url): return jsonify(error="Invalid or blocked URL."),400
    analyze_tmp=tempfile.mkdtemp(prefix="mediaflow-analyze-")
    cookie_path=None
    try:
        if cookie_upload and cookie_upload.filename:
            if not cookie_upload.filename.lower().endswith(".txt"): return jsonify(error="Please upload a .txt cookie file."),400
            cookie_path=os.path.join(analyze_tmp,"cookie.txt");cookie_upload.save(cookie_path)
            if not validate_cookie_file(cookie_path): return jsonify(error="Invalid Netscape-format cookie.txt."),400
            log.info("ANALYZE cookie accepted ip=%s",client_ip())
        opts={"quiet":True,"no_warnings":True,"skip_download":True,"noplaylist":True,"js_runtimes":{"deno":{}},"socket_timeout":20,"retries":3,"extractor_retries":3,"http_chunk_size":10485760}
        if cookie_path: opts["cookiefile"]=cookie_path
        with yt_dlp.YoutubeDL(opts) as ydl: info=ydl.extract_info(url,download=False)
        fs=build_formats(info); log_format_summary(info,fs)
        if not fs:return jsonify(error="No downloadable formats found."),400
        jid=uuid.uuid4().hex
        with LOCK:
            JOBS[jid]={"url":url,"formats":{x["id"]:x for x in fs},"title":info.get("title") or "Media","created":time.time(),"size_ready":False,"cookie_text":Path(cookie_path).read_text(encoding="utf-8",errors="ignore") if cookie_path else None}
        threading.Thread(target=background_probe_sizes,args=(jid,info),daemon=True).start()
        response=[]
        for x in fs:
            y={k:x[k] for k in ("kind","label","detail","size")};y["size"]=human_size(y["size"]) if y.get("size") else "Calculating…";y["token"]=make_token(jid,x["id"]);response.append(y)
        log.info("ANALYSIS COMPLETE | job=%s | title=%s | options=%d",jid,(info.get("title") or "Media")[:180],len(fs))
        return jsonify(title=info.get("title") or "Media",formats=response,job=jid,size_ready=False)
    except Exception as e:
        log.exception("ANALYZE failed ip=%s",client_ip());return jsonify(error=str(e)[:1000]),400
    finally: cleanup(analyze_tmp)

@app.get("/api/formats/<job_id>")
def formats_status(job_id):
    with LOCK: job=JOBS.get(job_id)
    if not job or time.time()-job["created"]>JOB_TTL:return jsonify(error="Job expired."),404
    response=[]
    for x in job["formats"].values():
        y={k:x[k] for k in ("kind","label","detail","size")};y["size"]=human_size(y["size"]) if y.get("size") else ("Calculating…" if not job.get("size_ready") else "Size unavailable");y["token"]=make_token(job_id,x["id"]);response.append(y)
    return jsonify(title=job["title"],formats=response,size_ready=job.get("size_ready",False))

@app.get("/download/<path:token>")
def download(token):
    verified=verify_token(token)
    if not verified: abort(404)
    jid,fid=verified
    with LOCK:
        job=JOBS.get(jid)
    log.info("DOWNLOAD request ip=%s job=%s format=%s", client_ip(), jid, fid)
    if not job or time.time()-job["created"]>JOB_TTL: abort(404)
    fmt=job["formats"].get(fid)
    if not fmt: abort(404)
    log.info("DOWNLOAD SELECTED | job=%s | label=%s | kind=%s | detail=%s | estimated_size=%s | client=%s",
             jid, fmt.get("label"), fmt.get("kind"), fmt.get("detail"),
             human_size(fmt.get("size")) if fmt.get("size") else "unavailable", client_ip())

    work=os.path.join(TMP_ROOT,uuid.uuid4().hex)
    os.makedirs(work,exist_ok=True)
    if job.get("cookie_text"):
        Path(os.path.join(work,"cookie.txt")).write_text(job["cookie_text"],encoding="utf-8")
    acquired=DOWNLOAD_SEM.acquire(timeout=2)
    if not acquired:
        cleanup(work)
        return jsonify(error="Server is busy. Please try again shortly."),429
    try:
        out=os.path.join(work,"%(title).180B [%(id)s].%(ext)s")
        opts={
            "format":fmt["format"],"outtmpl":out,"noplaylist":True,
            **({"cookiefile": os.path.join(work,"cookie.txt")} if job.get("cookie_text") else {}),
            "quiet":True,"no_warnings":True,"retries":3,"fragment_retries":3,
            "concurrent_fragment_downloads":4,"socket_timeout":30,
            "js_runtimes":{"deno":{}},
            "http_chunk_size":10485760,
            "retry_sleep_functions":{"http": lambda n: min(10, 1.5 ** n)},
            "restrictfilenames":True,"max_filesize":MAX_FILE_BYTES,
            "merge_output_format":fmt["container"],
            "paths":{"home":work,"temp":work},
            "nopart":False,
            # aria2c is a fallback/accelerator for direct HTTP/HTTPS downloads.
            "external_downloader":"aria2c",
            "external_downloader_args":{"aria2c":["-x","8","-s","8","-k","1M","--file-allocation=none"]},
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            info=ydl.extract_info(job["url"],download=True)
            requested=ydl.prepare_filename(info)
        files=[os.path.join(work,n) for n in os.listdir(work)
               if os.path.isfile(os.path.join(work,n)) and not n.endswith((".part",".ytdl"))]
        if not files:
            raise RuntimeError("No output file was created.")
        # Prefer the largest completed media file.
        path=max(files,key=os.path.getsize)
        actual_size=os.path.getsize(path)
        log.info("DOWNLOAD ready job=%s file=%s size=%s", jid, os.path.basename(path), human_size(actual_size))
        log.info("BROWSER response started job=%s", jid)
        ext=fmt["container"]
        base=safe_filename(info.get("title") or "download")
        filename=f"{base}.{ext}"
        resp=send_file(path,as_attachment=True,download_name=filename,
                       conditional=True,max_age=0)
        @resp.call_on_close
        def finish():
            log.info("CLEANUP job=%s temporary_data=%s", jid, work)
            cleanup(work)
            DOWNLOAD_SEM.release()
            with LOCK:
                JOBS.pop(jid,None)
        return resp
    except Exception as e:
        log.exception("DOWNLOAD failed job=%s", jid)
        cleanup(work)
        DOWNLOAD_SEM.release()
        return jsonify(error=str(e)[:1500]),500

if __name__=="__main__":
    app.run(host="0.0.0.0",port=PORT,debug=False,threaded=True)
