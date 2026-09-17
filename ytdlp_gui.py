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
TMP_ROOT = os.path.join(tempfile.gettempdir(), "mediaflow")
os.makedirs(TMP_ROOT, exist_ok=True)
JOBS, RATE = {}, {}
LOCK = threading.RLock()
SEM = threading.BoundedSemaphore(MAX_CONCURRENT)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("mediaflow")

def hs(n):
    if n is None: return "Size unavailable"
    n=float(n); u=["B","KB","MB","GB","TB"]; i=0
    while n>=1024 and i<len(u)-1: n/=1024; i+=1
    return f"{n:.1f} {u[i]}" if i else f"{int(n)} B"

def hspeed(n): return hs(n)+"/s" if n else "—"
def heta(n):
    if n is None: return "—"
    n=int(n); m,s=divmod(n,60); h,m=divmod(m,60)
    return f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")

def safe(n): return (re.sub(r'[\\/:*?"<>|]+',"_",n or "download")[:180].strip(" .") or "download")
def cip(): return request.remote_addr or "unknown"

def rate_ok():
    now=time.time(); k=cip()
    with LOCK:
        a=[x for x in RATE.get(k,[]) if now-x<60]
        if len(a)>=RATE_LIMIT: RATE[k]=a; return False
        a.append(now); RATE[k]=a
    return True

def valid_url(url):
    try:
        p=urlparse(url)
        if p.scheme not in ("http","https") or not p.hostname: return False
        h=p.hostname.lower().rstrip(".")
        if h=="localhost" or h.endswith(".local"): return False
        for x in socket.getaddrinfo(h,None):
            a=ipaddress.ip_address(x[4][0])
            if a.is_private or a.is_loopback or a.is_link_local or a.is_reserved: return False
        return True
    except Exception: return False

def make_token(jid,idx):
    raw=json.dumps({"job":jid,"fmt":idx},separators=(",",":"),sort_keys=True).encode()
    sig=hmac.new(SECRET.encode(),raw,hashlib.sha256).hexdigest().encode()
    import base64
    return base64.urlsafe_b64encode(raw+b"."+sig).decode().rstrip("=")

def check_token(t):
    try:
        import base64
        b=base64.urlsafe_b64decode(t+"="*(-len(t)%4)); raw,sig=b.rsplit(b".",1)
        exp=hmac.new(SECRET.encode(),raw,hashlib.sha256).hexdigest().encode()
        if not hmac.compare_digest(sig,exp): return None
        return json.loads(raw.decode())
    except Exception: return None

def tiny_probe(url):
    if not requests or not url: return None
    try:
        r=requests.get(url,headers={"Range":"bytes=0-0","User-Agent":"Mozilla/5.0"},stream=True,timeout=10)
        cr=r.headers.get("Content-Range","")
        m=re.search(r"/(\d+)$",cr); r.close()
        return int(m.group(1)) if m else None
    except Exception: return None

def fsize(f,dur):
    for k in ("filesize","filesize_approx"):
        if f.get(k): return int(f[k])
    if dur and f.get("tbr"):
        try: return int(float(f["tbr"])*1000/8*float(dur))
        except Exception: pass
    return tiny_probe(f.get("url"))

def reslabel(f):
    w,h=f.get("width"),f.get("height")
    if not h: return "Audio"
    if w:
        x=max(int(w),int(h))
        return {2160:"2160p",1440:"1440p",1080:"1080p",720:"720p",480:"480p",360:"360p",240:"240p",144:"144p"}.get(x,f"{w}×{h}")
    return f"{h}p"

def make_formats(info):
    dur=info.get("duration"); videos=[]; audios=[]
    for raw in info.get("formats") or []:
        if not raw.get("format_id"): continue
        f=dict(raw); f["_size"]=fsize(f,dur)
        if f.get("vcodec") not in (None,"none"): videos.append(f)
        elif f.get("acodec") not in (None,"none"): audios.append(f)
    ba=max(audios,key=lambda x:(x.get("abr") or 0,x.get("_size") or 0),default=None)
    out=[]; seen=set()
    videos.sort(key=lambda x:((x.get("height") or 0),(x.get("tbr") or 0)),reverse=True)
    for v in videos:
        key=(v.get("width"),v.get("height"))
        if key in seen: continue
        seen.add(key); sz=v.get("_size")
        if ba and sz is not None and ba.get("_size") is not None: sz+=ba["_size"]
        selector=v["format_id"]+(f"+{ba['format_id']}" if ba else "")
        for c in ("mp4","mkv"):
            out.append({"kind":"video","label":f"{reslabel(v)} • {c.upper()}","detail":"Video + best audio",
                        "size":hs(sz),"size_bytes":sz,"selector":selector,"container":c})
    for a in sorted(audios,key=lambda x:x.get("abr") or 0,reverse=True)[:12]:
        ext=(a.get("ext") or "m4a").lower(); abr=a.get("abr")
        out.append({"kind":"audio","label":f"{ext.upper()} • {int(abr) if abr else '?'}kbps",
                    "detail":"Original audio","size":hs(a.get("_size")),"size_bytes":a.get("_size"),
                    "selector":a["format_id"],"container":ext})
    return out[:80]

@app.after_request
def headers(r):
    r.headers["X-Content-Type-Options"]="nosniff"; r.headers["X-Frame-Options"]="DENY"
    r.headers["Referrer-Policy"]="no-referrer"; r.headers["Cache-Control"]="no-store"
    return r

@app.get("/health")
def health(): return jsonify(ok=True,service="MediaFlow PRO")

@app.get("/")
def index(): return render_template_string(PAGE)

@app.post("/api/formats")
def analyze():
    if not rate_ok(): return jsonify(error="Too many requests"),429
    url=(request.form.get("url") or "").strip()
    if not valid_url(url): return jsonify(error="Invalid or blocked URL"),400
    cookie=""
    up=request.files.get("cookie_file")
    if up and up.filename:
        raw=up.read()
        if len(raw)>2*1024*1024: return jsonify(error="cookie.txt is too large"),400
        cookie=raw.decode("utf-8","ignore")
        if "# Netscape HTTP Cookie File" not in cookie and "\t" not in cookie:
            return jsonify(error="Please upload a Netscape-format cookies.txt file"),400
    jid=uuid.uuid4().hex; cp=None
    try:
        o={"quiet":True,"no_warnings":True,"skip_download":True,"noplaylist":True,
           "retries":3,"fragment_retries":3,"socket_timeout":20,"js_runtimes":{"deno":{}}}
        if cookie:
            cp=os.path.join(TMP_ROOT,"cookie-"+jid+".txt"); Path(cp).write_text(cookie,encoding="utf-8"); o["cookiefile"]=cp
        log.info("ANALYZE request ip=%s url=%s",cip(),url)
        with yt_dlp.YoutubeDL(o) as ydl: info=ydl.extract_info(url,download=False)
        fs=make_formats(info)
        JOBS[jid]={"created":time.time(),"url":url,"title":info.get("title") or url,"formats":fs,"cookie":cookie,"status":"analyzed","progress":{}}
        for i,f in enumerate(fs):
            log.info("FORMAT | index=%d | kind=%s | label=%s | size=%s",i,f["kind"],f["label"],f["size"])
        result=[]
        for i,f in enumerate(fs):
            x=dict(f,token=make_token(jid,i)); x.pop("selector",None); x.pop("size_bytes",None); result.append(x)
        return jsonify(job_id=jid,title=JOBS[jid]["title"],thumbnail=info.get("thumbnail"),formats=result)
    except Exception as e:
        log.exception("ANALYZE failed"); return jsonify(error=str(e)),500
    finally:
        if cp:
            try: os.remove(cp)
            except OSError: pass

def worker(jid,fmt,wd):
    j=JOBS[jid]; cp=None
    try:
        if j.get("cookie"):
            cp=os.path.join(wd,"cookie.txt"); Path(cp).write_text(j["cookie"],encoding="utf-8")
        st={"percent":0,"downloaded":"0 B","total":fmt["size"],"speed":"—","eta":"—"}; j["progress"]=st
        def hook(d):
            if d.get("status")=="downloading":
                total=d.get("total_bytes") or d.get("total_bytes_estimate") or 0; done=d.get("downloaded_bytes") or 0
                st.update(percent=round(done*100/total,1) if total else 0,downloaded=hs(done),
                          total=hs(total) if total else fmt["size"],speed=hspeed(d.get("speed")),eta=heta(d.get("eta")))
                log.info("PROGRESS job=%s %.1f%% %s/%s speed=%s ETA=%s",jid,st["percent"],st["downloaded"],st["total"],st["speed"],st["eta"])
            elif d.get("status")=="finished": st["percent"]=100
        o={"format":fmt["selector"],"outtmpl":os.path.join(wd,"%(title)s [%(id)s].%(ext)s"),
           "noplaylist":True,"restrictfilenames":True,"quiet":True,"no_warnings":True,
           "retries":5,"fragment_retries":5,"concurrent_fragment_downloads":4,"socket_timeout":30,
           "max_filesize":int(MAX_FILE_GB*1024**3),"merge_output_format":fmt.get("container"),
           "progress_hooks":[hook],"js_runtimes":{"deno":{}},"paths":{"home":wd,"temp":wd}}
        if cp:o["cookiefile"]=cp
        with yt_dlp.YoutubeDL(o) as ydl: ydl.download([j["url"]])
        files=[p for p in Path(wd).iterdir() if p.is_file() and p.name!="cookie.txt"]
        if not files: raise RuntimeError("No output file was produced")
        p=max(files,key=lambda x:x.stat().st_size)
        j.update(status="ready",result=str(p),filename=safe(p.name)); st["percent"]=100
        log.info("DOWNLOAD READY job=%s size=%s",jid,hs(p.stat().st_size))
    except Exception as e:
        j.update(status="error",error=str(e)); log.exception("DOWNLOAD failed job=%s",jid)
    finally:
        if cp:
            try: os.remove(cp)
            except OSError: pass

@app.get("/api/progress/<token>")
def progress(token):
    p=check_token(token)
    if not p or p["job"] not in JOBS:return jsonify(error="expired"),404
    j=JOBS[p["job"]];return jsonify(status=j.get("status"),progress=j.get("progress",{}),error=j.get("error"))

@app.get("/download/<token>")
def download(token):
    p=check_token(token)
    if not p:return "Invalid or expired download link",404
    jid=p["job"];j=JOBS.get(jid)
    if not j:return "Download job expired or not found",404
    if time.time()-j["created"]>JOB_TTL:JOBS.pop(jid,None);return "Download link expired",410
    try:f=j["formats"][int(p["fmt"])]
    except (ValueError,TypeError,KeyError,IndexError):return "Format not found",404
    if not SEM.acquire(blocking=False):return "Server is busy. Try again shortly.",503
    wd=tempfile.mkdtemp(prefix="mediaflow-",dir=TMP_ROOT);j["status"]="downloading"
    threading.Thread(target=worker,args=(jid,f,wd),daemon=True).start()
    deadline=time.time()+3600
    while time.time()<deadline:
        if j.get("status")=="ready":break
        if j.get("status")=="error":
            SEM.release();shutil.rmtree(wd,ignore_errors=True);return jsonify(error=j.get("error")),500
        time.sleep(.25)
    if j.get("status")!="ready":
        SEM.release();shutil.rmtree(wd,ignore_errors=True);return "Download timed out",504
    r=send_file(j["result"],as_attachment=True,download_name=j["filename"],max_age=0,conditional=True)
    @r.call_on_close
    def cleanup_response():
        shutil.rmtree(wd,ignore_errors=True)
        with LOCK:JOBS.pop(jid,None)
        SEM.release();log.info("CLEANUP job=%s temporary files removed",jid)
    return r

PAGE = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MediaFlow PRO</title>
<style>
body{margin:0;background:#080b10;color:#f5f7fb;font:15px system-ui,Segoe UI,sans-serif}.wrap{max-width:1050px;margin:auto;padding:34px 18px 70px}
.brand{font-size:30px;font-weight:800}.sub,.meta,.status{color:#91a0b3}.box,.format{background:#111722;border:1px solid #263142;border-radius:15px;padding:17px;margin:14px 0}
.row{display:flex;gap:10px}.url{flex:1;background:#0b111b;color:white;border:1px solid #263142;border-radius:10px;padding:13px}
.btn{border:0;border-radius:10px;padding:12px 18px;background:#58a6ff;font-weight:800;cursor:pointer}.download{background:#35c48b}
.format{display:flex;align-items:center;justify-content:space-between;gap:15px}.bar{height:5px;background:#080d14;border-radius:8px;margin-top:9px}.bar i{display:block;height:100%;width:0;background:#58a6ff}
.cookie{margin-top:12px;color:#91a0b3}@media(max-width:650px){.row,.format{flex-direction:column;align-items:stretch}.download{width:100%}}
</style></head><body><div class="wrap"><div class="brand">MediaFlow PRO</div><div class="sub">Koyeb Ready • temporary processing • automatic cleanup</div>
<div class="box"><div class="row"><input class="url" id="url" placeholder="Paste video URL"><button class="btn" id="go">Analyze</button></div>
<div class="cookie">Optional cookies.txt: <input type="file" id="cookie" accept=".txt"></div><div class="status" id="msg"></div></div><div id="out"></div></div>
<script>
const $=x=>document.querySelector(x),esc=x=>String(x??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
$('#go').onclick=async()=>{const u=$('#url').value.trim();if(!u)return;$('#msg').textContent='Analyzing…';$('#out').innerHTML='';
const fd=new FormData();fd.append('url',u);const c=$('#cookie').files[0];if(c)fd.append('cookie_file',c);
try{const r=await fetch('/api/formats',{method:'POST',body:fd});const d=await r.json();if(!r.ok)throw Error(d.error||'Analysis failed');$('#msg').textContent=d.title||'Available formats';
const box=document.createElement('div');d.formats.forEach(f=>{const e=document.createElement('div');e.className='format';
e.innerHTML='<div><b>'+esc(f.label)+'</b><div class="meta">'+esc(f.detail)+' · <b>'+esc(f.size)+'</b></div><div class="bar"><i></i></div><div class="status"></div></div><button class="btn download">Download</button>';
e.querySelector('button').onclick=()=>dl(f.token,e);box.appendChild(e)});$('#out').appendChild(box)}catch(e){$('#msg').textContent='Error: '+e.message}};
async function dl(t,e){const b=e.querySelector('button'),s=e.querySelector('.status'),i=e.querySelector('i');b.disabled=true;b.textContent='Preparing…';
const poll=setInterval(async()=>{try{const d=await(await fetch('/api/progress/'+encodeURIComponent(t))).json();const p=d.progress||{};i.style.width=(p.percent||0)+'%';s.textContent=(p.percent||0)+'% · '+(p.downloaded||'0 B')+' / '+(p.total||'unknown')+' · '+(p.speed||'—')+' · ETA '+(p.eta||'—')}catch(_){}} ,700);
try{const r=await fetch('/download/'+encodeURIComponent(t));if(!r.ok)throw Error(await r.text());const blob=await r.blob();const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=e.querySelector('b').textContent.replaceAll(' ','_');a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1000);i.style.width='100%';s.textContent='Download complete • temporary file cleaned'}catch(x){s.textContent='Error: '+x.message}
clearInterval(poll);b.disabled=false;b.textContent='Download'}
</script></body></html>"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
