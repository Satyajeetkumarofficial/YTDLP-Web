#!/usr/bin/env python3
import os, uuid, threading, tempfile, shutil, mimetypes
from flask import Flask, request, jsonify, render_template_string, send_file, abort
import yt_dlp

app = Flask(__name__)
JOBS = {}

# Temporary per-job data only. Nothing is kept permanently.
TMP_ROOT = os.path.join(tempfile.gettempdir(), "ytdlp_koyeb")
os.makedirs(TMP_ROOT, exist_ok=True)

PAGE = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MediaFlow — Multi-Site Downloader</title>
<style>
:root{--bg:#070a10;--card:#101620;--card2:#0c1119;--line:#202a38;--text:#eef4ff;--muted:#8c9ab0;--accent:#6ea8fe;--accent2:#8b7cff;--ok:#55d187}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0,#17243b 0,transparent 38%),var(--bg);color:var(--text);font:15px Inter,system-ui,-apple-system,Segoe UI,sans-serif}
.wrap{max-width:920px;margin:auto;padding:35px 18px 70px}.brand{font-size:28px;font-weight:800;letter-spacing:-.6px}.brand span{color:var(--accent)}
.sub{color:var(--muted);margin:5px 0 28px}.search{background:rgba(16,22,32,.9);border:1px solid var(--line);padding:14px;border-radius:18px;display:flex;gap:10px;box-shadow:0 15px 45px #0006}
input{flex:1;background:#080d14;border:1px solid var(--line);color:var(--text);padding:14px;border-radius:12px;outline:none;font-size:15px}.btn{border:0;border-radius:12px;padding:0 22px;background:linear-gradient(135deg,var(--accent),var(--accent2));color:white;font-weight:800;cursor:pointer}.btn:disabled{opacity:.5}
#msg{color:var(--muted);padding:14px 3px}.section{margin-top:24px}.section h2{font-size:14px;text-transform:uppercase;letter-spacing:.12em;color:var(--muted)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:12px}.format{background:linear-gradient(145deg,var(--card),var(--card2));border:1px solid var(--line);border-radius:16px;padding:15px;display:flex;justify-content:space-between;gap:12px;align-items:center}.info b{display:block;font-size:16px}.info small{display:block;color:var(--muted);margin-top:4px}.download{background:#17263a;color:#dceaff;border:1px solid #29405e;border-radius:10px;padding:10px 13px;font-weight:700;cursor:pointer;white-space:nowrap}.download:hover{background:#203653}.note{margin-top:25px;color:#728097;font-size:12px;text-align:center}
@media(max-width:600px){.search{flex-direction:column}.btn{height:48px}.format{align-items:flex-start}.download{padding:9px 10px}}
</style>
</head>
<body><div class="wrap">
<div class="brand">⚡ Media<span>Flow</span></div>
<div class="sub">Fast multi-site media downloader · Browser download</div>
<div class="search">
<input id="url" placeholder="Paste a supported video, audio or media URL…" autocomplete="off">
<button class="btn" id="analyze" onclick="analyze()">Analyze</button>
</div>
<div id="msg"></div><div id="results"></div>
<div class="note">Files are processed temporarily for the requested download and cleaned up afterward.</div>
</div>
<script>
async function analyze(){
 const u=document.getElementById('url').value.trim(); if(!u)return;
 const b=document.getElementById('analyze'); b.disabled=true; b.textContent='Analyzing…';
 document.getElementById('results').innerHTML=''; document.getElementById('msg').textContent='Finding available formats…';
 try{
  const r=await fetch('/analyze',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url:u})});
  const d=await r.json(); if(!r.ok) throw Error(d.error||'Could not analyze URL');
  render(d);
 }catch(e){document.getElementById('msg').textContent='❌ '+e.message}
 finally{b.disabled=false;b.textContent='Analyze'}
}
function render(d){
 document.getElementById('msg').textContent=(d.title||'Media')+' — '+d.formats.length+' downloadable options';
 const v=d.formats.filter(x=>x.kind==='video'), a=d.formats.filter(x=>x.kind==='audio');
 let h='';
 if(v.length) h+='<div class="section"><h2>🎬 Video</h2><div class="grid">'+v.map(card).join('')+'</div></div>';
 if(a.length) h+='<div class="section"><h2>🎵 Audio</h2><div class="grid">'+a.map(card).join('')+'</div></div>';
 document.getElementById('results').innerHTML=h||'<div class="section">No downloadable formats found.</div>';
}
function esc(s){return String(s).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
function card(x){
 return `<div class="format"><div class="info"><b>${esc(x.label)}</b><small>${esc(x.detail)}</small></div><button class="download" onclick="download('${x.token}')">Download</button></div>`;
}
function download(t){ window.location.href='/download/'+encodeURIComponent(t); }
document.getElementById('url').addEventListener('keydown',e=>{if(e.key==='Enter')analyze()});
</script>
</body></html>
"""

def cleanup(path):
    try: shutil.rmtree(path, ignore_errors=True)
    except Exception: pass

def format_options(info):
    out=[]
    seen=set()
    # Combined formats: show common container/quality choices.
    for f in info.get("formats", []):
        fid=f.get("format_id")
        if not fid or fid in seen: continue
        ext=f.get("ext") or "file"
        h=f.get("height")
        vcodec=f.get("vcodec")
        acodec=f.get("acodec")
        if vcodec and vcodec != "none":
            if h:
                label=f"{h}p • {ext.upper()}"
            else:
                label=f"Video • {ext.upper()}"
            if acodec and acodec != "none":
                detail="Video + Audio"
            else:
                detail="Video only"
            out.append({"fid":fid,"kind":"video","label":label,"detail":detail,
                        "height":h or 0,"has_audio":bool(acodec and acodec!="none"),
                        "ext":ext})
        elif acodec and acodec != "none":
            abr=f.get("abr")
            label=f"{ext.upper()}"+(f" • {int(abr)}kbps" if abr else "")
            out.append({"fid":fid,"kind":"audio","label":label,"detail":"Audio only",
                        "height":0,"has_audio":True,"ext":ext})
    # Prefer useful options and cap duplicate low-level formats.
    videos=[x for x in out if x["kind"]=="video"]
    audios=[x for x in out if x["kind"]=="audio"]
    videos=sorted(videos,key=lambda x:(x["height"],x["has_audio"]),reverse=True)
    selected=[]; heights=set()
    for x in videos:
        if x["height"] not in heights:
            selected.append(x); heights.add(x["height"])
    selected += audios[:12]
    return selected

@app.route("/")
def index(): return render_template_string(PAGE)

@app.post("/analyze")
def analyze():
    data=request.get_json(silent=True) or {}
    url=(data.get("url") or "").strip()
    if not url: return jsonify(error="URL is required"),400
    try:
        opts={"quiet":True,"no_warnings":True,"skip_download":True,"noplaylist":True}
        with yt_dlp.YoutubeDL(opts) as ydl:
            info=ydl.extract_info(url,download=False)
        formats=format_options(info)
        token=str(uuid.uuid4())
        JOBS[token]={"url":url,"formats":formats}
        # Remove old jobs opportunistically.
        if len(JOBS)>100:
            for k in list(JOBS)[:30]: JOBS.pop(k,None)
        return jsonify(title=info.get("title","Media"),formats=[
            {k:x[k] for k in ("kind","label","detail")}|{"token":token+":"+x["fid"]}
            for x in formats
        ])
    except Exception as e:
        return jsonify(error=str(e)),400

@app.get("/download/<path:key>")
def download(key):
    try:
        token,fid=key.split(":",1)
        job=JOBS.get(token)
        if not job: abort(404)
        fmt=next(x for x in job["formats"] if x["fid"]==fid)
    except Exception: abort(404)

    work=os.path.join(TMP_ROOT,str(uuid.uuid4()))
    os.makedirs(work,exist_ok=True)
    try:
        # Prefer requested exact format; for video-only formats, merge best audio.
        if fmt["kind"]=="video":
            selector=fid if fmt["has_audio"] else f"{fid}+bestaudio/best"
        else:
            selector=fid
        opts={
            "format":selector,
            "outtmpl":os.path.join(work,"%(title)s [%(id)s].%(ext)s"),
            "noplaylist":True,"quiet":True,"no_warnings":True,
            "restrictfilenames":True,"merge_output_format":fmt["ext"]
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            info=ydl.extract_info(job["url"],download=True)
            path=ydl.prepare_filename(info)
        # Merging can change extension.
        candidates=[os.path.join(work,x) for x in os.listdir(work)]
        if not os.path.exists(path):
            if candidates: path=max(candidates,key=os.path.getsize)
            else: raise RuntimeError("Downloaded file was not created")
        name=os.path.basename(path)
        response=send_file(path,as_attachment=True,download_name=name)
        @response.call_on_close
        def remove_temp():
            cleanup(work)
        return response
    except Exception as e:
        cleanup(work)
        return jsonify(error=str(e)),500

@app.get("/health")
def health(): return "OK",200

if __name__=="__main__":
    port=int(os.environ.get("PORT","8000"))
    app.run(host="0.0.0.0",port=port,debug=False)
