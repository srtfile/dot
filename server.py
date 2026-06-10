#!/usr/bin/env python3
"""
Stream URL Extractor — FastAPI Backend
Run: pip install fastapi uvicorn requests beautifulsoup4 cloudscraper
     uvicorn server:app --host 0.0.0.0 --port 8000
"""

import re, json, ast, codecs, random, string, time
from urllib.parse import urlparse
from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Stream URL Extractor", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend
if Path("index.html").exists():
    @app.get("/")
    def serve_index():
        return FileResponse("index.html")

PIPELINE_JSON_URL = "https://raw.githubusercontent.com/srtfile/movie-data/main/pipeline_summary.json"
MAX_WORKERS = 6
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36")

_pipeline_cache: list = []
_pipeline_fetched_at: float = 0
PIPELINE_TTL = 300  # 5 min cache


# ─────────────────────────────────────────────────────────────────────────────
# Request / Response models
# ─────────────────────────────────────────────────────────────────────────────
class ExtractRequest(BaseModel):
    tmdb_ids: List[int]

class StreamResult(BaseModel):
    status: str
    host_label: str
    embed_url: str
    stream_url: Optional[str] = None
    stream_type: Optional[str] = None
    headers: Optional[dict] = None
    error: Optional[str] = None

class MovieResult(BaseModel):
    tmdb_id: int
    imdb_id: Optional[str] = None
    title: str
    results: List[StreamResult]

class ExtractResponse(BaseModel):
    results: List[MovieResult]
    summary: dict


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers (copy from CLI)
# ─────────────────────────────────────────────────────────────────────────────
def _session(headers: dict = None) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    if headers:
        s.headers.update(headers)
    return s

def _to_base(n, base):
    if n == 0: return "0"
    chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    out = []
    while n:
        out.append(chars[n % base])
        n //= base
    return "".join(reversed(out))

def unpack_packer(packed):
    m = re.search(
        r"}\s*\(\s*'((?:[^'\\]|\\.)*)'\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*'((?:[^'\\]|\\.)*)'\s*\.split\(",
        packed, re.DOTALL)
    if not m:
        m = re.search(
            r"eval\(function\(p,a,c,k,e,d\)\{[^}]+\}\('(.*?)',(\d+),(\d+),'(.*?)'\.split\('\|'\)\)\)",
            packed, re.DOTALL)
    if not m:
        return packed
    payload = m.group(1).replace("\\'", "'")
    base = int(m.group(2))
    keys = m.group(4).split("|")
    lookup = {_to_base(i, base): w for i, w in enumerate(keys) if w}
    return re.sub(r"\b\w+\b", lambda mo: lookup.get(mo.group(0), mo.group(0)), payload)

def find_m3u8(text):
    return list(dict.fromkeys(re.findall(
        r'https?://[^\s"\'\]\[<>]+\.m3u8[^\s"\'\]\[<>]*', text)))

def find_mp4(text):
    return list(dict.fromkeys(re.findall(
        r'https?://[^\s"\'\]\[<>]+\.mp4[^\s"\'\]\[<>]*', text)))


# ─────────────────────────────────────────────────────────────────────────────
# Extractors (identical to CLI — pasted inline for single-file deploy)
# ─────────────────────────────────────────────────────────────────────────────
def _mixdrop_unpack(p, a, c, k):
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    def base_encode(n):
        rem = n % a
        digit = chr(rem + 29) if rem > 35 else digits[rem]
        return digit if n < a else base_encode(n // a) + digit
    d = {}
    for i in range(c - 1, -1, -1):
        key = base_encode(i)
        d[key] = k[i] if i < len(k) and k[i] else key
    return re.compile(r'\b\w+\b').sub(lambda mo: d.get(mo.group(0), mo.group(0)), p)

def _mixdrop_extract_args(html):
    start = html.find("eval(function(p,a,c,k,e,d)")
    if start == -1: raise RuntimeError("MixDrop: eval(function... not found")
    i = start + len("eval(function(p,a,c,k,e,d)"); depth = 0
    while i < len(html):
        if html[i] == '{': depth += 1
        elif html[i] == '}':
            depth -= 1
            if depth == 0: i += 1; break
        i += 1
    while i < len(html) and html[i] != '(': i += 1
    i += 1; arg_start = i; depth = 1
    while i < len(html) and depth > 0:
        if html[i] == '(': depth += 1
        elif html[i] == ')': depth -= 1
        i += 1
    return html[arg_start:i - 1]

def extract_mixdrop(url):
    url = url.replace('/f/', '/e/')
    host = urlparse(url).scheme + "://" + urlparse(url).netloc
    r = _session({"Referer": host + "/"}).get(url, timeout=20); r.raise_for_status()
    raw_args = _mixdrop_extract_args(r.text).replace(".split('|')", "")
    data = ast.literal_eval(f"({raw_args})")
    p, a, c, k = str(data[0]), int(data[1]), int(data[2]), data[3]
    if isinstance(k, str): k = k.split('|')
    decoded = _mixdrop_unpack(p, a, c, k)
    vm = re.search(r'MDCore\.wurl\s*=\s*["\']([^"\']+)["\']', decoded)
    if not vm: raise RuntimeError("MixDrop: MDCore.wurl not found")
    video_url = vm.group(1)
    if not video_url.startswith("http"): video_url = "https:" + video_url
    return {"url": video_url, "type": "mp4", "headers": {"Referer": host + "/"}}

def extract_vidmoly(url):
    r = _session({"Referer": "https://vidmoly.biz"}).get(url, timeout=20); r.raise_for_status()
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', r.text, re.DOTALL)
    joined = "\n".join(filter(None, scripts))
    m = re.search(r'file\s*:\s*[\'"]([^\'"]+?\.m3u8[^\'"]*)[\'"]', joined)
    if not m: raise RuntimeError("Vidmoly: m3u8 not found")
    return {"url": m.group(1), "type": "m3u8", "headers": {"Referer": "https://vidmoly.biz"}}

def extract_voe(url):
    from bs4 import BeautifulSoup
    host = urlparse(url).scheme + "://" + urlparse(url).netloc + "/"
    r = _session({"Referer": host}).get(url, timeout=20); r.raise_for_status()
    html = r.text
    if 'Redirecting...' in html:
        new_url = re.search(r"href\s*=\s*'(.*?)';", html).group(1)
        r = _session({"Referer": host}).get(new_url, timeout=20); r.raise_for_status(); html = r.text
    soup = BeautifulSoup(html, 'html.parser')
    script_tag = soup.find('script', attrs={'type': 'application/json'})
    if not script_tag: raise RuntimeError("Voe: JSON script tag not found")
    encoded = re.search(r'\["(.*?)"\]', script_tag.string).group(1)
    data = codecs.decode(encoded, 'rot_13')
    for p in ["@$", "^^", "~@", "%?", "*~", "!!", "#&"]:
        data = re.sub(re.escape(p), "_", data)
    data = data.replace("_", "")
    data = b64decode(data).decode()
    data = ''.join(chr(ord(c) - 3) for c in data)
    data = data[::-1]
    data = b64decode(data).decode()
    parsed = json.loads(data)
    video_url = parsed.get('source') or parsed.get('hls') or parsed.get('url')
    if not video_url: raise RuntimeError("Voe: source URL not found")
    return {"url": video_url, "type": "m3u8" if ".m3u8" in video_url else "mp4", "headers": {"Referer": host}}

def extract_streamwish(url):
    m = re.search(r'/e/([A-Za-z0-9]+)', url)
    if not m: raise ValueError("StreamWish: cannot parse file code")
    file_code = m.group(1)
    origin = urlparse(url).netloc
    target = f"https://playnixes.com/e/{file_code}"
    r = _session({"Referer": f"https://{origin}/"}).get(target, timeout=20); r.raise_for_status()
    packed = re.search(r"(eval\(function\(p,a,c,k,e,d\)\{.*?\.split\('\|'\)[^)]*\)\))", r.text, re.DOTALL)
    if not packed:
        urls = find_m3u8(r.text)
        if urls: return {"url": urls[0], "type": "m3u8", "extra": urls}
        raise ValueError("StreamWish: packed JS not found")
    decoded = unpack_packer(packed.group(1))
    streams = dict(re.findall(r'"(hls[234])"\s*:\s*"([^"]+)"', decoded))
    extra = find_m3u8(decoded)
    best = streams.get("hls4") or streams.get("hls3") or streams.get("hls2") or (extra[0] if extra else None)
    if not best: raise RuntimeError("StreamWish: no stream URL found")
    return {"url": best, "type": "m3u8", "streams": streams, "extra": extra}

def extract_generic(url):
    try:
        from curl_cffi import requests as cf
        r = cf.get(url, impersonate="chrome", timeout=25,
                   headers={"Referer": urlparse(url).scheme + "://" + urlparse(url).netloc + "/"})
        html = r.text
    except Exception:
        r = _session().get(url, timeout=20); html = r.text
    html = html.replace("\\/", "/")
    packed = re.search(r"(eval\(function\(p,a,c,k,e,d\)\{.*?\.split\('\|'\)[^)]*\)\))", html, re.DOTALL)
    text = html
    if packed: text = html + "\n" + unpack_packer(packed.group(1))
    m3us = find_m3u8(text); mp4s = find_mp4(text)
    combined = m3us + [u for u in mp4s if u not in m3us]
    if combined:
        return {"url": combined[0], "type": "m3u8" if combined[0] in m3us else "mp4", "extra": combined}
    raise RuntimeError("Generic: no stream URL found")

HOST_MAP = {
    "mixdrop":    ["mixdrop"],
    "vidmoly":    ["vidmoly"],
    "voe":        ["voe.sx", "kellywhatcould"],
    "streamwish": ["streamwish", "playnixes"],
    "generic":    [],
}

EXTRACTOR_MAP = {
    "mixdrop":    extract_mixdrop,
    "vidmoly":    extract_vidmoly,
    "voe":        extract_voe,
    "streamwish": extract_streamwish,
    "generic":    extract_generic,
}

def detect_host(url):
    host = urlparse(url).netloc.lower().lstrip("www.")
    for family, patterns in HOST_MAP.items():
        for p in patterns:
            if p in host: return family
    return "generic"

def extract_stream(url):
    host = detect_host(url)
    fn = EXTRACTOR_MAP.get(host, extract_generic)
    result = fn(url)
    result["host"] = host
    result["input_url"] = url
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline helpers
# ─────────────────────────────────────────────────────────────────────────────
def get_pipeline():
    global _pipeline_cache, _pipeline_fetched_at
    if time.time() - _pipeline_fetched_at < PIPELINE_TTL and _pipeline_cache:
        return _pipeline_cache
    r = requests.get(PIPELINE_JSON_URL, timeout=30); r.raise_for_status()
    _pipeline_cache = r.json()
    _pipeline_fetched_at = time.time()
    return _pipeline_cache

def get_embed_urls(entry):
    urls = []
    i = 1
    while True:
        uk = f"url-{i}"; hk = f"host-{i}"
        if uk not in entry: break
        urls.append({"host_label": entry.get(hk, "unknown"), "embed_url": entry[uk]})
        i += 1
    return urls

def process_embed(item):
    embed_url = item["embed_url"]
    host_label = item["host_label"]
    try:
        result = extract_stream(embed_url)
        return {
            "status": "ok",
            "host_label": host_label,
            "embed_url": embed_url,
            "stream_url": result.get("url"),
            "stream_type": result.get("type"),
            "headers": result.get("headers"),
        }
    except Exception as e:
        return {
            "status": "error",
            "host_label": host_label,
            "embed_url": embed_url,
            "error": str(e),
        }


# ─────────────────────────────────────────────────────────────────────────────
# API routes
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/api/extract", response_model=ExtractResponse)
def extract(req: ExtractRequest):
    try:
        pipeline = get_pipeline()
    except Exception as e:
        raise HTTPException(503, f"Failed to fetch pipeline: {e}")

    tmdb_set = set(req.tmdb_ids)
    entries = [e for e in pipeline if e.get("tmdb_id") in tmdb_set]
    if not entries:
        raise HTTPException(404, f"No entries found for TMDB IDs: {req.tmdb_ids}")

    all_results = []
    for entry in entries:
        embed_items = get_embed_urls(entry)
        results = [None] * len(embed_items)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(process_embed, item): i for i, item in enumerate(embed_items)}
            for future in as_completed(futures):
                results[futures[future]] = future.result()
        all_results.append({
            "tmdb_id": entry["tmdb_id"],
            "imdb_id": entry.get("imdb_id"),
            "title": entry.get("title", "Unknown"),
            "results": results,
        })

    total_ok  = sum(sum(1 for r in m["results"] if r["status"] == "ok") for m in all_results)
    total_all = sum(len(m["results"]) for m in all_results)
    return {"results": all_results, "summary": {"ok": total_ok, "total": total_all}}


@app.get("/api/health")
def health():
    return {"status": "ok", "pipeline_cached": bool(_pipeline_cache)}


@app.get("/api/pipeline/count")
def pipeline_count():
    try:
        p = get_pipeline()
        return {"count": len(p)}
    except Exception as e:
        raise HTTPException(503, str(e))


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
