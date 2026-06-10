# Stream URL Extractor — Deploy Guide

## Files
- `index.html`  — Full web UI (self-contained, works standalone)
- `server.py`   — FastAPI backend (real extraction)
- `requirements.txt` — Python deps

---

## Option A: Static frontend only (demo mode)
Just open `index.html` in any browser.
- Fetches pipeline JSON directly from GitHub
- Simulates extraction flow (no real stream URLs)
- Export JSON / TXT still works

---

## Option B: Full deploy with real extraction

### Install deps
```bash
pip install -r requirements.txt
```

### Run server
```bash
uvicorn server:app --host 0.0.0.0 --port 8000
```

Place `index.html` in the same directory as `server.py`.
Open http://localhost:8000 — the UI will automatically use the backend.

### Optional extras
```bash
pip install curl-cffi cloudscraper  # improves success rate for some hosts
```

---

## Option C: Deploy to Render / Railway / Fly.io

1. Create `Procfile`:
   ```
   web: uvicorn server:app --host 0.0.0.0 --port $PORT
   ```
2. Push both files to GitHub
3. Set start command to above Procfile line
4. Done — the `/` route serves `index.html`
