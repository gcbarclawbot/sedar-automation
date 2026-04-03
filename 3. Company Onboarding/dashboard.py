"""
dashboard.py - MM Company Onboarding Dashboard
================================================
Flask server for the Company Onboarding dashboard.
URL: http://100.107.184.77:5758

Endpoints:
  GET  /                         - main page
  GET  /api/companies            - list of onboarded companies
  GET  /api/company/<symbol>     - company data (filings, state, run status)
  GET  /api/pdf/<symbol>/<path>  - serve PDF file
  POST /api/run/<symbol>         - trigger update run for company
  GET  /api/run-status/<symbol>  - polling: run status + log tail

Start:
  python dashboard.py
  Or: Start-Process -FilePath "python" -ArgumentList "dashboard.py"
        -WorkingDirectory "...3. Company Onboarding" -WindowStyle Hidden
"""

import sys, os, re, csv, json, time, subprocess, threading
from pathlib import Path
from datetime import datetime, date
import pytz

SCRIPT_DIR   = Path(__file__).parent
RESULTS_DIR  = SCRIPT_DIR / "Results"
UNIVERSE_CSV = SCRIPT_DIR.parent / "1. Canadian Master Sync" / "canadian_universe.csv"
LOG_PATH     = SCRIPT_DIR / "dashboard.log"

def _load_universe_lookup() -> dict:
    """Load canadian_universe.csv as dict keyed by symbol (cached at module level)."""
    lookup = {}
    try:
        with open(UNIVERSE_CSV, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                sym = row.get("symbol", "").strip().upper()
                if sym:
                    lookup[sym] = row
    except Exception:
        pass
    return lookup

_universe = _load_universe_lookup()
TORONTO_TZ  = pytz.timezone("America/Toronto")

# Load config
PORT = 5758
HOST = "0.0.0.0"
try:
    import tomllib
    with open(SCRIPT_DIR / "config.toml", "rb") as f:
        _cfg = tomllib.load(f)
    PORT = _cfg.get("dashboard", {}).get("port", 5758)
    HOST = _cfg.get("dashboard", {}).get("host", "0.0.0.0")
except Exception:
    pass

# Force UTF-8
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from flask import Flask, jsonify, request, send_file, render_template, Response
app = Flask(__name__, template_folder="templates", static_folder="static")

# Track running processes: symbol -> {process, started_at, log_path}
_running: dict = {}
_running_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_onboarded_companies() -> list[dict]:
    """Return list of all onboarded companies (have a state.json)."""
    companies = []
    if not RESULTS_DIR.exists():
        return companies
    for d in sorted(RESULTS_DIR.iterdir()):
        if not d.is_dir():
            continue
        state_path = d / "state.json"
        csv_path   = d / "filings_log.csv"
        if not state_path.exists():
            continue
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            state = {}
        companies.append({
            "symbol":        d.name,
            "last_run":      state.get("last_run", ""),
            "last_run_date": state.get("last_run_date", ""),
            "run_mode":      state.get("run_mode", ""),
            "aif_filing_date": state.get("aif_filing_date", ""),
            "as_at_date":    state.get("as_at_date", ""),
            "has_csv":       csv_path.exists(),
        })
    return companies

def load_filings(symbol: str) -> list[dict]:
    """Load filings_log.csv for a company."""
    csv_path = RESULTS_DIR / symbol / "filings_log.csv"
    if not csv_path.exists():
        return []
    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        # Don't send full news_text over API - truncate to 500 chars for preview
        for r in rows:
            if len(r.get("news_text", "")) > 500:
                r["news_text_preview"] = r["news_text"][:500] + "…"
                r["news_text"] = r["news_text"]  # keep full for detail endpoint
        return rows
    except Exception as e:
        return []

def get_run_log_tail(symbol: str, lines: int = 100) -> list[str]:
    """Get last N lines of run.log for a company."""
    log_path = RESULTS_DIR / symbol / "run.log"
    if not log_path.exists():
        return []
    try:
        text  = log_path.read_text(encoding="utf-8", errors="replace")
        return text.splitlines()[-lines:]
    except Exception:
        return []

def parse_progress(log_lines: list[str]) -> dict:
    """
    Parse run.log lines to extract progress info for the progress bar.
    Returns {stage, stage_num, total_stages, pct, status, detail}
    """
    stage_map = {
        "STAGE 1/5": (1, "AIF lookup & download"),
        "STAGE 2/5": (2, "Stockwatch SEDAR search"),
        "STAGE 3/5": (3, "SEDAR+ gap fill (14 days)"),
        "STAGE 4/5": (4, "Fetching news release text"),
        "STAGE 5/5": (5, "LLM classification"),
        "STAGE 6/6": (6, "Presentation scan"),
        "COMPLETE":  (7, "Complete"),
    }
    current_stage = 0
    current_label = "Starting..."
    detail = ""
    status = "running"

    for line in log_lines:
        for key, (num, label) in stage_map.items():
            if key in line:
                current_stage = num
                current_label = label
        # Extract detail from download lines
        if "DL " in line or "→" in line or "classified" in line or "found" in line:
            detail = line.split(" INFO ")[-1].strip() if " INFO " in line else line.strip()
        if "COMPLETE" in line and "STAGE" not in line:
            current_stage = 6
            current_label = "Complete"
            status = "done"
        if "failed:" in line.lower() and "onboarding failed" in line.lower():
            status = "error"
            detail = line.split(" ERROR ")[-1].strip() if " ERROR " in line else line.strip()

    total = 6
    pct = min(int((current_stage / (total + 1)) * 100), 99) if status == "running" else 100
    if status == "done":
        pct = 100

    return {
        "stage":        current_stage,
        "stage_label":  current_label,
        "total_stages": total,
        "pct":          pct,
        "status":       status,
        "detail":       detail[-120:] if detail else "",
    }

# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    import os
    js_path = SCRIPT_DIR / "static" / "dashboard.js"
    v = int(js_path.stat().st_mtime) if js_path.exists() else 0
    return render_template("index.html", now=v)

@app.route("/api/companies")
def api_companies():
    return jsonify(get_onboarded_companies())

@app.route("/api/universe/<symbol>")
def api_universe(symbol):
    """Return universe info for a not-yet-onboarded company (Miners only)."""
    symbol = symbol.upper()
    u = _universe.get(symbol)
    if not u:
        return jsonify({"error": "not in universe"}), 404
    company_type = u.get("company_type", "").lower()
    if "miner" not in company_type:
        return jsonify({"error": "not a miner"}), 404
    return jsonify({
        "symbol":       symbol,
        "name":         u.get("name", symbol),
        "exchange":     u.get("exchange", ""),
        "sedar_party":  u.get("sedar_party_number", ""),
        "commodity":    u.get("primary_commodity", ""),
        "company_type": u.get("company_type", ""),
    })

@app.route("/api/company/<symbol>")
def api_company(symbol):
    symbol = symbol.upper()
    state_path = RESULTS_DIR / symbol / "state.json"
    if not state_path.exists():
        return jsonify({"error": f"{symbol} not onboarded"}), 404

    state    = json.loads(state_path.read_text(encoding="utf-8"))
    filings  = load_filings(symbol)
    log_tail = get_run_log_tail(symbol, 50)

    # Look up exchange + SEDAR party number from canadian_universe.csv
    u = _universe.get(symbol, {})
    exchange    = u.get("exchange", "")
    sedar_party = u.get("sedar_party_number", "")

    # Check if currently running
    with _running_lock:
        is_running = symbol in _running

    # Group filings by category
    by_cat = {}
    for f in filings:
        cat = f.get("category", "Other")
        by_cat.setdefault(cat, []).append(f)

    # Determine prev_last_run for "new since last run" highlighting
    prev_run = state.get("last_run_date", "")

    return jsonify({
        "symbol":     symbol,
        "exchange":   exchange,
        "sedar_party": sedar_party,
        "state":      state,
        "is_running": is_running,
        "prev_last_run_date": prev_run,
        "filings_by_category": by_cat,
        "total_filings": len(filings),
        "log_tail":   log_tail,
    })

@app.route("/api/news-html/<symbol>/<date_key>")
def api_news_html(symbol, date_key):
    """Serve cleaned news release HTML for a given symbol + date (YYYY-MM-DD)."""
    symbol = symbol.upper()
    html_path = RESULTS_DIR / symbol / "news_html" / f"{date_key}.html"
    if not html_path.exists():
        return Response("", status=404)
    raw_html = html_path.read_text(encoding="utf-8")
    # Wrap in a minimal styled page for iframe rendering
    page = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
  body {{
    background: #0f1117; color: #e2e8f0;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    font-size: 13px; line-height: 1.6; padding: 20px 28px; margin: 0;
  }}
  a {{ color: #6366f1; }}
  table {{ border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 12px; }}
  th, td {{ border: 1px solid #252838; padding: 5px 8px; text-align: left; }}
  th {{ background: #1a1d2e; color: #94a3b8; font-weight: 600; }}
  tr:nth-child(even) td {{ background: #141624; }}
  h1,h2,h3,h4 {{ color: #cbd5e1; margin: 14px 0 6px; }}
  p {{ margin: 6px 0; }}
  .News {{ max-width: 860px; }}
</style>
</head>
<body>{raw_html}</body>
</html>"""
    return Response(page, mimetype="text/html")

@app.route("/api/filing-text/<symbol>/<path:filename>")
def api_filing_text(symbol, filename):
    """Return full news_text for a specific filing."""
    symbol = symbol.upper()
    filings = load_filings(symbol)
    for f in filings:
        if f.get("pdf_url", "").endswith(filename) or f.get("article_id", "") == filename:
            return jsonify({"text": f.get("news_text", ""), "headline": f.get("synopsis", "")})
    return jsonify({"error": "not found"}), 404

@app.route("/api/pdf/<symbol>/<path:filepath>")
def api_pdf(symbol, filepath):
    """Serve a PDF file through the browser."""
    # filepath is relative to Results/{symbol}/
    full_path = RESULTS_DIR / symbol / filepath
    if not full_path.exists():
        # Try to find by filename only
        for f in (RESULTS_DIR / symbol).rglob("*.pdf"):
            if f.name == filepath:
                full_path = f
                break
    if not full_path.exists():
        return "PDF not found", 404
    return send_file(str(full_path), mimetype="application/pdf")

@app.route("/api/reset/<symbol>", methods=["POST"])
def api_reset(symbol):
    """Reset a company: delete state.json + filings_log.csv, then trigger a fresh FULL run."""
    symbol = symbol.upper()
    with _running_lock:
        if symbol in _running:
            return jsonify({"error": f"{symbol} is currently running - stop it first"}), 409

    company_dir = RESULTS_DIR / symbol
    if not company_dir.exists():
        return jsonify({"error": f"{symbol} not found"}), 404

    # Delete state and filings so next run is a FULL scrape from scratch
    deleted = []
    for fname in ["state.json", "filings_log.csv", "run.log"]:
        p = company_dir / fname
        if p.exists():
            p.unlink()
            deleted.append(fname)

    # Now trigger a fresh run (reuse api_run logic)
    return api_run(symbol)


@app.route("/api/run/<symbol>", methods=["POST"])
def api_run(symbol):
    """Trigger an update run for a company."""
    symbol = symbol.upper()
    with _running_lock:
        if symbol in _running:
            return jsonify({"error": f"{symbol} already running"}), 409

    # Find the company in canadian_universe.csv
    company_row = _universe.get(symbol)
    if not company_row:
        # Fall back: check if Results folder exists (previously onboarded)
        state_path = RESULTS_DIR / symbol / "state.json"
        if not state_path.exists():
            return jsonify({"error": f"{symbol} not found in universe or Results"}), 404

    # Clear the run.log before starting
    run_log = RESULTS_DIR / symbol / "run.log"
    try:
        run_log.write_text("", encoding="utf-8")
    except Exception:
        pass

    # Start the onboarding script as a subprocess
    python_exe = sys.executable
    script     = str(SCRIPT_DIR / "mm_onboarding.py")
    cmd        = [python_exe, script, "--symbol", symbol]

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    proc = subprocess.Popen(
        cmd,
        cwd=str(SCRIPT_DIR),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    started_at = datetime.now(TORONTO_TZ).isoformat()
    with _running_lock:
        _running[symbol] = {"process": proc, "started_at": started_at}

    # Background thread to clean up when done
    def _cleanup():
        proc.wait()
        with _running_lock:
            _running.pop(symbol, None)
    threading.Thread(target=_cleanup, daemon=True).start()

    return jsonify({"status": "started", "symbol": symbol, "started_at": started_at})

@app.route("/api/running")
def api_running():
    """Return all currently running symbols plus batch progress metadata."""
    with _running_lock:
        runs = {sym: {"started_at": info.get("started_at", ""), "source": "dashboard"}
                for sym, info in _running.items()}
    # Also check lockfiles written by CLI runs
    lock_dir = SCRIPT_DIR / ".running"
    batch_meta = {}
    if lock_dir.exists():
        for lf in lock_dir.glob("*.lock"):
            sym = lf.stem.upper()
            if sym not in runs:
                try:
                    started_at = lf.read_text(encoding="utf-8").strip()
                except Exception:
                    started_at = ""
                runs[sym] = {"started_at": started_at, "source": "cli"}
        # Check for batch metadata file written by the script
        meta_file = lock_dir / "batch_meta.json"
        if meta_file.exists():
            try:
                import json as _json
                batch_meta = _json.loads(meta_file.read_text(encoding="utf-8"))
            except Exception:
                pass
    return jsonify({"running": runs, "batch_meta": batch_meta})

@app.route("/api/run-status/<symbol>")
def api_run_status(symbol):
    """Poll run status. Returns progress + last N log lines."""
    symbol = symbol.upper()
    with _running_lock:
        is_running = symbol in _running
        started_at = _running.get(symbol, {}).get("started_at", "")

    log_lines = get_run_log_tail(symbol, 200)
    progress  = parse_progress(log_lines)

    # If process just started and log is empty, show 'running' not 'done'
    if is_running and not log_lines:
        progress["status"]      = "running"
        progress["pct"]         = 0
        progress["stage_label"] = "Starting..."

    # If process ended but status still says running, mark done
    if not is_running and progress["status"] == "running":
        progress["status"] = "done"
        progress["pct"]    = 100

    return jsonify({
        "is_running":  is_running,
        "started_at":  started_at,
        "progress":    progress,
        "log_lines":   log_lines[-50:],  # last 50 lines for live log panel
    })

# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"MM Company Onboarding Dashboard")
    print(f"Access at: http://100.107.184.77:{PORT}")
    print(f"Results dir: {RESULTS_DIR}")
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
