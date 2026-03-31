"""
universe_builder.py - Canadian Mining Universe Builder (v2)
============================================================
Determines which Canadian-listed companies belong in the mining universe
and classifies them by type and primary commodity.

Pipeline:
  Phase 1: Tier assignment   - classify all master CSV companies by TMX sector/industry + keywords
  Phase 2: Filing fetch      - Stockwatch /News/Sedar HTTP search for AIF/MD&A (parallel workers)
  Phase 3: LLM classify      - GPT classification using prompt.txt (80 parallel workers)
  Phase 4: SEDAR party fetch - Playwright SEDAR+ lookup for confirmed-relevant companies only
                               Tier 1 processed first, then other relevant companies.
                               Runs concurrently with Phase 3 via a background thread queue.

Key design decisions:
  - Stockwatch HTTP for filing fetch (fast, parallelisable) instead of Playwright SEDAR+
  - SEDAR+ party number lookup ONLY for relevant companies (not wasted on Out companies)
  - party numbers cached in results CSV - subsequent runs skip Playwright entirely
  - 15,000 chars of filing text to LLM

Usage:
  python universe_builder.py --phase all         # full run
  python universe_builder.py --phase 1           # tier only
  python universe_builder.py --phase 2           # filing fetch
  python universe_builder.py --phase 3           # LLM classify
  python universe_builder.py --phase 4           # SEDAR party numbers for relevant companies
  python universe_builder.py --phase 2 --limit 20   # test
  python universe_builder.py --phase 2 --symbol BZ  # single company
"""

import sys, csv, json, time, logging, re, os, threading, queue
import argparse
from datetime import datetime, date, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytz
import requests
from bs4 import BeautifulSoup

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
SCRIPT_DIR   = Path(__file__).parent
MASTER_CSV   = SCRIPT_DIR / "canadian_master.csv"
RESULTS_CSV  = SCRIPT_DIR / "canadian_universe.csv"
PROMPT_PATH  = SCRIPT_DIR / "Universe Builder" / "prompt.txt"
LOG_PATH     = SCRIPT_DIR / "universe_builder.log"
PDFS_DIR     = SCRIPT_DIR / "Universe Builder" / "Results" / "pdfs"

CDP_URL      = "http://127.0.0.1:18800"
SEDAR_BASE   = "https://www.sedarplus.ca"
SW_SEDAR_URL = "https://www.stockwatch.com/News/Sedar"
SW_BASE      = "https://www.stockwatch.com"
TORONTO_TZ   = pytz.timezone("America/Toronto")

FILING_WORKERS = 8    # Stockwatch HTTP parallel workers for Phase 2
LLM_WORKERS    = 80   # OpenAI parallel workers for Phase 3
TEXT_CHARS     = 15000 # chars of filing text to feed LLM

# Exact Stockwatch doc_type strings we want, in priority order (English only)
# French equivalents (INTERIM_MDA_FR, MDA_FR etc.) are explicitly excluded
FILING_DOC_TYPES = [
    # (exact_doc_type_strings, label)
    ({"ANNUAL_INFORMATION_FORM_EN"},                       "AIF"),
    ({"INTERIM_MDA_EN"},                                   "Interim MD&A"),
    ({"MDA_EN", "MDA_AMENDED_EN"},                        "Annual MD&A"),
    ({"ANNUAL_REPORT_EN"},                                 "Annual Report"),
]

# Keep FILING_TYPE_PRIORITY for keyword fallback on older/differently-named filings
FILING_TYPE_PRIORITY = [
    ("annual information form", "AIF"),
    ("interim md",              "Interim MD&A"),
    ("management discussion",   "Annual MD&A"),
    ("annual report",           "Annual Report"),
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Result fieldnames
# ---------------------------------------------------------------------------
RESULT_FIELDS = [
    "symbol", "name", "exchange", "sector", "industry", "market_cap", "description",
    "tier",
    "sedar_party_number", "sedar_party_name",
    # Primary filing (AIF preferred)
    "filing_type",       # human label: AIF, Interim MD&A, Annual MD&A, Annual Report
    "filing_date",       # publication date from Stockwatch e.g. 2025-03-27
    "filing_doc_name",   # raw Stockwatch doc_type e.g. ANNUAL_INFORMATION_FORM_EN
    "filing_url",        # Stockwatch PDF URL
    "filing_pdf_path",   # local path to saved PDF
    "filing_text_chars", # chars extracted (up to TEXT_CHARS)
    "sw_symbol",         # Stockwatch symbol (may differ from our master symbol e.g. FR vs AG)
    "sedar_only",        # Y = no Stockwatch match, filing fetched directly from SEDAR+
    # LLM results from primary filing
    "company_type", "company_type_confidence",
    "primary_commodity", "primary_commodity_confidence",
    "adjacent_category", "reasoning",
    "in_universe",
    # Secondary filing (MD&A, fetched when primary is AIF)
    "mda_type", "mda_date", "mda_doc_name", "mda_url", "mda_pdf_path", "mda_text_chars",
    # LLM results from MD&A (cross-check)
    "mda_company_type", "mda_primary_commodity", "mda_reasoning",
    # Discrepancy flag: set if AIF and MD&A classifications differ
    "discrepancy",
    "override_type", "override_commodity",
]

IN_UNIVERSE_TYPES = {
    "Miner", "Royalty & Streaming", "Mining Services", "Mining Technology",
    "Metals Extraction", "Mining Investment Company", "Mining Finance", "Conglomerate",
}

# ---------------------------------------------------------------------------
# PHASE 1: TIER ASSIGNMENT
# ---------------------------------------------------------------------------
TIER1_INDUSTRIES = {"Mining"}

TIER2_COMBOS = {
    ("Materials", "Steel"),
    ("Materials", "Chemicals"),
    ("Finance", "Asset Management Services"),
    ("Finance", "Capital Markets"),
    ("Finance", "Diversified Financial Services"),
    ("Industrials", "Industrial Goods"),
    ("Industrials", "Industrial Machinery"),
    ("Industrials", "Industrial Equipment Distributors"),
    ("Energy", "Other Energy Sources"),
    ("Energy", "Thermal Coal"),
}

MINING_KEYWORDS = [
    "gold","silver","copper","mining","mineral","resource","lithium","uranium",
    "zinc","nickel","cobalt","platinum","exploration","drilling","royalt","stream",
    "metal","iron ore","coal","potash","graphite","rare earth","tungsten",
    "molybdenum","vanadium","manganese","critical mineral","battery metal",
    "precious metal","base metal","tailings","concentrat","smelter","refiner",
    "assay","geological","geophysic",
]

def assign_tier(row: dict) -> str:
    sector   = row.get("sector", "")
    industry = row.get("industry", "")
    name     = row.get("name", "").lower()
    desc     = row.get("description", "").lower()
    if industry in TIER1_INDUSTRIES:
        return "1"
    if (sector, industry) in TIER2_COMBOS:
        return "2"
    if not sector and not industry:
        if any(kw in name + " " + desc for kw in MINING_KEYWORDS):
            return "3"
        return "out"
    return "out"

def phase1_tier_assignment(rows: list) -> list:
    log.info("=" * 60)
    log.info("PHASE 1: Tier assignment")
    counts = {"1": 0, "2": 0, "3": 0, "out": 0}
    results = []
    for row in rows:
        tier = assign_tier(row)
        counts[tier] += 1
        r = {f: "" for f in RESULT_FIELDS}
        for k in ["symbol","name","exchange","sector","industry","market_cap","description"]:
            r[k] = row.get(k, "")
        r["tier"] = tier
        # Carry over any existing data (party numbers, filings, LLM results)
        for k in RESULT_FIELDS:
            if k not in r and row.get(k):
                r[k] = row[k]
        results.append(r)
    log.info(f"  Tier 1 (Mining industry):     {counts['1']:5d}")
    log.info(f"  Tier 2 (Adjacent sectors):    {counts['2']:5d}")
    log.info(f"  Tier 3 (Keyword match):       {counts['3']:5d}")
    log.info(f"  Out:                          {counts['out']:5d}")
    log.info(f"  Total for filing fetch:       {counts['1']+counts['2']+counts['3']:5d}")
    return results

# ---------------------------------------------------------------------------
# PHASE 2: FILING FETCH VIA STOCKWATCH /News/Sedar
# ---------------------------------------------------------------------------
def _ensure_cdp_ready(timeout: int = 30) -> bool:
    """Check if OpenClaw browser CDP is reachable; launch it if not."""
    import urllib.request, subprocess as _sp, time as _t
    def _alive():
        try:
            urllib.request.urlopen(f"{CDP_URL}/json", timeout=2)
            return True
        except Exception:
            return False
    if _alive():
        return True
    log.info("  CDP not reachable - launching OpenClaw browser...")
    try:
        _sp.Popen(["openclaw", "browser", "start"], shell=True,
                  stdout=_sp.DEVNULL, stderr=_sp.DEVNULL)
    except Exception as e:
        log.warning(f"  Failed to launch browser: {e}")
        return False
    deadline = _t.time() + timeout
    while _t.time() < deadline:
        _t.sleep(2)
        if _alive():
            log.info("  CDP ready")
            return True
    log.warning(f"  CDP not reachable after {timeout}s")
    return False

SESSION_PATH = SCRIPT_DIR / "2. Canadian Batch Run" / "stockwatch_session.json"

def _get_sw_cookies() -> dict:
    """
    Extract Stockwatch cookies from OpenClaw browser via CDP.
    Falls back to cached stockwatch_session.json if browser unavailable.
    """
    from playwright.sync_api import sync_playwright
    _ensure_cdp_ready()
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(CDP_URL)
            ctx = browser.contexts[0]
            cookies = {c["name"]: c["value"] for c in ctx.cookies()
                       if "stockwatch.com" in c.get("domain", "")}
            browser.close()
        if "XXX" in cookies:
            log.info(f"  Got {len(cookies)} Stockwatch cookies from browser")
            # Cache for reuse
            import json as _json
            SESSION_PATH.parent.mkdir(parents=True, exist_ok=True)
            SESSION_PATH.write_text(
                _json.dumps({"cookies": cookies, "saved_at": datetime.now().isoformat()}),
                encoding="utf-8")
            return cookies
        log.warning("  XXX auth cookie missing from browser - trying saved session")
    except Exception as e:
        log.warning(f"  Browser cookie extract failed: {e} - trying saved session")

    if SESSION_PATH.exists():
        try:
            import json as _json
            data = _json.loads(SESSION_PATH.read_text(encoding="utf-8"))
            cookies = data.get("cookies", {})
            if "XXX" in cookies:
                log.info(f"  Using saved Stockwatch session from {data.get('saved_at','?')}")
                return cookies
        except Exception:
            pass

    raise RuntimeError(
        "No valid Stockwatch session. Open the Stockwatch site in OpenClaw browser and retry."
    )

def _sw_session(cookies: dict) -> requests.Session:
    """Build a requests session with Stockwatch cookies and headers."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": SW_SEDAR_URL,
    })
    for name, value in cookies.items():
        s.cookies.set(name, value, domain="www.stockwatch.com")
    return s

def _sw_load_session(session: requests.Session) -> dict:
    """
    Fetch the Stockwatch SEDAR form, extract viewstate tokens, verify login.
    Returns dict with vs, vsg keys.
    """
    resp = session.get(SW_SEDAR_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    vs  = (soup.find("input", {"id": "__VIEWSTATE"})          or {}).get("value", "")
    vsg = (soup.find("input", {"id": "__VIEWSTATEGENERATOR"}) or {}).get("value", "")
    logged_in = "logged in" in resp.text.lower() or "imxgadeita" in resp.text
    log.info(f"  Stockwatch SEDAR form loaded (logged_in={logged_in})")
    return {"vs": vs, "vsg": vsg}

def _sw_search(session: requests.Session, vs: dict, symbol: str,
               date_from: date, date_to: date, doc_type: str = "") -> list:
    """Post to Stockwatch SEDAR search, return parsed filing rows."""
    data = [
        ("__EVENTTARGET", ""), ("__EVENTARGUMENT", ""),
        ("__VIEWSTATE", vs["vs"]),
        ("__VIEWSTATEGENERATOR", vs["vsg"]),
        ("ctl00$TextSymbol2", ""), ("ctl00$RadioRegion2", "RadioCanada2"),
        ("ctl00$setfocus", ""), ("ctl00$scrolly", ""),
        ("ctl00$redirectto", ""), ("ctl00$showcert", ""),
        ("ctl00$MainContent$tPublicFrom", date_from.strftime("%Y%m%d")),
        ("ctl00$MainContent$tPublicTo",   date_to.strftime("%Y%m%d")),
        ("ctl00$MainContent$tPublicName", ""),
        ("ctl00$MainContent$tPublicSymbol", symbol),
        ("ctl00$MainContent$tPublicKeywords", ""),
        ("ctl00$MainContent$dPublicDoctype", doc_type),
        ("ctl00$MainContent$dPublicSort", "filingdate"),
        ("ctl00$MainContent$bPublic.x", "0"),
        ("ctl00$MainContent$bPublic.y", "0"),
    ]
    resp = session.post(SW_SEDAR_URL, data=data, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    tbl  = soup.find("table", {"id": "MainContent_gSedar"})
    if not tbl:
        return []
    rows = []
    for tr in tbl.find_all("tr")[1:]:
        cells = tr.find_all("td")
        if len(cells) < 6:
            continue
        doc_type_cell = cells[4].get_text("\n", strip=True)
        dtype    = doc_type_cell.split("\n")[0].strip()
        synopsis = cells[6].get_text(strip=True) if len(cells) > 6 else ""
        # Skip French
        if re.search(r'\bfrench\b|\bfranÃ§ais\b|\b_fr\b', dtype + " " + synopsis, re.I):
            continue
        pdf_link = cells[5].find("a")
        pdf_url  = pdf_link["href"] if pdf_link else ""
        if pdf_url and not pdf_url.startswith("http"):
            pdf_url = f"{SW_BASE}{pdf_url}"
        rows.append({
            "filing_date": cells[3].get_text(strip=True),
            "doc_type":    dtype,
            "synopsis":    synopsis,
            "pdf_url":     pdf_url,
            "sw_symbol":   cells[1].get_text(strip=True) if len(cells) > 1 else "",
            "sw_issuer":   cells[2].get_text(strip=True) if len(cells) > 2 else "",
        })
    return rows

def _sw_search_by_name(session, vs, name, date_from, date_to):
    """
    Search Stockwatch by company name with fuzzy matching (level 5).
    Tries progressively shorter name variants until results found.
    Returns list of filing rows, each with sw_symbol/sw_issuer fields.
    """
    import re as _re
    from bs4 import BeautifulSoup as _BS
    LEGAL = _re.compile(r'\b(inc\.?|corp\.?|ltd\.?|limited|llc|lp|plc|co\.?)\b', _re.I)
    clean = LEGAL.sub('', name).strip()
    words = [w for w in clean.split() if len(w) > 1]
    if not words:
        return []
    # Try progressively shorter name terms
    search_terms = [" ".join(words[:i]) for i in range(len(words), 1, -1)]
    seen = []
    for term in search_terms:
        if term in seen:
            continue
        seen.append(term)
        data = [
            ("__EVENTTARGET", ""), ("__EVENTARGUMENT", ""),
            ("__VIEWSTATE", vs["vs"]),
            ("__VIEWSTATEGENERATOR", vs["vsg"]),
            ("ctl00$TextSymbol2", ""), ("ctl00$RadioRegion2", "RadioCanada2"),
            ("ctl00$setfocus", ""), ("ctl00$scrolly", ""),
            ("ctl00$redirectto", ""), ("ctl00$showcert", ""),
            ("ctl00$MainContent$tPublicFrom", date_from.strftime("%Y%m%d")),
            ("ctl00$MainContent$tPublicTo",   date_to.strftime("%Y%m%d")),
            ("ctl00$MainContent$tPublicName", term),
            ("ctl00$MainContent$tPublicSymbol", ""),
            ("ctl00$MainContent$tPublicKeywords", ""),
            ("ctl00$MainContent$dPublicDoctype", ""),
            ("ctl00$MainContent$dPublicSort", "filingdate"),
            ("ctl00$MainContent$dPublicStemming", "Yes"),
            ("ctl00$MainContent$dPublicSearchtype", "Natural language"),
            ("ctl00$MainContent$dPublicFuzzy", "5"),
            ("ctl00$MainContent$dPublicPhonic", "No"),
            ("ctl00$MainContent$bPublic.x", "0"),
            ("ctl00$MainContent$bPublic.y", "0"),
        ]
        try:
            resp = session.post(SW_SEDAR_URL, data=data, timeout=60)
            resp.raise_for_status()
        except Exception:
            continue
        soup = _BS(resp.text, "html.parser")
        tbl  = soup.find("table", {"id": "MainContent_gSedar"})
        if not tbl:
            continue
        rows = []
        for tr in tbl.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if len(cells) < 6:
                continue
            dtype_raw = cells[4].get_text("\n", strip=True)
            dtype    = dtype_raw.split("\n")[0].strip()
            synopsis = cells[6].get_text(strip=True) if len(cells) > 6 else ""
            if re.search(r'\bfrench\b|\bfran.ais\b|\b_fr\b', dtype + " " + synopsis, re.I):
                continue
            pdf_link = cells[5].find("a")
            pdf_url  = pdf_link["href"] if pdf_link else ""
            if pdf_url and not pdf_url.startswith("http"):
                pdf_url = f"{SW_BASE}{pdf_url}"
            rows.append({
                "filing_date": cells[3].get_text(strip=True),
                "doc_type":    dtype,
                "synopsis":    synopsis,
                "pdf_url":     pdf_url,
                "sw_symbol":   cells[1].get_text(strip=True) if len(cells) > 1 else "",
                "sw_issuer":   cells[2].get_text(strip=True) if len(cells) > 2 else "",
            })
        if rows:
            return rows  # found with this term
    return []


def _filing_priority(doc_type: str, synopsis: str) -> int:
    """Return priority score for a filing row (lower = better). -1 = skip."""
    dt = doc_type.upper().strip()

    # Step 1: exact match against known English doc_type strings (highest confidence)
    for i, (exact_set, _) in enumerate(FILING_DOC_TYPES):
        if dt in exact_set:
            return i

    # Step 2: French variants of the exact strings â†’ always skip
    FRENCH_EXACT = {
        "ANNUAL_INFORMATION_FORM_FR", "INTERIM_MDA_FR",
        "MDA_FR", "MDA_AMENDED_FR", "ANNUAL_REPORT_FR",
    }
    if dt in FRENCH_EXACT:
        return -1

    # Step 3: Skip ancillary docs by pattern
    SKIP_PATTERNS = [
        r'52109',             # CEO/CFO certifications
        r'CERTIFICATION',
        r'CONSENT',
        r'CERTIFICATE',
        r'QUALIFICATION',
        r'FINANCIAL_STATEMENT',
        r'INTERIM_FINANCIAL',
        r'DISSEMINATION',
        r'AUDITOR',
        r'LEGAL_COUNSEL',
        r'_FR$',              # anything else ending in _FR
        r'FRENCH',
    ]
    for pat in SKIP_PATTERNS:
        if re.search(pat, dt):
            return -1

    # Step 4: keyword fallback for older filings with non-standard doc_type strings
    # e.g. "Interim MD&A - English", "Annual information form - English"
    combined = (doc_type + " " + synopsis).lower()
    if re.search(r'french|franÃ§ais', combined):
        return -1
    for i, (kw, _) in enumerate(FILING_TYPE_PRIORITY):
        if kw in combined:
            return i

    return 99  # not a useful filing type

def _strip_toc(text: str) -> str:
    """
    Remove preamble sections (TOC, glossary, definitions) from extracted PDF text
    by finding the first real business-narrative paragraph.

    Skips:
    - Table of contents lines (text....page_number format)
    - Glossary/definitions sections (short term-definition pairs, "X means Y" patterns)
    - Section headers that are known preamble (CAUTIONARY, GLOSSARY, DEFINITIONS, etc.)

    Returns text starting from the first substantive narrative paragraph.
    """
    TOC_LINE      = re.compile(r'\.{3,}\s*\w*\s*\d+\s*$|^\s*[ivxIVX]+\s*$|^\s*\d+\s*$')
    PREAMBLE_HDR  = re.compile(
        r'^\s*(CAUTIONARY|GLOSSARY|DEFINITIONS?\s+AND|ABBREVIATIONS?|NOTE\s+TO|'
        r'FORWARD.LOOKING|NON.GAAP|CURRENCY\s+PRESENTATION|'
        r'TABLE\s+OF\s+CONTENTS?|TECHNICAL\s+AND\s+THIRD.PARTY)\b',
        re.IGNORECASE)
    # Glossary pattern: short line (term) followed by "means" definition
    GLOSSARY_LINE = re.compile(r'\bmeans\b|\brefers?\s+to\b', re.IGNORECASE)
    REAL_PARA     = re.compile(r'[a-z]{3,}.*[a-z]{3,}')
    SENTENCE      = re.compile(r'[,;:.!?]')

    lines = text.split('\n')
    consecutive_real = 0
    start_idx = 0
    in_preamble = True

    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            if not in_preamble:
                consecutive_real = max(0, consecutive_real - 1)
            continue

        # Skip known preamble section headers
        if PREAMBLE_HDR.match(stripped):
            consecutive_real = 0
            in_preamble = True
            continue

        # Skip TOC lines
        if TOC_LINE.search(stripped):
            consecutive_real = 0
            in_preamble = True
            continue

        # Skip glossary/definition lines (short + "means/refers to")
        if len(stripped) < 200 and GLOSSARY_LINE.search(stripped):
            consecutive_real = 0
            in_preamble = True
            continue

        # Check if this looks like real business-narrative text
        if (len(stripped) > 60
                and REAL_PARA.search(stripped)
                and SENTENCE.search(stripped)
                and not GLOSSARY_LINE.search(stripped)):
            if consecutive_real == 0:
                start_idx = i
            consecutive_real += 1
            in_preamble = False
            if consecutive_real >= 2:
                return '\n'.join(lines[start_idx:])
        else:
            consecutive_real = 0

    return text  # no clear preamble found, return as-is

def _extract_pdf_text(pdf_bytes: bytes, max_chars: int = TEXT_CHARS) -> str:
    """
    Extract text from PDF bytes using fitz.
    Strips table of contents (dot-leader lines ending in page numbers)
    so the LLM gets actual business content, not TOC boilerplate.
    Returns up to max_chars.
    """
    try:
        import fitz
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            text = ""
            for page in doc:
                text += page.get_text()
                if len(text) >= max_chars * 3:  # over-extract then strip TOC
                    break
        text = text.encode("ascii", "ignore").decode("ascii")
        text = _strip_toc(text)
        return text[:max_chars]
    except Exception:
        return ""

def fetch_filing_for_company(symbol: str, name: str, session: requests.Session,
                              vs: dict, date_from: date, date_to: date) -> dict:
    """
    Search Stockwatch for the most recent useful filing (AIF/MD&A/Annual Report).
    Tries symbol search first, falls back to company name search if nothing found.
    Returns dict with filing_type, filing_date, filing_doc_name, filing_url,
    filing_text_chars, _filing_text. Empty dict if nothing found.
    """
    try:
        rows = _sw_search(session, vs, symbol, date_from, date_to)
        if not rows:
            # Fallback: search by company name instead of symbol
            rows = _sw_search_by_name(session, vs, name, date_from, date_to)
            if rows:
                log.info(f"  {symbol}: found via name search ({name[:40]})")
        if not rows:
            log.info(f"  {symbol}: no Stockwatch filings found")
            return {}

        # Find best filing by priority
        best = None
        best_priority = 999
        for row in rows:
            p = _filing_priority(row["doc_type"], row["synopsis"])
            if p == -1:
                continue
            if p < best_priority:
                best_priority = p
                best = row

        if not best:
            log.info(f"  {symbol}: no suitable filing found in {len(rows)} rows")
            return {}

        label = FILING_TYPE_PRIORITY[best_priority][1] if best_priority < len(FILING_TYPE_PRIORITY) else "Filing"
        sw_sym = best.get("sw_symbol", "")
        if sw_sym and sw_sym.upper() != symbol.upper():
            log.info(f"  {symbol}: found as {sw_sym} on Stockwatch | {label} | {best['filing_date']}")
        else:
            log.info(f"  {symbol}: {label} | {best['filing_date']} | {best['doc_type'][:40]}")

        # Download PDF
        if not best["pdf_url"]:
            return {}
        r = session.get(best["pdf_url"], timeout=60)
        if r.status_code != 200 or r.content[:4] != b"%PDF":
            log.info(f"  {symbol}: PDF fetch failed (status {r.status_code})")
            return {}

        text = _extract_pdf_text(r.content)
        if not text.strip():
            log.info(f"  {symbol}: PDF extracted but no text")
            return {}

        # Save PDF into TICKER_NAME subfolder
        safe_sym  = re.sub(r"[^\w]", "_", symbol)
        safe_name = re.sub(r"[^\w\s-]", "", name).strip()
        safe_name = re.sub(r"[\s/]+", "_", safe_name)[:40]
        company_dir = PDFS_DIR / f"{safe_sym}_{safe_name}"
        company_dir.mkdir(parents=True, exist_ok=True)
        date_iso = ""
        try:
            d = datetime.strptime(best["filing_date"].strip(), "%Y-%m-%d")
            date_iso = d.strftime("%Y-%m-%d")
        except Exception:
            date_iso = best["filing_date"].replace(" ", "-")
        pdf_filename = f"{date_iso}_{safe_sym}_{label.replace(' ', '_')}.pdf"
        pdf_path = company_dir / pdf_filename
        pdf_path.write_bytes(r.content)

        return {
            "filing_type":      label,
            "filing_date":      best["filing_date"],
            "filing_doc_name":  best["doc_type"],
            "filing_url":       best["pdf_url"],
            "filing_pdf_path":  str(pdf_path),
            "filing_text_chars": len(text),
            "sw_symbol":        best.get("sw_symbol", ""),
            "_filing_text":     text,
        }

    except Exception as e:
        log.warning(f"  {symbol}: filing fetch error: {type(e).__name__}: {e}")
        return {}

def phase2_fetch_filings(results: list, limit: int = 0, symbol_filter: str = "") -> list:
    log.info("=" * 60)
    log.info("PHASE 2: Stockwatch filing fetch")

    candidates = [r for r in results
                  if r.get("tier") in ("1","2","3")
                  and not r.get("filing_type")]  # skip already fetched
    if symbol_filter:
        candidates = [r for r in candidates if r.get("symbol","").upper() == symbol_filter.upper()]
    if limit:
        candidates = candidates[:limit]
    log.info(f"  {len(candidates)} companies to fetch ({FILING_WORKERS} workers)")

    if not candidates:
        return results

    # Get Stockwatch session once - share cookies across workers (read-only, thread-safe)
    cookies = _get_sw_cookies()
    date_from = date.today() - timedelta(days=730)  # 2 years back
    date_to   = date.today() - timedelta(days=14)   # /News/Sedar has 14-day lag

    def _worker(row: dict) -> dict:
        session = _sw_session(cookies)  # each thread gets its own session object
        vs      = _sw_load_session(session)
        time.sleep(0.2)  # gentle stagger
        filing  = fetch_filing_for_company(
            row["symbol"], row["name"], session, vs, date_from, date_to)
        if filing:
            for k, v in filing.items():
                if not k.startswith("_"):
                    row[k] = v
            row["_filing_text"]    = filing.get("_filing_text", "")
            row["filing_pdf_path"] = filing.get("filing_pdf_path", "")
            row["sw_symbol"]       = filing.get("sw_symbol", "")

            # If primary filing is an AIF, also fetch most recent MD&A as cross-check
            if filing.get("filing_type") == "AIF":
                # Temporarily override priority to prefer MD&A types only
                mda_types = {"INTERIM_MDA_EN", "MDA_EN", "MDA_AMENDED_EN"}
                mda_rows = _sw_search(session, vs, row["symbol"], date_from, date_to)
                best_mda = None
                best_p = 999
                for r in mda_rows:
                    dt = r["doc_type"].upper().strip()
                    if dt in mda_types:
                        p = list(mda_types).index(dt) if dt in list(mda_types) else 99
                        # Priority: INTERIM_MDA > MDA_AMENDED > MDA
                        type_priority = {"INTERIM_MDA_EN": 0, "MDA_EN": 1, "MDA_AMENDED_EN": 1}
                        p = type_priority.get(dt, 99)
                        if p < best_p:
                            best_p = p
                            best_mda = r
                if best_mda and best_mda.get("pdf_url"):
                    try:
                        r2 = session.get(best_mda["pdf_url"], timeout=60)
                        if r2.status_code == 200 and r2.content[:4] == b"%PDF":
                            mda_text = _extract_pdf_text(r2.content)
                            mda_label = "Interim MD&A" if "INTERIM" in best_mda["doc_type"] else "Annual MD&A"
                            safe_sym = re.sub(r"[^\w]", "_", row["symbol"])
                            safe_name = re.sub(r"[^\w\s-]", "", row["name"]).strip()
                            safe_name = re.sub(r"[\s/]+", "_", safe_name)[:40]
                            co_dir = PDFS_DIR / f"{safe_sym}_{safe_name}"
                            co_dir.mkdir(parents=True, exist_ok=True)
                            mda_filename = f"{best_mda['filing_date']}_{safe_sym}_{mda_label.replace(' ','_')}.pdf"
                            mda_path = co_dir / mda_filename
                            mda_path.write_bytes(r2.content)
                            row["mda_type"]      = mda_label
                            row["mda_date"]      = best_mda["filing_date"]
                            row["mda_doc_name"]  = best_mda["doc_type"]
                            row["mda_url"]       = best_mda["pdf_url"]
                            row["mda_pdf_path"]  = str(mda_path)
                            row["mda_text_chars"] = len(mda_text)
                            row["_mda_text"]     = mda_text
                            log.info(f"  {row['symbol']}: MD&A cross-check: {mda_label} | {best_mda['filing_date']}")
                    except Exception as e:
                        log.debug(f"  {row['symbol']}: MD&A fetch error: {e}")
        return row

    done = 0
    with ThreadPoolExecutor(max_workers=FILING_WORKERS) as pool:
        futures = {pool.submit(_worker, row): row for row in candidates}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                log.warning(f"  Worker error: {e}")
            done += 1
            if done % 50 == 0 or done == len(candidates):
                log.info(f"  Phase 2: {done}/{len(candidates)} done")
                save_results(results)

    save_results(results)
    fetched = sum(1 for r in candidates if r.get("filing_type"))
    log.info(f"Phase 2 complete: {fetched}/{len(candidates)} fetched")
    return results

# ---------------------------------------------------------------------------
# PHASE 3: LLM CLASSIFICATION
# ---------------------------------------------------------------------------
def load_prompt() -> str:
    if PROMPT_PATH.exists():
        return PROMPT_PATH.read_text(encoding="utf-8")
    raise FileNotFoundError(f"prompt.txt not found at {PROMPT_PATH}")

def classify_company(row: dict, prompt_template: str) -> dict:
    """Classify one company via OpenAI. Returns updated row."""
    from openai import OpenAI
    client = OpenAI()
    filing_text = row.get("_filing_text", "") or ""
    # Escape braces in prompt template that aren't our placeholders
    # (the JSON example in the prompt uses { } which conflict with str.format)
    safe_template = prompt_template.replace("{", "{{").replace("}", "}}")
    # Re-open our actual placeholders
    for field in ["symbol", "name", "exchange", "sector", "industry", "market_cap", "filing_text"]:
        safe_template = safe_template.replace("{{" + field + "}}", "{" + field + "}")
    prompt = safe_template.format(
        symbol      = row.get("symbol", ""),
        name        = row.get("name", ""),
        exchange    = row.get("exchange", ""),
        sector      = row.get("sector", ""),
        industry    = row.get("industry", ""),
        market_cap  = row.get("market_cap", ""),
        filing_text = filing_text[:TEXT_CHARS],
    )
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=300,
            response_format={"type": "json_object"},
        )
        raw = (resp.choices[0].message.content or "{}").strip()
        # Strip markdown code fences if present
        raw = re.sub(r'^```(?:json)?\s*', '', raw, flags=re.MULTILINE)
        raw = re.sub(r'\s*```$', '', raw, flags=re.MULTILINE)
        result = json.loads(raw)
        row["company_type"]             = result.get("company_type", "Uncertain")
        row["company_type_confidence"]  = result.get("company_type_confidence", 0)
        row["primary_commodity"]        = result.get("primary_commodity", "")
        row["primary_commodity_confidence"] = result.get("primary_commodity_confidence", 0)
        row["adjacent_category"]        = result.get("adjacent_category", "")
        row["reasoning"]                = result.get("reasoning", "")
        row["in_universe"]              = "Y" if row["company_type"] in IN_UNIVERSE_TYPES else "N"
    except Exception as e:
        row["company_type"]            = "Uncertain"
        row["company_type_confidence"] = 0
        row["reasoning"]               = f"LLM error: {e}"
        row["in_universe"]             = "N"
        log.warning(f"  LLM error for {row.get('symbol')}: {type(e).__name__}: {e}")
    return row

def phase3_llm_classify(results: list, limit: int = 0, symbol_filter: str = "") -> list:
    log.info("=" * 60)
    log.info("PHASE 3: LLM classification")
    prompt = load_prompt()
    candidates = [r for r in results
                  if r.get("tier") in ("1","2","3")
                  and r.get("filing_type")          # must have a filing
                  and not r.get("company_type")]     # not yet classified
    if symbol_filter:
        candidates = [r for r in candidates if r.get("symbol","").upper() == symbol_filter.upper()]
    if limit:
        candidates = candidates[:limit]
    log.info(f"  {len(candidates)} companies to classify ({LLM_WORKERS} workers)")

    if not candidates:
        return results

    def _reload_mda_text(row: dict) -> str:
        """Re-extract MD&A text from saved PDF if not in memory."""
        if row.get("_mda_text"):
            return row["_mda_text"]
        explicit = row.get("mda_pdf_path", "")
        if explicit:
            p = Path(explicit)
            if p.exists():
                text = _extract_pdf_text(p.read_bytes())
                row["_mda_text"] = text
                return text
        return ""

    # Re-extract filing text from PDFs for rows that don't have it in memory
    # (text is not saved to CSV - must re-read from the saved PDF)
    def _reload_text(row: dict) -> str:
        if row.get("_filing_text"):
            return row["_filing_text"]
        sym = row.get("symbol", "")
        # Prefer the explicit path stored in the CSV
        explicit = row.get("filing_pdf_path", "")
        if explicit:
            p = Path(explicit)
            if p.exists():
                text = _extract_pdf_text(p.read_bytes())
                row["_filing_text"] = text
                return text
        # Fallback: search subdirectories for any PDF matching this symbol
        safe_sym = re.sub(r"[^\w]", "_", sym)
        matches = list(PDFS_DIR.rglob(f"*_{safe_sym}_*.pdf"))
        if matches:
            text = _extract_pdf_text(matches[0].read_bytes())
            row["_filing_text"] = text
            return text
        log.debug(f"  {sym}: no PDF found for text re-extraction")
        return ""

    for row in candidates:
        _reload_text(row)
        _reload_mda_text(row)

    done = total_in = discrepancies = 0
    with ThreadPoolExecutor(max_workers=LLM_WORKERS) as pool:
        futures = {pool.submit(classify_company, row, prompt): row for row in candidates}
        for future in as_completed(futures):
            try:
                updated = future.result()
                if updated.get("in_universe") == "Y":
                    total_in += 1
            except Exception as e:
                import traceback
                log.warning(f"  LLM future error: {type(e).__name__}: {e}\n{traceback.format_exc()}")
            done += 1
            if done % 100 == 0 or done == len(candidates):
                log.info(f"  LLM: {done}/{len(candidates)} | {total_in} in universe")
                save_results(results)

    # MD&A cross-check: classify using MD&A text, store separately, flag discrepancies
    mda_candidates = [r for r in candidates
                      if r.get("mda_type") and r.get("_mda_text") and not r.get("mda_company_type")]
    if mda_candidates:
        log.info(f"  MD&A cross-check: classifying {len(mda_candidates)} companies")

        def _classify_with_mda(row: dict) -> dict:
            """Classify using MD&A text, return dict with mda_ fields only (no row mutation)."""
            import copy as _copy
            proxy = _copy.copy(row)
            proxy["_filing_text"] = row.get("_mda_text", "")
            classify_company(proxy, prompt)
            return {
                "symbol":              row["symbol"],
                "mda_company_type":    proxy.get("company_type", ""),
                "mda_primary_commodity": proxy.get("primary_commodity", ""),
                "mda_reasoning":       proxy.get("reasoning", ""),
            }

        row_by_sym = {r["symbol"]: r for r in mda_candidates}
        with ThreadPoolExecutor(max_workers=LLM_WORKERS) as pool:
            futures = {pool.submit(_classify_with_mda, row): row for row in mda_candidates}
            for future in as_completed(futures):
                try:
                    res = future.result()
                    sym = res["symbol"]
                    r   = row_by_sym[sym]
                    r["mda_company_type"]      = res["mda_company_type"]
                    r["mda_primary_commodity"] = res["mda_primary_commodity"]
                    r["mda_reasoning"]         = res["mda_reasoning"]
                    # Flag discrepancy if classifications differ
                    aif_t = r.get("company_type", "")
                    mda_t = res["mda_company_type"]
                    if aif_t and mda_t and aif_t != mda_t:
                        r["discrepancy"] = f"AIF={aif_t} | MDA={mda_t}"
                        discrepancies += 1
                        log.info(f"  DISCREPANCY {sym}: AIF={aif_t} vs MD&A={mda_t}")
                    else:
                        r["discrepancy"] = ""
                except Exception as e:
                    log.warning(f"  MD&A cross-check error: {e}")

    log.info(f"  Cross-check complete: {discrepancies} discrepancies flagged")

    save_results(results)
    log.info(f"Phase 3 complete: {total_in} in universe")
    return results

# ---------------------------------------------------------------------------
# PHASE 4: SEDAR+ PARTY NUMBER LOOKUP (Playwright, single-threaded)
# Runs on Tier 1 first, then other relevant companies.
# Can run concurrently with Phase 3 via background thread.
# ---------------------------------------------------------------------------
def _sedar_lookup_party(page, symbol: str, name: str) -> tuple[str, str]:
    """
    Look up SEDAR+ party number for a company by name.
    Returns (party_number, party_name) or ("", "") if not found.
    Uses the same autocomplete approach proven in the onboarding script.
    """
    LEGAL = re.compile(
        r'\b(inc\.?|corp\.?|ltd\.?|limited|llc|lp|plc|co\.?|incorporated|corporation|s\.a\.?|n\.v\.?)\b',
        re.IGNORECASE)

    def _strip_legal(s):
        return re.sub(r'\s+', ' ', LEGAL.sub(' ', s)).strip()

    try:
        page.goto(
            f"{SEDAR_BASE}/csa-party/service/create.html"
            "?targetAppCode=csa-party&service=searchDocuments&_locale=en",
            wait_until="domcontentloaded", timeout=20000)
        page.wait_for_selector("#SubmissionDate", timeout=15000)
    except Exception as e:
        log.debug(f"  SEDAR {symbol}: page load failed: {e}")
        return "", ""

    clean = _strip_legal(name)
    words = clean.split()
    search_terms = [name, clean] + [" ".join(words[:i]) for i in range(len(words)-1, 0, -1)]
    search_terms = list(dict.fromkeys(search_terms))  # deduplicate preserving order

    pn = page.locator('input[placeholder="Profile name or number"]')
    name_lower = name.lower()
    name_words = [w for w in re.sub(r'[^\w\s]', ' ', name_lower).split() if len(w) > 2]

    for term in search_terms:
        if not term or len(term) < 3:
            continue
        try:
            pn.fill("")
            pn.click()
            page.wait_for_timeout(100)
            pn.type(term, delay=40)
            page.wait_for_timeout(1600)

            items = page.locator("ul.ui-autocomplete li.ui-menu-item")
            count = items.count()
            if count == 0:
                continue

            for i in range(min(count, 8)):
                text = items.nth(i).inner_text()
                m = re.search(r'\((\d{9})\)', text)
                if not m:
                    continue
                text_lower = text.lower()
                matches = sum(1 for w in name_words
                              if re.search(r'\b' + re.escape(w) + r'\b', text_lower))
                if matches >= max(1, len(term.split()) // 2):
                    party_number = m.group(1)
                    party_name   = re.sub(r'\s*\(\d{9}\).*$', '', text).strip()
                    log.info(f"  SEDAR {symbol}: found {party_number} -> {party_name[:50]}")
                    return party_number, party_name
        except Exception as e:
            log.debug(f"  SEDAR {symbol}: search error for '{term}': {e}")
            continue

    log.info(f"  SEDAR {symbol}: no match found for '{name[:40]}'")
    return "", ""

def phase4_sedar_parties(results: list, limit: int = 0, symbol_filter: str = "") -> list:
    """
    Look up SEDAR+ party numbers for relevant companies.
    Tier 1 processed first, then other IN_UNIVERSE companies.
    Skips companies that already have a party number.
    """
    log.info("=" * 60)
    log.info("PHASE 4: SEDAR+ party number lookup")

    # Order: Tier 1 first, then others
    tier1 = [r for r in results if r.get("tier") == "1"
              and not r.get("sedar_party_number")
              and r.get("in_universe") != "N"]
    others = [r for r in results if r.get("tier") != "1"
               and not r.get("sedar_party_number")
               and r.get("in_universe") == "Y"]
    candidates = tier1 + others

    if symbol_filter:
        candidates = [r for r in candidates if r.get("symbol","").upper() == symbol_filter.upper()]
    if limit:
        candidates = candidates[:limit]
    log.info(f"  {len(candidates)} companies ({len(tier1)} Tier 1 first, then {len(others)} others)")

    if not candidates:
        log.info("  Nothing to do")
        return results

    if not _ensure_cdp_ready():
        log.error("  CDP not available - cannot run Phase 4")
        return results

    # Check SEDAR+ is accessible
    try:
        r = requests.get(f"{SEDAR_BASE}/csa-party/", timeout=10)
        page_text = r.text.lower()
        if "maintenance" in page_text or "unavailable" in page_text:
            log.error("  SEDAR+ appears unavailable (maintenance?)")
            return results
        log.info("  SEDAR+ accessible")
    except Exception as e:
        log.error(f"  SEDAR+ check failed: {e}")
        return results

    from playwright.sync_api import sync_playwright
    found = 0
    with sync_playwright() as pw:
        browser = pw.chromium.connect_over_cdp(CDP_URL)
        ctx     = browser.contexts[0]
        page    = ctx.new_page()
        try:
            for i, row in enumerate(candidates, 1):
                sym  = row.get("symbol", "")
                name = row.get("name", "")
                pnum, pname = _sedar_lookup_party(page, sym, name)
                if pnum:
                    row["sedar_party_number"] = pnum
                    row["sedar_party_name"]   = pname
                    found += 1
                if i % 20 == 0 or i == len(candidates):
                    log.info(f"  Phase 4: {i}/{len(candidates)} | {found} found")
                    save_results(results)
        finally:
            try: page.close()
            except Exception: pass
            try: browser.close()
            except Exception: pass

    save_results(results)
    log.info(f"Phase 4 complete: {found}/{len(candidates)} party numbers found")
    return results

# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------
PARTIES_CSV    = SCRIPT_DIR / "Universe Builder" / "sedar_parties.csv"
NO_FILING_CSV  = SCRIPT_DIR / "Universe Builder" / "no_filing_found.csv"

def load_results() -> list:
    if not RESULTS_CSV.exists():
        return []
    with open(RESULTS_CSV, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    # Merge in any party numbers from sedar_parties.csv (written by find_sedar_parties.py)
    if PARTIES_CSV.exists():
        with open(PARTIES_CSV, newline="", encoding="utf-8-sig") as f:
            parties = {r["symbol"]: r for r in csv.DictReader(f)
                       if r.get("sedar_party_number") and r["sedar_party_number"] != "NOT_FOUND"}
        for row in rows:
            sym = row.get("symbol", "")
            if sym in parties and not row.get("sedar_party_number"):
                row["sedar_party_number"] = parties[sym]["sedar_party_number"]
                row["sedar_party_name"]   = parties[sym].get("sedar_party_name", "")
    return rows

def save_results(rows: list):
    with open(RESULTS_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=RESULT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

# ---------------------------------------------------------------------------
# PHASE 5: SEDAR+ DIRECT FILING FETCH (for companies with no Stockwatch match)
# Uses Playwright to search SEDAR+ by party number and download AIF/MD&A
# ---------------------------------------------------------------------------
def phase5_sedar_direct_fetch(no_filing_csv: str, results: list) -> list:
    """
    For companies in no_filing_csv that have a SEDAR party number,
    fetch their most recent AIF or MD&A directly from SEDAR+ via Playwright.
    Marks them with sedar_only=Y.
    """
    log.info("=" * 60)
    log.info("PHASE 5: SEDAR+ direct filing fetch for no-Stockwatch companies")

    # Load the no-filing list
    no_filing_syms = set()
    try:
        with open(no_filing_csv, newline="", encoding="utf-8-sig") as f:
            no_filing_syms = {r["symbol"] for r in csv.DictReader(f)}
    except Exception as e:
        log.error(f"  Cannot read {no_filing_csv}: {e}")
        return results

    # Load party numbers from sedar_parties.csv
    party_map = {}
    if PARTIES_CSV.exists():
        with open(PARTIES_CSV, newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                if r.get("sedar_party_number") and r["sedar_party_number"] != "NOT_FOUND":
                    party_map[r["symbol"]] = r["sedar_party_number"]

    # Also check universe_results for party numbers
    for r in results:
        sym = r.get("symbol","")
        if sym and r.get("sedar_party_number") and not party_map.get(sym):
            party_map[sym] = r["sedar_party_number"]

    results_map = {r["symbol"]: r for r in results}

    candidates = [sym for sym in no_filing_syms
                  if sym in results_map
                  and not results_map[sym].get("filing_type")]

    log.info(f"  {len(candidates)} companies to process")
    have_party   = [s for s in candidates if party_map.get(s)]
    no_party     = [s for s in candidates if not party_map.get(s)]
    log.info(f"  {len(have_party)} have party numbers | {len(no_party)} need lookup first")

    if not candidates:
        return results

    if not _ensure_cdp_ready():
        log.error("  CDP not available")
        return results

    # Check SEDAR+ accessible
    try:
        r = requests.get(f"{SEDAR_BASE}/csa-party/", timeout=10)
        if "maintenance" in r.text.lower():
            log.error("  SEDAR+ under maintenance")
            return results
        log.info("  SEDAR+ accessible")
    except Exception as e:
        log.error(f"  SEDAR+ check failed: {e}")
        return results

    from playwright.sync_api import sync_playwright

    FILING_PRIORITY_SEDAR = [
        ("ANNUAL_INFORMATION_FORMS",  "AIF"),
        ("INTERIM_MDA",               "Interim MD&A"),
        ("ANNUAL_MDA",                "Annual MD&A"),
        ("ANNUAL_REPORTS",            "Annual Report"),
    ]

    def _fetch_sedar_filing(page, party_number: str, symbol: str) -> dict:
        """
        Fetch the most recent AIF and/or MD&A for a company from SEDAR+.
        Uses the exact same Playwright pattern as mm_onboarding.py sedar_gap_fill.
        Navigates fresh for each company to avoid stale state.
        """
        FILING_TYPES_WANTED = [
            ("ANNUAL_INFORMATION_FORMS", "AIF"),
            ("INTERIM_MDA",              "Interim MD&A"),
            ("ANNUAL_MDA",               "Annual MD&A"),
        ]
        aif_result = None
        mda_result = None

        for sedar_type, label in FILING_TYPES_WANTED:
            # Skip if we already have what we need
            if sedar_type == "ANNUAL_INFORMATION_FORMS" and aif_result:
                continue
            if sedar_type in ("INTERIM_MDA","ANNUAL_MDA") and mda_result:
                continue

            try:
                # Navigate FRESH for every search - critical to avoid stale chip state
                page.goto(
                    f"{SEDAR_BASE}/csa-party/service/create.html"
                    "?targetAppCode=csa-party&service=searchDocuments&_locale=en",
                    wait_until="domcontentloaded", timeout=30000)
                page.wait_for_selector("#SubmissionDate", timeout=20000)

                # Apply chip by party number (exact onboarding pattern)
                pn = page.locator('input[placeholder="Profile name or number"]')
                pn.click(); page.wait_for_timeout(200)
                pn.type(party_number, delay=40); page.wait_for_timeout(1600)

                triggered = page.evaluate("""() => {
                    const input = document.querySelector('input[placeholder="Profile name or number"]');
                    const items = document.querySelectorAll('ul.ui-autocomplete li.ui-menu-item');
                    if (!items.length || !window.jQuery) return false;
                    const ac = jQuery(input).data('ui-autocomplete') || jQuery(input).data('autocomplete');
                    if (!ac) return false;
                    const itemData = jQuery(items[0]).data('ui-autocomplete-item');
                    if (!itemData) return false;
                    ac._trigger('select', null, {item: itemData});
                    return true;
                }""")
                if not triggered:
                    log.debug(f"  {symbol}: chip trigger failed for {sedar_type}")
                    continue
                page.wait_for_selector("#SubmissionDate", timeout=10000)
                page.wait_for_timeout(500)



                # Select FilingType and Search
                page.evaluate(f"""() => {{
                    window._done = false;
                    const orig = XMLHttpRequest.prototype.open;
                    XMLHttpRequest.prototype.open = function(m, url) {{
                        this._url = url;
                        this.addEventListener('load', ()=>{{ if(this._url?.includes('update.html')) window._done=true; }});
                        orig.apply(this, arguments);
                    }};
                    const ft = document.getElementById('FilingType');
                    if (ft) {{
                        Array.from(ft.options).forEach(o=>o.selected=false);
                        const m = Array.from(ft.options).find(o=>o.value==='{sedar_type}');
                        if (m) {{ m.selected=true; ft.dispatchEvent(new Event('change',{{bubbles:true}})); }}
                    }}
                }}""")
                try: page.wait_for_function("()=>window._done", timeout=8000)
                except: pass
                page.wait_for_timeout(300)

                page.evaluate("""() => {
                    document.getElementById('SubmissionDate').value  = '01/01/2020';
                    document.getElementById('SubmissionDate2').value = '31/12/2026';
                    window._done = false;
                    const orig2 = XMLHttpRequest.prototype.open;
                    XMLHttpRequest.prototype.open = function(m, url) {
                        this._url = url;
                        this.addEventListener('load', ()=>{ if(this._url?.includes('update.html')) window._done=true; });
                        orig2.apply(this, arguments);
                    };
                    Array.from(document.querySelectorAll('button')).find(b=>b.textContent.trim()==='Search')?.click();
                }""")
                try: page.wait_for_function("()=>window._done", timeout=10000)
                except: pass
                page.wait_for_timeout(300)

                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                tbl  = soup.find("table", attrs={"aria-label": "List of data items"})
                if not tbl:
                    log.info(f"  {symbol}: no {label} found on SEDAR+")
                    continue

                tbl_rows = (tbl.find("tbody") or tbl).find_all("tr")
                if not tbl_rows:
                    continue

                # Prefer English row
                chosen = None
                for tr in tbl_rows:
                    cells = tr.find_all("td")
                    if len(cells) > 2 and "english" in cells[2].get_text(" ",strip=True).lower():
                        chosen = tr; break
                if not chosen:
                    chosen = tbl_rows[0]

                cells    = chosen.find_all("td")
                doc_link = cells[2].find("a", href=re.compile(r"resource\.html")) if len(cells)>2 else None
                if not doc_link:
                    continue

                doc_name = doc_link.get_text(strip=True)
                href     = doc_link["href"]
                if href.startswith("/"): href = f"{SEDAR_BASE}{href}"
                date_m   = re.search(r"(\d{1,2}\s+\w{3}\s+\d{4})", cells[3].get_text(" ",strip=True) if len(cells)>3 else "")
                filing_date = date_m.group(1) if date_m else ""

                # Attach current session ID (critical - session-scoped URLs)
                view_id_m = re.search(r"id=([a-f0-9]+)", page.url)
                if view_id_m:
                    href = re.sub(r"[&?]id=[^&]*","",href) + f"&id={view_id_m.group(1)}"

                log.info(f"  {symbol}: SEDAR+ {label} | {filing_date} | {doc_name[:50]}")

                # Download PDF via browser page.expect_download()
                # SEDAR+ serves PDFs as Content-Disposition:attachment - requests and JS fetch
                # both fail (get HTML redirect or timeout). Must intercept the browser download.
                pdf_bytes = None
                safe_sym = re.sub(r"[^\w]","_",symbol)
                co_dir   = PDFS_DIR / f"{safe_sym}_SEDAR"
                co_dir.mkdir(parents=True, exist_ok=True)
                try: date_iso = datetime.strptime(filing_date.strip(), "%d %b %Y").strftime("%Y-%m-%d")
                except: date_iso = filing_date.replace(" ","-")
                pdf_path = co_dir / f"{date_iso}_{safe_sym}_{label.replace(' ','_')}.pdf"

                try:
                    with page.expect_download(timeout=60000) as dl_info:
                        try:
                            page.goto(href, wait_until="load", timeout=30000)
                        except Exception:
                            # SEDAR+ resource URLs immediately trigger a file download,
                            # which causes page.goto to throw "Download is starting".
                            # That's expected - the download event is already captured.
                            pass
                    download = dl_info.value
                    download.save_as(pdf_path)
                    pdf_bytes = pdf_path.read_bytes()
                    if pdf_bytes[:4] != b"%PDF":
                        log.debug(f"  {symbol}: expect_download returned non-PDF ({len(pdf_bytes)}b) - discarding")
                        pdf_bytes = None
                        try: pdf_path.unlink()
                        except: pass
                except Exception as e:
                    log.debug(f"  {symbol}: expect_download error: {e}")
                    pdf_bytes = None

                if not pdf_bytes:
                    log.info(f"  {symbol}: PDF download failed for {label}")
                    continue

                log.info(f"  {symbol}: saved {pdf_path.name} ({len(pdf_bytes)//1024}KB)")

                text = _extract_pdf_text(pdf_bytes)
                result = {
                    "filing_type":       label,
                    "filing_date":       date_iso,
                    "filing_doc_name":   doc_name,
                    "filing_url":        href,
                    "filing_pdf_path":   str(pdf_path),
                    "filing_text_chars": len(text),
                    "sedar_only":        "Y",
                    "_filing_text":      text,
                }
                if sedar_type == "ANNUAL_INFORMATION_FORMS":
                    aif_result = result
                else:
                    mda_result = result

            except Exception as e:
                log.warning(f"  {symbol}: SEDAR {sedar_type} error: {type(e).__name__}: {e}")
                continue

        best = aif_result or mda_result
        if best and mda_result and mda_result is not best:
            best["mda_pdf_path"]   = mda_result.get("filing_pdf_path","")
            best["_mda_text"]      = mda_result.get("_filing_text","")
            best["mda_text_chars"] = mda_result.get("filing_text_chars",0)
        return best or {}


    fetched = 0
    with sync_playwright() as pw:
        browser = pw.chromium.connect_over_cdp(CDP_URL)
        ctx     = browser.contexts[0]

        # First: lookup party numbers for those that don't have one
        if no_party:
            log.info(f"  Looking up {len(no_party)} missing party numbers...")
            page = ctx.new_page()
            try:
                for sym in no_party:
                    row = results_map.get(sym, {})
                    pnum, pname = _sedar_lookup_party(page, sym, row.get("name",""))
                    if pnum:
                        party_map[sym] = pnum
                        row["sedar_party_number"] = pnum
                        row["sedar_party_name"]   = pname
                        log.info(f"  {sym}: party number found: {pnum}")
            finally:
                try: page.close()
                except: pass

        # Now fetch filings for all that have party numbers
        page = ctx.new_page()
        try:
            for sym in candidates:
                pnum = party_map.get(sym)
                if not pnum:
                    log.info(f"  {sym}: no party number - skipping")
                    continue
                row = results_map.get(sym, {})
                filing = _fetch_sedar_filing(page, pnum, sym)
                if filing:
                    for k, v in filing.items():
                        if not k.startswith("_"):
                            row[k] = v
                    row["_filing_text"] = filing.get("_filing_text", "")
                    fetched += 1
                    log.info(f"  {sym}: fetched {filing.get('filing_type','')} ({filing.get('filing_text_chars',0)} chars)")
                else:
                    log.info(f"  {sym}: no filing found on SEDAR+")
        finally:
            try: page.close()
            except: pass
            try: browser.close()
            except: pass

    save_results(results)
    log.info(f"Phase 5 complete: {fetched}/{len(candidates)} fetched from SEDAR+")
    return results

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase",  default="all",
                    choices=["1","2","3","5","all"])
    ap.add_argument("--limit",  type=int, default=0)
    ap.add_argument("--symbol", type=str, default="")
    args = ap.parse_args()

    now = datetime.now(TORONTO_TZ)
    log.info("=" * 70)
    log.info(f"UNIVERSE BUILDER v2 - Phase {args.phase} - {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    log.info("=" * 70)

    with open(MASTER_CSV, newline="", encoding="utf-8-sig") as f:
        master = list(csv.DictReader(f))
    log.info(f"Master loaded: {len(master)} companies")

    if args.phase in ("1", "all"):
        results = phase1_tier_assignment(master)
        # Carry over existing results (party numbers, filings, LLM) from previous run
        existing = {r["symbol"]: r for r in load_results()}
        for r in results:
            sym = r["symbol"]
            if sym in existing:
                for k in RESULT_FIELDS:
                    if not r.get(k) and existing[sym].get(k):
                        r[k] = existing[sym][k]
                # Carry over _filing_text (not in RESULT_FIELDS)
                if existing[sym].get("_filing_text"):
                    r["_filing_text"] = existing[sym]["_filing_text"]
        save_results(results)
        log.info("Phase 1 complete")
    else:
        results = load_results()
        if not results:
            log.error("No results found - run Phase 1 first")
            return
        # Reload _filing_text from phase 2 cache if needed
        # (it's not stored in CSV - re-fetch in phase 3 if missing)

    if args.phase in ("2", "all"):
        results = phase2_fetch_filings(results, limit=args.limit, symbol_filter=args.symbol)

    if args.phase in ("3", "all"):
        results = phase3_llm_classify(results, limit=args.limit, symbol_filter=args.symbol)

    if args.phase == "5":
        results = phase5_sedar_direct_fetch(str(NO_FILING_CSV), results)
        # Chain Phase 3 on newly fetched companies
        results = phase3_llm_classify(results, symbol_filter=args.symbol)

    elapsed = (datetime.now(TORONTO_TZ) - now).total_seconds()
    log.info("=" * 70)
    log.info(f"DONE in {elapsed:.0f}s")
    log.info("=" * 70)

if __name__ == "__main__":
    main()
