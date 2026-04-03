"""
mm_onboarding.py - MM Company Onboarding
=========================================
Builds a comprehensive backlog of relevant SEDAR+ filings for specific
Canadian mining companies, from their last AIF "as at" date to today.

Purpose: Quickly establish a picture of each company's current mining
projects and any changes to mineral resources/reserves since the last AIF.

Pipeline:
  Phase 1 (Stockwatch fast path):
    - Search stockwatch.com/News/Sedar by ticker for all filings since AIF as-at date
    - Collect up to 200 rows per window; if exactly 200, split date range and recurse
    - PDF URLs from Stockwatch are permanent (stockwatch.com/News/Sedardoc/{id}.pdf)
    - Download relevant types; log all types in CSV
    - Covers: as-at date â†’ today minus ~2 weeks (Stockwatch lag)

  Phase 2 (SEDAR+ gap fill):
    - Use SEDAR+ browser to fetch filings from last 14 days for this company
    - Uses our proven party number + chip approach from Universe Builder
    - Covers: today-14 days â†’ today

AIF "as at" inference:
  - Filed month M â†’ as at = last day of (M-3), adjusting year if needed
  - e.g. filed 14 Mar 2026 â†’ as at 31 Dec 2025
  - e.g. filed 15 Jun 2025 â†’ as at 31 Mar 2025

Relevant filing types (downloaded as PDF):
  - NI 43-101 Technical Reports
  - Material Change Reports
  - Prospectus (short form, long form, preliminary)
  - Business Acquisition Reports
  - Filing Statements (reverse takeovers)

Logged only (not downloaded):
  - News Releases (all logged in CSV for reference)
  - Everything else

Input:  custom_run_or_onboarding_list.csv  (symbol, company_name, exchange, sedar_party_number)
Output per company:
  Results/{SYMBOL}/filings_log.csv   - every filing with full metadata
  Results/{SYMBOL}/pdfs/{category}/  - downloaded relevant PDFs

Usage:
  python mm_onboarding.py --companies custom_run_or_onboarding_list.csv
  python mm_onboarding.py --companies custom_run_or_onboarding_list.csv --limit 3   # test
  python mm_onboarding.py --symbol AYA                          # single

Backup:  _backups/mm_onboarding_YYYY-MM-DD_HHMM.bak  (always before editing)
Git:     gcbarclawbot/sedar-automation
"""

import sys
import os
import csv
import re
import json
import time
import logging
import requests
import pytz
import tomllib
import calendar
from datetime import datetime, date, timedelta
from pathlib import Path
from bs4 import BeautifulSoup
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from aif_date_extractor import extract_aif_as_at_date
sys.path.insert(0, str(Path(__file__).parent.parent))  # repo root for shared modules
from stockwatch_auth import get_cookies as _get_sw_cookies_shared, test_session as _test_sw_session, SESSION_PATH as _SW_SESSION_PATH

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Ensure OPENAI_API_KEY is available even when running as a Windows service
# (NSSM services run as SYSTEM and don't inherit user environment variables)
# Load from credentials file which is accessible to all users/services
if not os.environ.get("OPENAI_API_KEY"):
    try:
        _creds = Path(r"C:\Users\Admin\.openclaw\credentials\openai.env")
        for _line in _creds.read_text(encoding="utf-8").splitlines():
            if _line.startswith("OPENAI_API_KEY="):
                os.environ["OPENAI_API_KEY"] = _line.split("=", 1)[1].strip()
                break
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR    = Path(__file__).parent
COMPANIES_CSV = SCRIPT_DIR / "custom_run_or_onboarding_list.csv"
UNIVERSE_CSV  = SCRIPT_DIR.parent / "1. Canadian Master Sync" / "canadian_universe.csv"
RESULTS_DIR   = SCRIPT_DIR / "Results"
LOG_PATH      = SCRIPT_DIR / "mm_onboarding.log"
SESSION_PATH  = _SW_SESSION_PATH  # canonical shared session file
CDP_URL       = "http://127.0.0.1:18800"

def load_universe_lookup() -> dict:
    """Load canadian_universe.csv as a dict keyed by symbol."""
    lookup = {}
    if not UNIVERSE_CSV.exists():
        log.warning(f"Universe CSV not found: {UNIVERSE_CSV}")
        return lookup
    with open(UNIVERSE_CSV, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            sym = row.get("symbol", "").strip().upper()
            if sym:
                lookup[sym] = row
    return lookup

def ensure_cdp_ready(timeout: int = 30) -> bool:
    """
    Check if OpenClaw browser CDP is reachable. If not, launch it via
    'openclaw browser start' and wait up to `timeout` seconds for it to come up.
    Returns True if CDP is ready, False if it never came up.
    """
    import urllib.request, subprocess as _sp, time as _time

    def _cdp_alive() -> bool:
        try:
            urllib.request.urlopen(f"{CDP_URL}/json", timeout=2)
            return True
        except Exception:
            return False

    if _cdp_alive():
        return True

    log.info("  CDP not reachable - launching OpenClaw browser...")
    try:
        _sp.Popen(["openclaw", "browser", "start"], shell=True,
                  stdout=_sp.DEVNULL, stderr=_sp.DEVNULL)
    except Exception as e:
        log.warning(f"  Failed to launch browser: {e}")
        return False

    deadline = _time.time() + timeout
    while _time.time() < deadline:
        _time.sleep(2)
        if _cdp_alive():
            log.info("  CDP ready")
            return True

    log.warning(f"  CDP still not reachable after {timeout}s")
    return False
TORONTO_TZ    = pytz.timezone("America/Toronto")
SEDAR_BASE    = "https://www.sedarplus.ca"
SW_SEDAR_URL  = "https://www.stockwatch.com/News/Sedar"
SW_BASE       = "https://www.stockwatch.com"

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
# Cloudflare R2 upload (same bucket as heatmap/newsletter: ds-newsletter)
# ---------------------------------------------------------------------------
R2_ENDPOINT    = "https://ce472fd89429a4b3960a37955e5766ee.r2.cloudflarestorage.com"
R2_BUCKET      = "ds-newsletter"
R2_PUBLIC_BASE = "https://pub-e6549556cd074443b759440dcb9b174f.r2.dev"
R2_ACCESS_KEY  = "47a85a6618fd3587e89b525700d13fb9"
R2_SECRET_KEY  = "190c9af4c1eda599c2dba24e2e0446484a0038aa3cbbea223c8a17e11f37071c"
_r2_client = None

def _get_r2():
    global _r2_client
    if _r2_client is None:
        import boto3
        _r2_client = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name="auto",
        )
    return _r2_client

def upload_to_r2(local_path: Path, symbol: str) -> str:
    """Upload PDF to R2 at onboarding/{SYMBOL}/{filename}. Returns public URL."""
    try:
        key        = f"onboarding/{symbol}/{local_path.name}"
        public_url = f"{R2_PUBLIC_BASE}/{key}"
        _get_r2().upload_file(
            str(local_path), R2_BUCKET, key,
            ExtraArgs={"ContentType": "application/pdf"}
        )
        return public_url
    except Exception as e:
        log.debug(f"  R2 upload failed {local_path.name}: {e}")
        return ""

def upload_html_to_r2(local_path: Path, symbol: str) -> str:
    """Upload HTML news release to R2 at onboarding/{SYMBOL}/news_html/{filename}. Returns public URL."""
    try:
        key        = f"onboarding/{symbol}/news_html/{local_path.name}"
        public_url = f"{R2_PUBLIC_BASE}/{key}"
        _get_r2().upload_file(
            str(local_path), R2_BUCKET, key,
            ExtraArgs={"ContentType": "text/html; charset=utf-8"}
        )
        return public_url
    except Exception as e:
        log.debug(f"  R2 HTML upload failed {local_path.name}: {e}")
        return ""

# ---------------------------------------------------------------------------
# Per-company state (Results/{SYMBOL}/state.json)
# ---------------------------------------------------------------------------
def attach_company_log(symbol: str) -> logging.FileHandler:
    """Add a per-company log file handler. Returns handler so caller can remove it."""
    log_path = RESULTS_DIR / symbol / "run.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(handler)
    return handler

def detach_company_log(handler: logging.FileHandler):
    """Remove the per-company log handler and flush."""
    try:
        handler.flush()
        handler.close()
        log.removeHandler(handler)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# LLM classification of news releases
# ---------------------------------------------------------------------------
def load_prompt() -> str:
    """Load prompt.txt from script dir."""
    prompt_path = SCRIPT_DIR / "prompt.txt"
    if prompt_path.exists():
        return prompt_path.read_text(encoding="utf-8")
    return "Classify this news release as CHANGED, POSSIBLE, or NONE for resource/reserve changes. Respond JSON: {\"flag\": \"...\", \"summary\": \"...\"}\n\n{text}"

def classify_material_change(pdf_path: str, model: str = "gpt-4o-mini") -> str:
    """Extract the nature of a material change from its PDF. Returns <=5 word description."""
    try:
        import fitz as _fitz
        from openai import OpenAI
        with _fitz.open(pdf_path) as doc:
            text = "\n".join(page.get_text() for page in doc)
        text = text.encode("ascii", "ignore").decode("ascii")[:15000]
        client = OpenAI()
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content":
                f"Summarise the nature of this material change in 5 words or fewer. "
                f"Be specific (e.g. 'Oko West construction decision', 'CEO resignation', 'La Mancha top-up investment', 'Environmental permit received'). "
                f"Return only the phrase, no punctuation, no explanation.\n\n{text}"}],
            temperature=0,
            max_tokens=20,
        )
        return (resp.choices[0].message.content or "").strip()[:60]
    except Exception as e:
        log.debug(f"  Material change classify error: {e}")
        return ""

def run_material_change_classification(symbol: str, all_filings: list,
                                        model: str = "gpt-4o-mini",
                                        max_workers: int = 20) -> list:
    """Classify all downloaded material change PDFs with a brief description."""
    to_classify = [
        (i, f) for i, f in enumerate(all_filings)
        if f.get("category") == "MaterialChange"
        and f.get("pdf_path")
        and not f.get("mat_summary")
    ]
    if not to_classify:
        return all_filings

    log.info(f"  Material changes: classifying {len(to_classify)} PDFs ({model})")

    def _classify(args):
        idx, filing = args
        summary = classify_material_change(filing["pdf_path"], model)
        return idx, summary

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_classify, item): item for item in to_classify}
        for future in as_completed(futures):
            try:
                idx, summary = future.result()
                all_filings[idx]["mat_summary"] = summary
            except Exception:
                pass

    return all_filings

RESULTS_RELEASE_KEYWORDS = re.compile(
    r'\b(q[1-4]\s*20\d\d\s+results?|first quarter|second quarter|third quarter|fourth quarter|'
    r'annual results?|full.year results?|year.end results?|financial results?|'
    r'operational results?|production results?|quarterly results?)\b',
    re.IGNORECASE
)

def classify_news_release(text: str, headline: str, prompt_template: str,
                           model: str = "gpt-4o-mini",
                           company_name: str = "",
                           ni43101_hint: str = "") -> dict:
    """Call OpenAI to classify one news release. Returns {flag, summary} or defaults on error."""
    try:
        from openai import OpenAI
        client = OpenAI()
        truncated = (headline + "\n\n" + text)[:15000]
        filled    = prompt_template.replace("{text}", truncated).replace("{company}", company_name or "the company")

        # Inject NI43-101 hint first (highest priority context)
        if ni43101_hint:
            filled = ni43101_hint + filled
        # Pre-filter: inject results-release warning if headline signals a periodic results announcement
        elif RESULTS_RELEASE_KEYWORDS.search(headline):
            warning = (
                "⚠ IMPORTANT CONTEXT: This release appears to be a periodic results announcement "
                "(e.g. quarterly or annual financial/operational results). "
                "Apply Rule 2 strictly: any MRE or reserve figures mentioned are almost certainly "
                "recaps of prior announcements made during the period, NOT new changes. "
                "Default strongly to NONE unless the release is clearly and primarily dedicated "
                "to announcing a new resource/reserve update.\n\n"
            )
            filled = warning + filled
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": filled}],
            temperature=0,
            max_tokens=120,
            response_format={"type": "json_object"},
        )
        raw  = resp.choices[0].message.content or "{}"
        data = json.loads(raw)
        flag    = data.get("flag", "NONE").upper()
        summary = data.get("summary", "")[:100]
        project = data.get("project", "")[:80] if flag == "CHANGED" else ""
        if flag not in ("CHANGED", "POSSIBLE", "NONE"):
            flag = "NONE"
        return {"llm_flag": flag, "llm_summary": summary, "llm_project": project, "llm_error": ""}
    except Exception as e:
        return {"llm_flag": "NONE", "llm_summary": "", "llm_project": "", "llm_error": str(e)[:80]}

def run_llm_classification(symbol: str, all_filings: list,
                            model: str = "gpt-4o-mini",
                            max_workers: int = 80) -> list:
    """
    Classify all news releases that have text but no LLM flag yet.
    Updates filings in-place. Returns updated list.
    Uses up to max_workers parallel threads.
    """
    # ---------------------------------------------------------------------------
    # Pre-screen: deterministic title-based classification before hitting LLM
    # gpt-4o-mini misclassifies certain patterns regardless of prompt wording
    import re as _re

    # 1. Standalone R&R update releases are always CHANGED
    #    Mark with a special tag so the LLM classify step still runs to get a proper summary
    #    but the flag is pinned to CHANGED regardless of what the LLM returns
    RR_UPDATE_TITLE_KEYWORDS = [
        r'\b(mineral\s+)?(reserve|resource)s?\s+(and\s+(mineral\s+)?(reserve|resource)s?\s+)?update\b',
        r'\bannounces?\s+(year.end|annual|updated?)\s+(mineral\s+)?(reserve|resource)',
        r'\breport(s|ing)?\s+(year.end|annual)\s+(mineral\s+)?(reserve|resource)',
        r'\b(year.end|annual)\s+(mineral\s+)?(reserve|resource)s?\b',
        r'\bmineral\s+reserve\s+and\s+resource\b',  # singular form
        r'\b(reserve|resource)\s+and\s+resource\s+(estimate|update)\b',  # "reserve and resource estimates"
    ]
    for i, f in enumerate(all_filings):
        if f.get("category") != "NewsRelease" or f.get("llm_flag"):
            continue
        synopsis = (f.get("synopsis") or "").lower()
        if any(_re.search(kw, synopsis, _re.IGNORECASE) for kw in RR_UPDATE_TITLE_KEYWORDS):
            f["_rr_prescreened"] = True  # sentinel: LLM runs for summary but flag is pinned CHANGED
            log.info(f"  Pre-screen CHANGED (R&R update): {f.get('filing_date','')} - {synopsis[:60]}")

    # 2. Periodic financial/operational results releases are always NONE
    RESULTS_TITLE_KEYWORDS = [
        r'\bQ[1-4][-\s]?20\d\d\b',          # Q1-2025, Q3 2024 etc.
        r'\b(first|second|third|fourth) quarter\b',
        r'\b(full[ -]year|full year|annual)\b.*\b(result|financial|report)\b',
        r'\b(financial|operational)\s+result',
        r'\brecord (revenue|results|income|earnings)\b',
        r'\b(year ended|quarter ended)\b',
        r'\b(production|operational) (update|result)',
    ]
    for i, f in enumerate(all_filings):
        if f.get("category") != "NewsRelease" or f.get("llm_flag"):
            continue
        synopsis = (f.get("synopsis") or "").lower()
        if any(_re.search(kw, synopsis, _re.IGNORECASE) for kw in RESULTS_TITLE_KEYWORDS):
            f["llm_flag"]    = "NONE"
            f["llm_summary"] = "Periodic results/financial release (pre-screened)"
            f["llm_error"]   = ""
            log.info(f"  Pre-screen NONE: {f.get('filing_date','')} - {synopsis[:60]}")
    # ---------------------------------------------------------------------------

    prompt = load_prompt()
    to_classify = [
        (i, f) for i, f in enumerate(all_filings)
        if f.get("category") == "NewsRelease"
        and f.get("news_text")
        and not f.get("llm_flag")
    ]
    if not to_classify:
        log.info(f"  LLM: no news releases to classify for {symbol}")
        return all_filings

    log.info(f"  LLM: classifying {len(to_classify)} news releases ({model}, {max_workers} workers)")

    # Get company name from first filing
    company_name = next((f.get("issuer","") for f in all_filings if f.get("issuer")), symbol)

    # Build a lookup of NI43-101 technical reports by date for hint injection
    ni43101_by_date: dict = {}
    for f in all_filings:
        dt = (f.get("doc_type") or "").upper()
        if ("TECHNICAL_REPORT" in dt
                and "CONSENT" not in dt
                and "CERTIFICATE" not in dt
                and f.get("filing_date")):
            date_str = f.get("filing_date", "")[:10]
            ni43101_by_date.setdefault(date_str, []).append(f.get("synopsis", "") or f.get("doc_type", ""))

    def _get_ni_hint(filing_date: str) -> str:
        """Return hint text if an NI43-101 was filed 0-3 days before this release."""
        try:
            fdate = date.fromisoformat(filing_date[:10])
        except Exception:
            return ""
        for delta in range(4):  # 0, 1, 2, 3 days before
            check = (fdate - timedelta(days=delta)).isoformat()
            if check in ni43101_by_date:
                titles = "; ".join(t for t in ni43101_by_date[check] if t)
                return (
                    f"⚠ NI 43-101 CONTEXT: A NI 43-101 technical report was filed on {check} "
                    f"(within 3 days of this release): {titles or 'technical report'}. "
                    f"This means the release IS accompanied by a formal NI 43-101 filing - "
                    f"classify as CHANGED and identify the project from the report title.\n\n"
                )
        return ""

    def _classify(args):
        idx, filing = args
        ni_hint = _get_ni_hint(filing.get("filing_date", ""))
        result = classify_news_release(
            filing.get("news_text", ""),
            filing.get("synopsis", ""),
            prompt, model,
            company_name=company_name,
            ni43101_hint=ni_hint
        )
        return idx, result

    done = failed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_classify, item): item for item in to_classify}
        for future in as_completed(futures):
            try:
                idx, result = future.result()
                all_filings[idx].update(result)
                # Pin flag to CHANGED for R&R pre-screened releases (LLM runs for summary only)
                if all_filings[idx].get("_rr_prescreened"):
                    all_filings[idx]["llm_flag"] = "CHANGED"
                if result.get("llm_error"):
                    failed += 1
                else:
                    done += 1
            except Exception as e:
                failed += 1

    changed  = sum(1 for f in all_filings if f.get("llm_flag") == "CHANGED")
    possible = sum(1 for f in all_filings if f.get("llm_flag") == "POSSIBLE")
    log.info(f"  LLM: {done} classified, {failed} failed | ðŸ”´{changed} CHANGED, ðŸŸ¡{possible} POSSIBLE")
    return all_filings

def load_company_state(symbol: str) -> dict:
    """Load previous run state for a company, or empty dict if first run."""
    state_path = RESULTS_DIR / symbol / "state.json"
    if state_path.exists():
        try:
            return json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def save_company_state(symbol: str, state: dict):
    """Save run state for a company."""
    state_path = RESULTS_DIR / symbol / "state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")

# ---------------------------------------------------------------------------
# Filing classification
# ---------------------------------------------------------------------------
# Stockwatch doc type â†’ our category
SW_TYPE_MAP = {
    "ANNUAL_INFORMATION_FORM_EN":           "AIF",
    "ANNUAL_INFORMATION_FORM_FR":           "AIF",
    "TECHNICAL_REPORT_NI_43101_EN":         "NI43-101",
    "TECHNICAL_REPORT_NI_43101_FR":         "NI43-101",
    "MATERIAL_CHANGE_REPORT_EN":            "MaterialChange",
    "MATERIAL_CHANGE_REPORT_FR":            "MaterialChange",
    "MATERIAL_CHANGE_REPORT_AMENDED_EN":    "MaterialChange",
    "INTERIM_MDA_EN":                       "MD&A",
    "INTERIM_MDA_FR":                       "MD&A",
    "MDA_EN":                               "MD&A",
    "MDA_FR":                               "MD&A",
    "ANNUAL_MDANDA_EN":                     "MD&A",
    "FINAL_SHORT_FORM_PROSPECTUS_EN":       "Prospectus",
    "FINAL_SHORT_FORM_PROSPECTUS_FR":       "Prospectus",
    "PRELIMINARY_SHORT_FORM_PROSPECTUS_EN": "Prospectus",
    "PRELIMINARY_SHORT_FORM_PROSPECTUS_FR": "Prospectus",
    "FINAL_LONG_FORM_PROSPECTUS_EN":        "Prospectus",
    "LONG_FORM_PROSPECTUS_EN":              "Prospectus",
    "SHELF_PROSPECTUS_EN":                  "Prospectus",
    "BUSINESS_ACQUISITION_REPORT_EN":       "Acquisition",
    "BUSINESS_ACQUISITION_REPORT_FR":       "Acquisition",
    "FILING_STATEMENT_EN":                  "Acquisition",
    "FILING_STATEMENT_FR":                  "Acquisition",
    "NEWS_RELEASE_EN":                      "NewsRelease",
    "NEWS_RELEASE_FR":                      "NewsRelease",
}

# Categories we download PDFs for (news releases are log-only)
DOWNLOAD_CATEGORIES = {"AIF", "NI43-101", "MD&A", "MaterialChange", "Prospectus", "Acquisition"}

# Within NI43-101 category, only download the actual technical report (not consents/certs)
NI43101_DOWNLOAD_TYPES = {
    "TECHNICAL_REPORT_NI_43101_EN",
    "TECHNICAL_REPORT_NI_43101_FR",
    "AMENDED_AND_RESTATED_TECHNICAL_REPORT_EN",
}

def _is_french(doc_type: str, synopsis: str = "") -> bool:
    """Return True if this filing is a French-language document."""
    dt = (doc_type or "").upper()
    # Stockwatch doc types end with _FR for French versions
    if dt.endswith("_FR"):
        return True
    # Also check synopsis/doc name (e.g. "Annual MD&A - French.pdf")
    syn = (synopsis or "").lower()
    if " - french" in syn or "(french)" in syn or "- fr." in syn:
        return True
    return False

def _should_download(category: str, doc_type: str, synopsis: str = "") -> bool:
    """Return True if this filing should have its PDF downloaded."""
    # Never download French-language versions
    if _is_french(doc_type, synopsis):
        return False
    if category == "NI43-101":
        # Only download the actual technical report, not consent/certificate docs
        return doc_type in NI43101_DOWNLOAD_TYPES
    return category in DOWNLOAD_CATEGORIES

def _classify_sw_type(doc_type: str) -> str:
    """Map Stockwatch doc type string to our category."""
    cat = SW_TYPE_MAP.get(doc_type)
    if cat:
        return cat
    dt = doc_type.upper()
    if "TECHNICAL_REPORT" in dt or "NI_43101" in dt:
        return "NI43-101"
    if "MATERIAL_CHANGE" in dt:
        return "MaterialChange"
    if "INTERIM_MDA" in dt or "MDA" in dt or "MDANDA" in dt:
        return "MD&A"
    if "PROSPECTUS" in dt:
        return "Prospectus"
    if "ACQUISITION" in dt or "FILING_STATEMENT" in dt:
        return "Acquisition"
    if "NEWS_RELEASE" in dt or "PRESS_RELEASE" in dt:
        return "NewsRelease"
    if "ANNUAL_INFORMATION_FORM" in dt:
        return "AIF"
    return "Other"

# ---------------------------------------------------------------------------
# AIF "as at" date inference
# ---------------------------------------------------------------------------
def infer_as_at_date(aif_filing_date: date) -> date:
    """
    Infer the "as at" date from the AIF filing date.
    Rule: as_at = last day of (filing_month - 3), adjusting year if needed.
    e.g. filed 14 Mar 2026 â†’ as at 31 Dec 2025
         filed 15 Jun 2025 â†’ as at 31 Mar 2025
         filed 20 Jan 2025 â†’ as at 31 Oct 2024
    """
    m = aif_filing_date.month - 3
    y = aif_filing_date.year
    if m <= 0:
        m += 12
        y -= 1
    last_day = calendar.monthrange(y, m)[1]
    return date(y, m, last_day)

# ---------------------------------------------------------------------------
# Stockwatch session - delegated to shared stockwatch_auth module
# ---------------------------------------------------------------------------
def get_stockwatch_cookies() -> dict:
    """Delegate to shared stockwatch_auth module (canonical session, shared login logic)."""
    return _get_sw_cookies_shared()


class StockwatchSedarSession:
    """
    Handles searching stockwatch.com/News/Sedar by ticker + date range.

    Key notes:
    - ASP.NET WebForms - must POST with __VIEWSTATE etc. from the form
    - Results in table id='MainContent_gSedar', max 200 rows
    - PDF URLs: stockwatch.com/News/Sedardoc/{id}.pdf - permanent, no auth needed
    - Doc type in column 4 (index 4), e.g. 'ANNUAL_INFORMATION_FORM_EN'
    - Date in column 3 (index 3), format YYYY-MM-DD
    - If exactly 200 rows returned, split date range and recurse
    """

    def __init__(self, cookies: dict, delay: float = 0.5):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": SW_SEDAR_URL,
        })
        for name, value in cookies.items():
            self.session.cookies.set(name, value, domain="www.stockwatch.com")
        self._vs = self._vsg = ""
        self._load_form()

    def _sleep(self):
        time.sleep(self.delay)

    def _load_form(self):
        resp = self.session.get(SW_SEDAR_URL, timeout=30)
        resp.raise_for_status()
        body = resp.text
        if ("NotLoggedIn" in resp.url or "notloggedin" in resp.url.lower() or
                "NotLoggedIn" in body or
                ("PowerUserName" in body and "PowerPassword" in body)):
            raise RuntimeError("Stockwatch session expired - re-login required")
        soup = BeautifulSoup(body, "html.parser")
        self._vs  = (soup.find("input", {"id": "__VIEWSTATE"})          or {}).get("value", "")
        self._vsg = (soup.find("input", {"id": "__VIEWSTATEGENERATOR"}) or {}).get("value", "")
        if not self._vs:
            raise RuntimeError("Stockwatch session expired - no VIEWSTATE found (not authenticated)")
        log.info(f"  Stockwatch SEDAR form loaded (logged_in=True)")
        self._sleep()

    def _post_search(self, symbol: str, date_from: date, date_to: date,
                     doc_type: str = "") -> str:
        """POST the search form, return raw HTML."""
        data = [
            ("__EVENTTARGET", ""), ("__EVENTARGUMENT", ""),
            ("__VIEWSTATE", self._vs),
            ("__VIEWSTATEGENERATOR", self._vsg),
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
        resp = self.session.post(SW_SEDAR_URL, data=data, timeout=60)
        resp.raise_for_status()
        self._sleep()
        return resp.text

    def _parse_results(self, html: str) -> list[dict]:
        """Parse MainContent_gSedar table into list of filing dicts."""
        soup = BeautifulSoup(html, "html.parser")
        tbl  = soup.find("table", {"id": "MainContent_gSedar"})
        if not tbl:
            return []
        filings = []
        for row in tbl.find_all("tr")[1:]:  # skip header
            cells = row.find_all("td")
            if len(cells) < 6:
                continue
            symbol_text = cells[1].get_text(strip=True)
            issuer      = cells[2].get_text(strip=True)
            filing_date = cells[3].get_text(strip=True)
            type_text   = cells[4].get_text("\n", strip=True)
            doc_type    = type_text.split("\n")[0].strip() if type_text else ""
            industry    = type_text.split("\n")[1].strip() if "\n" in type_text else ""
            pdf_link    = cells[5].find("a")
            pdf_url     = pdf_link["href"] if pdf_link else ""
            if pdf_url and not pdf_url.startswith("http"):
                pdf_url = f"{SW_BASE}{pdf_url}"
            synopsis = cells[6].get_text(strip=True) if len(cells) > 6 else ""
            # Skip French-language filings entirely (we only want English)
            if _is_french(doc_type, synopsis):
                continue
            filings.append({
                "source":       "stockwatch",
                "symbol":       symbol_text,
                "issuer":       issuer,
                "filing_date":  filing_date,
                "doc_type":     doc_type,
                "industry":     industry,
                "pdf_url":      pdf_url,
                "synopsis":     synopsis,
                "category":     _classify_sw_type(doc_type),
            })
        return filings

    def search(self, symbol: str, date_from: date, date_to: date,
               doc_type: str = "") -> list[dict]:
        """
        Search by ticker + date range. If exactly 200 results, split date
        range in half and recurse to ensure complete coverage.
        """
        log.info(f"  SW search: {symbol} | {date_from} â†’ {date_to} | type={doc_type or 'all'}")
        html     = self._post_search(symbol, date_from, date_to, doc_type)
        filings  = self._parse_results(html)
        log.info(f"    â†’ {len(filings)} rows")

        if len(filings) == 200:
            # Date window too large - split in half and recurse
            mid = date_from + (date_to - date_from) / 2
            mid = date(mid.year, mid.month, mid.day)
            log.info(f"    200 rows - splitting: {date_from}â†’{mid} and {mid+timedelta(1)}â†’{date_to}")
            left  = self.search(symbol, date_from,              mid,                  doc_type)
            right = self.search(symbol, mid + timedelta(days=1), date_to,             doc_type)
            # Deduplicate by (filing_date, doc_type, pdf_url)
            seen = set()
            combined = []
            for f in left + right:
                key = (f["filing_date"], f["doc_type"], f["pdf_url"])
                if key not in seen:
                    seen.add(key)
                    combined.append(f)
            return combined

        return filings

    def fetch_news_text_for_symbol(self, symbol: str,
                                    date_from: date, date_to: date) -> dict[str, dict]:
        """
        Search /News/Search for news releases by symbol + date range.
        Returns dict keyed by date string 'YYYY-MM-DD' -> {article_url, article_id, headline, text}.
        Only fetches News Release type (dType=200).

        Article URL format: /News/Item/Z-C!{SYM}-{article_id}/C/{SYM}
        Text extraction: strip sidebar tables, find release body after timestamp line.
        """
        log.info(f"  SW news search: {symbol} | {date_from} â†’ {date_to}")
        # POST to /News/Search (different form to /News/Sedar)
        sw_search_url = "https://www.stockwatch.com/News/Search"
        # Need fresh viewstate from /News/Search
        resp_form = self.session.get(sw_search_url, timeout=30)
        soup_form = BeautifulSoup(resp_form.text, "html.parser")
        vs  = (soup_form.find("input", {"id": "__VIEWSTATE"})          or {}).get("value", "")
        vs1 = (soup_form.find("input", {"id": "__VIEWSTATE1"})          or {}).get("value", "")
        vsg = (soup_form.find("input", {"id": "__VIEWSTATEGENERATOR"}) or {}).get("value", "")
        self._sleep()

        data = [
            ("__EVENTTARGET", ""), ("__EVENTARGUMENT", ""),
            ("__VIEWSTATEFIELDCOUNT", "2"),
            ("__VIEWSTATE", vs), ("__VIEWSTATE1", vs1),
            ("__VIEWSTATEGENERATOR", vsg),
            ("ctl00$TextSymbol2", ""), ("ctl00$RadioRegion2", "RadioCanada2"),
            ("ctl00$setfocus", ""), ("ctl00$scrolly", ""),
            ("ctl00$redirectto", ""), ("ctl00$showcert", ""),
            ("ctl00$MainContent$tSymbol",     symbol),
            ("ctl00$MainContent$tSymbolFrom", date_from.strftime("%Y%m%d")),
            ("ctl00$MainContent$tSymbolTo",   date_to.strftime("%Y%m%d")),
            ("ctl00$MainContent$dSymbolFeed", "C"),   # Canada
            ("ctl00$MainContent$dType",       "200"), # News releases only
            ("ctl00$MainContent$bSymbol.x",   "0"),
            ("ctl00$MainContent$bSymbol.y",   "0"),
        ]
        resp = self.session.post(sw_search_url, data=data, timeout=60)
        self._sleep()
        soup = BeautifulSoup(resp.text, "html.parser")

        tbl = soup.find("table", id="MainContent_NewsList_gNews")
        if not tbl:
            log.info(f"    No news results for {symbol}")
            return {}

        # Parse all rows - build lookup by date
        news_by_date: dict = {}
        for row in tbl.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) < 6:
                continue
            pub_dt   = cells[0].get_text(strip=True)  # "2026-03-12 07:00"
            date_key = pub_dt[:10]                      # "2026-03-12"
            news_type= cells[4].get_text(strip=True) if len(cells) > 4 else ""
            if "News Release" not in news_type and "200" not in news_type:
                continue  # skip "In the News" etc.
            headline = cells[5].get_text(strip=True) if len(cells) > 5 else ""
            link     = cells[5].find("a") if len(cells) > 5 else None
            art_url  = link["href"] if link else ""
            if art_url and not art_url.startswith("http"):
                art_url = f"https://www.stockwatch.com{art_url}"
            art_id_m = re.search(r"-(\d+)/", art_url)
            art_id   = art_id_m.group(1) if art_id_m else ""

            if date_key not in news_by_date:  # keep first (most recent same-day)
                news_by_date[date_key] = {
                    "article_url": art_url,
                    "article_id":  art_id,
                    "headline":    headline,
                    "pub_datetime": pub_dt,
                    "text":        "",
                }

        log.info(f"    {len(news_by_date)} news releases found")

        # Fetch text for each article - parallel with 5 workers, small per-worker delay
        import time as _time
        import copy as _copy
        fetched = failed = 0

        def _fetch_article(args):
            date_key, item = args
            url = item["article_url"]
            if not url:
                return date_key, None
            _time.sleep(0.3)  # polite per-worker delay
            try:
                r = self.session.get(url, timeout=30)
                soup_art = BeautifulSoup(r.text, "html.parser")
                news_div = soup_art.find("div", class_="News")
                if news_div:
                    for el in news_div.find_all(["nav", "script", "style"]):
                        el.decompose()
                    raw_html = str(news_div)
                    text = news_div.get_text("\n", strip=True)
                    m_ts = re.search(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}\s+ET", text)
                    if m_ts:
                        text = text[m_ts.start():]
                    return date_key, {"html": raw_html, "text": text, "ok": True}
                else:
                    return date_key, {"html": "", "text": "", "ok": False}
            except Exception as e:
                log.debug(f"    Article fetch error {url}: {e}")
                return date_key, {"html": "", "text": "", "ok": False}

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(_fetch_article, (dk, item)): dk
                       for dk, item in news_by_date.items() if item["article_url"]}
            for future in as_completed(futures):
                date_key, result = future.result()
                if result:
                    news_by_date[date_key]["html"] = result["html"]
                    news_by_date[date_key]["text"] = result["text"]
                    if result["ok"]:
                        fetched += 1
                    else:
                        failed += 1

        log.info(f"    Text: {fetched} fetched, {failed} failed")
        return news_by_date

    def find_aif(self, symbol: str, search_back_years: int = 3) -> dict | None:
        """Find the most recent AIF for a symbol."""
        to_date   = date.today()
        from_date = date(to_date.year - search_back_years, 1, 1)
        html      = self._post_search(symbol, from_date, to_date, "annual information form")
        filings   = self._parse_results(html)
        aifs      = [f for f in filings if "ANNUAL_INFORMATION_FORM" in f["doc_type"].upper()
                                        and "_EN" in f["doc_type"].upper()]
        if not aifs:
            return None
        # Most recent first (results sorted newest-first)
        return aifs[0]


# ---------------------------------------------------------------------------
# PDF download (Stockwatch PDFs are public - no auth needed)
# ---------------------------------------------------------------------------
def download_stockwatch_pdf(pdf_url: str, dest_path: Path,
                             session: requests.Session,
                             symbol: str = "") -> tuple[bool, int, str]:
    """Download PDF, upload to R2. Returns (ok, page_count, r2_url)."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        r = session.get(pdf_url, timeout=60, stream=False)
        if r.status_code != 200:
            log.warning(f"  PDF download {r.status_code}: {pdf_url}")
            return False, 0, ""
        if r.content[:4] != b"%PDF":
            log.warning(f"  Not a PDF: {pdf_url}")
            return False, 0, ""
        dest_path.write_bytes(r.content)
        page_count = 0
        try:
            import fitz
            with fitz.open(stream=r.content, filetype="pdf") as doc:
                page_count = doc.page_count
        except Exception:
            pass
        size_kb = len(r.content) // 1024
        r2_url = upload_to_r2(dest_path, symbol) if symbol else ""
        log.info(f"    -> {size_kb}KB, {page_count}p" + (" R2" if r2_url else ""))
        return True, page_count, r2_url
    except Exception as e:
        log.warning(f"  PDF download error: {e}")
        return False, 0, ""


# ---------------------------------------------------------------------------
# SEDAR+ gap fill (last 14 days)
# ---------------------------------------------------------------------------
def sedar_gap_fill(symbol: str, company_name: str, party_number: str,
                   gap_from: date, gap_to: date) -> list[dict]:
    """
    Fetch relevant filings from SEDAR+ for the gap period (last ~14 days).
    Uses our proven chip+search approach from Universe Builder.
    Returns list of filing dicts.
    """
    from playwright.sync_api import sync_playwright
    import base64

    # Filing types relevant for MRE tracking
    RELEVANT_TYPES = [
        ("TECHNICAL_REPORTS_NI_43101",  "NI43-101"),
        ("MATERIAL_CHANGE_REPORT",       "MaterialChange"),
        ("SHORT_FORM_PROSPECTUS_NI_44101", "Prospectus"),
        ("LONG_FORM_PROSPECTUS",          "Prospectus"),
        ("BUSINESS_ACQUISITION_REPORT",   "Acquisition"),
        ("FILING_STATEMENT",              "Acquisition"),
        ("NEWS_RELEASES",                 "NewsRelease"),
    ]

    results = []
    pw = browser = page = None
    try:
        if not ensure_cdp_ready():
            log.warning(f"  SEDAR gap fill: CDP not available for {symbol} - skipping")
            return []
        pw      = sync_playwright().start()
        browser = pw.chromium.connect_over_cdp(CDP_URL)
        ctx     = browser.contexts[0]
        page    = ctx.new_page()

        # Verify SEDAR+ is accessible (public portal, no login required)
        page.goto(f"{SEDAR_BASE}/csa-party/", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(1000)
        page_text = page.inner_text("body")
        # Check for maintenance or error pages
        is_unavailable = (
            "maintenance" in page_text.lower()
            or "unavailable" in page_text.lower()
            or "503" in page_text
            or "502" in page_text
        )
        if is_unavailable:
            log.error(f"  SEDAR gap fill: SEDAR+ appears unavailable (maintenance?) - skipping")
            page.close()
            return []
        log.info("  SEDAR+ accessible")

        # Navigate + apply chip
        page.goto(
            f"{SEDAR_BASE}/csa-party/service/create.html"
            "?targetAppCode=csa-party&service=searchDocuments&_locale=en",
            wait_until="domcontentloaded", timeout=30000
        )
        try:
            page.wait_for_selector("#SubmissionDate", timeout=20000)
        except Exception:
            log.warning(f"  SEDAR gap fill: search form not ready for {symbol} - skipping")
            return []

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
            log.warning(f"  SEDAR gap fill: chip trigger failed for {symbol}")
            return []
        page.wait_for_selector("#SubmissionDate", timeout=10000)
        page.wait_for_timeout(500)
        log.info(f"  SEDAR gap fill: chip applied for {symbol}")

        # Extract cookies for requests-based PDF download
        import requests as _req
        req_session = _req.Session()
        for c in page.context.cookies():
            req_session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))
        req_session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/pdf,*/*",
            "Referer": "https://www.sedarplus.ca/",
        })

        from_str = gap_from.strftime("%d/%m/%Y")
        to_str   = gap_to.strftime("%d/%m/%Y")

        for sedar_type, category in RELEVANT_TYPES:
            try:
                # Set FilingType + wait for XHR
                page.evaluate(f"""() => {{
                    window._done = false;
                    const o = XMLHttpRequest.prototype.open;
                    XMLHttpRequest.prototype.open = function(m, url) {{
                        this._url = url;
                        this.addEventListener('load', ()=>{{ if(this._url?.includes('update.html')) window._done=true; }});
                        o.apply(this, arguments);
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

                # Set dates + Search
                page.evaluate(f"""() => {{
                    document.getElementById('SubmissionDate').value  = '{from_str}';
                    document.getElementById('SubmissionDate2').value = '{to_str}';
                    window._done = false;
                    const o = XMLHttpRequest.prototype.open;
                    XMLHttpRequest.prototype.open = function(m, url) {{
                        this._url = url;
                        this.addEventListener('load', ()=>{{ if(this._url?.includes('update.html')) window._done=true; }});
                        o.apply(this, arguments);
                    }};
                    Array.from(document.querySelectorAll('button')).find(b=>b.textContent.trim()==='Search')?.click();
                }}""")
                try: page.wait_for_function("()=>window._done", timeout=10000)
                except: pass
                page.wait_for_timeout(300)

                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                tbl  = soup.find("table", attrs={"aria-label": "List of data items"})
                if not tbl:
                    continue

                tbl_rows = (tbl.find("tbody") or tbl).find_all("tr")
                log.info(f"  SEDAR gap {sedar_type}: {len(tbl_rows)} rows")

                for tr in tbl_rows:
                    cells = tr.find_all("td")
                    if len(cells) < 4:
                        continue
                    doc_name  = cells[2].get_text(" ", strip=True)
                    if "french" in doc_name.lower() or " - fr" in doc_name.lower():
                        continue  # skip French
                    date_text = cells[3].get_text(" ", strip=True)
                    date_m    = re.search(r"(\d{1,2}\s+\w{3}\s+\d{4})", date_text)
                    filing_date = date_m.group(1) if date_m else ""

                    doc_link = cells[2].find("a", href=re.compile(r"resource\.html"))
                    if not doc_link:
                        continue
                    href = doc_link["href"]
                    if href.startswith("/"):
                        href = f"{SEDAR_BASE}{href}"
                    view_id_m = re.search(r"id=([a-f0-9]+)", page.url)
                    if view_id_m:
                        href = re.sub(r"[&?]id=[^&]*", "", href) + f"&id={view_id_m.group(1)}"

                    # Download PDF via requests - skip NewsReleases entirely,
                    # Stockwatch /News/Search covers these up to today with full HTML
                    gap_r2 = ""
                    pdf_bytes = None
                    if category != "NewsRelease":
                        try:
                            r = req_session.get(href, timeout=60)
                            if r.status_code == 200 and r.content[:4] == b"%PDF":
                                pdf_bytes = r.content
                        except Exception:
                            pass

                    pdf_url  = href
                    pdf_path = ""
                    page_count = 0

                    if pdf_bytes and category != "NewsRelease":
                        try:
                            dt_iso = datetime.strptime(filing_date.strip(), "%d %b %Y").strftime("%Y-%m-%d") if filing_date else "unknown"
                        except Exception:
                            dt_iso = "unknown"
                        safe_name = re.sub(r"[^\w\s-]", "", doc_name)[:50].strip()
                        pdf_filename = f"{dt_iso}_{symbol}_{category}_{safe_name}.pdf"
                        pdf_dest = RESULTS_DIR / symbol / "pdfs" / category / pdf_filename
                        pdf_dest.parent.mkdir(parents=True, exist_ok=True)
                        pdf_dest.write_bytes(pdf_bytes)
                        try:
                            import fitz
                            with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
                                page_count = doc.page_count
                        except Exception:
                            pass
                        pdf_path = str(pdf_dest)
                        size_kb  = len(pdf_bytes) // 1024
                        gap_r2   = upload_to_r2(pdf_dest, symbol)
                        log.info(f"    DL {symbol} {category} {dt_iso} {size_kb}KB {page_count}p")

                    # Normalise filing_date to ISO format (SEDAR+ returns "27 Mar 2026")
                    try:
                        filing_date_iso = datetime.strptime(filing_date.strip(), "%d %b %Y").strftime("%Y-%m-%d") if filing_date else filing_date
                    except Exception:
                        filing_date_iso = filing_date

                    results.append({
                        "source":       "sedar_gap",
                        "r2_url":       gap_r2 if pdf_bytes else "",
                        "symbol":       symbol,
                        "issuer":       company_name,
                        "filing_date":  filing_date_iso,
                        "doc_type":     sedar_type,
                        "industry":     "",
                        "pdf_url":      pdf_url,
                        "synopsis":     doc_name,
                        "category":     category,
                        "pdf_path":     pdf_path,
                        "page_count":   page_count,
                        "pdf_size_kb":  len(pdf_bytes) // 1024 if pdf_bytes else 0,
                        "downloaded":   "yes" if (pdf_bytes and category != "NewsRelease") else "no",
                    })

            except Exception as e:
                log.warning(f"  SEDAR gap {sedar_type} error: {e}")
                continue

    except Exception as e:
        log.error(f"  SEDAR gap fill failed for {symbol}: {e}")
    finally:
        try:
            if page: page.close()
        except Exception: pass
        try:
            if browser: browser.close()
        except Exception: pass
        try:
            if pw: pw.stop()
        except Exception: pass

    return results


# ---------------------------------------------------------------------------
# Main onboarding function for one company
# ---------------------------------------------------------------------------
def onboard_company(symbol: str, company_name: str, exchange: str,
                    party_number: str, sw_session: StockwatchSedarSession,
                    req_session: requests.Session,
                    sw_symbol: str = "") -> dict:
    """
    Full onboarding pipeline for one company. Auto-detects first run vs update.
    - First run: full history from AIF as-at date to today
    - Update run: gap fill from last run date to today, check for new AIF
    sw_symbol: Stockwatch ticker if different from the canonical symbol (e.g. old symbol
               still used by Stockwatch). Falls back to symbol if blank.
    Returns summary dict.
    """
    # Use sw_symbol for all Stockwatch queries if provided, otherwise fall back to symbol
    sw_sym = sw_symbol.strip().upper() if sw_symbol and sw_symbol.strip() else symbol
    if sw_sym != symbol:
        log.info(f"  Stockwatch symbol override: using '{sw_sym}' (canonical: '{symbol}')")
    company_dir = RESULTS_DIR / symbol
    company_dir.mkdir(parents=True, exist_ok=True)
    pdfs_dir = company_dir / "pdfs"

    # Attach per-company realtime log (Results/{SYMBOL}/run.log)
    company_log_handler = attach_company_log(symbol)

    log.info("=" * 60)
    log.info(f"ONBOARDING: {symbol} - {company_name} ({exchange})")
    log.info(f"Started: {datetime.now(TORONTO_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    log.info("=" * 60)

    # Check for previous run state
    prev_state  = load_company_state(symbol)
    is_update   = bool(prev_state.get("last_run_date"))
    run_mode    = "UPDATE" if is_update else "FULL"
    log.info(f"  Mode: {run_mode}" + (f" (last run: {prev_state['last_run_date']})" if is_update else " (first run)"))

    all_filings = []

    # ------------------------------------------------------------------
    log.info(f"  STAGE 1/6: AIF lookup & download")
    log.info(f"  {'â”€'*50}")
    log.info(f"  Finding most recent AIF for {sw_sym}" + (f" (canonical: {symbol})" if sw_sym != symbol else ""))
    aif_info = sw_session.find_aif(sw_sym)

    if aif_info:
        try:
            aif_date = datetime.strptime(aif_info["filing_date"], "%Y-%m-%d").date()
        except Exception:
            aif_date = date.today() - timedelta(days=365)
        as_at_date = infer_as_at_date(aif_date)
        log.info(f"  AIF filed: {aif_date} | inferred as-at: {as_at_date}")

        # In update mode: skip if same AIF as last run (already downloaded)
        prev_aif = prev_state.get("aif_filing_date", "")
        aif_is_new = str(aif_date) != prev_aif
        if is_update and not aif_is_new:
            log.info(f"  AIF unchanged since last run ({aif_date}) - skipping download")
            # Re-upload to R2 if local file exists (ensures r2_url is always set)
            existing_pdf = pdfs_dir / "AIF" / f"{aif_date}_{symbol}_AIF.pdf"
            if existing_pdf.exists():
                r2 = upload_to_r2(existing_pdf, symbol)
                if r2:
                    aif_info["r2_url"]   = r2
                    aif_info["pdf_path"] = str(existing_pdf)
                    aif_info["downloaded"] = "yes"
        elif aif_info.get("pdf_url") and aif_is_new:
            log.info(f"  {'New AIF found' if is_update else 'Downloading AIF'}: {aif_date}")

        # Download the AIF PDF now (if new or first run)
        if aif_info.get("pdf_url") and (not is_update or aif_is_new):
            pdf_filename = f"{aif_date}_{symbol}_AIF.pdf"
            pdf_dest = pdfs_dir / "AIF" / pdf_filename
            pdf_dest.parent.mkdir(parents=True, exist_ok=True)
            if pdf_dest.exists():
                log.info(f"  AIF already downloaded: {pdf_filename}")
                r2 = upload_to_r2(pdf_dest, symbol)
                if r2:
                    aif_info["r2_url"] = r2
            else:
                log.info(f"  Downloading AIF: {pdf_filename}")
                ok, pages, r2 = download_stockwatch_pdf(aif_info["pdf_url"], pdf_dest, req_session, symbol)
                if ok:
                    size_kb = pdf_dest.stat().st_size // 1024
                    log.info(f"  AIF: {size_kb}KB, {pages}p")
                    aif_info["pdf_path"]   = str(pdf_dest)
                    aif_info["page_count"] = pages
                    aif_info["pdf_size_kb"]= size_kb
                    aif_info["downloaded"] = "yes"
                    aif_info["r2_url"]     = r2
                else:
                    log.warning(f"  AIF download failed")

        # Add AIF to filings list
        aif_info.setdefault("source",      "stockwatch")
        aif_info.setdefault("symbol",      symbol)
        aif_info.setdefault("issuer",      company_name)
        aif_info.setdefault("doc_type",    "ANNUAL_INFORMATION_FORM_EN")
        aif_info.setdefault("industry",    "")
        aif_info.setdefault("category",    "AIF")
        aif_info.setdefault("synopsis",    f"Annual Information Form - {aif_date}")
        aif_info.setdefault("pdf_path",    "")
        aif_info.setdefault("page_count",  0)
        aif_info.setdefault("pdf_size_kb", 0)
        aif_info.setdefault("downloaded",  "no")
        aif_info["as_at_date"] = str(as_at_date)
        aif_info["aif_filed"]  = str(aif_date)
        all_filings.append(aif_info)
        
        # Extract real as_at_date from AIF PDF using LLM (if downloaded)
        aif_pdf_path = aif_info.get("pdf_path", "")
        if aif_pdf_path and os.path.exists(aif_pdf_path):
            log.info(f"  Extracting as_at_date from AIF PDF...")
            try:
                llm_as_at = extract_aif_as_at_date(aif_pdf_path, company_name, as_at_date)
                if llm_as_at:
                    as_at_date = llm_as_at
                    # Update the AIF row with corrected date
                    aif_info["as_at_date"] = str(as_at_date)
                    # Update all_filings rows that reference as_at_date
                    for f in all_filings:
                        f["as_at_date"] = str(as_at_date)
            except Exception as e:
                log.error(f"  AIF as_at_date extraction error: {e}")
    else:
        log.warning(f"  No AIF found for {symbol} - defaulting to 2 years ago")
        aif_date   = date.today() - timedelta(days=730)
        as_at_date = infer_as_at_date(aif_date)

    if is_update:
        # Update mode: only fetch since last run date (with 1-day overlap for safety)
        last_run = datetime.strptime(prev_state["last_run_date"], "%Y-%m-%d").date()
        sw_from  = last_run - timedelta(days=1)
        log.info(f"  Update: fetching filings since {sw_from}")
    else:
        # Full run: fetch from AIF as-at date
        sw_from = as_at_date + timedelta(days=1)

    sw_to    = date.today() - timedelta(days=14)
    gap_from = date.today() - timedelta(days=14)
    gap_to   = date.today()

    # Load existing filings to avoid duplicates (update mode)
    existing_keys: set = set()
    if is_update:
        csv_path = company_dir / "filings_log.csv"
        if csv_path.exists():
            with open(csv_path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    # Key by (filing_date, doc_type, source)
                    existing_keys.add((row.get("filing_date",""), row.get("doc_type",""), row.get("source","")))
            log.info(f"  Loaded {len(existing_keys)} existing filing keys to avoid duplicates")

    # ------------------------------------------------------------------
    log.info(f"  STAGE 2/6: Stockwatch SEDAR search")
    log.info(f"  {'â”€'*50}")
    log.info(f"  Date range: {sw_from} â†’ {sw_to}")
    sw_filings = sw_session.search(sw_sym, sw_from, sw_to)
    log.info(f"  Stockwatch: {len(sw_filings)} filings found")

    # Process each Stockwatch filing
    for f in sw_filings:
        f["symbol"]     = symbol  # ensure correct symbol
        f["pdf_path"]   = ""
        f["page_count"] = 0
        f["pdf_size_kb"]= 0
        f["downloaded"] = "no"
        f["as_at_date"] = str(as_at_date)
        f["aif_filed"]  = str(aif_date)

        category = f.get("category", "Other")

        # Download relevant PDFs (not news releases, not NI43-101 consents/certs)
        if _should_download(category, f.get("doc_type", ""), f.get("synopsis", "")) and f.get("pdf_url"):
            try:
                filing_date_str = f.get("filing_date", "unknown").replace("-", "")
                safe_doc = re.sub(r"[^\w\s-]", "", f.get("doc_type", ""))[:30].strip()
                pdf_filename = f"{f.get('filing_date','unknown')}_{symbol}_{category}_{safe_doc}.pdf"
                pdf_dest = pdfs_dir / category / pdf_filename
                pdf_dest.parent.mkdir(parents=True, exist_ok=True)

                if pdf_dest.exists():
                    log.info(f"    SKIP (exists): {pdf_filename}")
                    f["downloaded"] = "exists"
                    f["pdf_path"]   = str(pdf_dest)
                    # Upload to R2 if not already done
                    r2 = upload_to_r2(pdf_dest, symbol)
                    if r2:
                        f["r2_url"] = r2
                else:
                    log.info(f"    DL {category}: {pdf_filename}")
                    ok, pages, r2 = download_stockwatch_pdf(f["pdf_url"], pdf_dest, req_session, symbol)
                    if ok:
                        f["downloaded"]  = "yes"
                        f["pdf_path"]    = str(pdf_dest)
                        f["page_count"]  = pages
                        f["pdf_size_kb"] = pdf_dest.stat().st_size // 1024
                        f["r2_url"]      = r2
                    else:
                        f["downloaded"]  = "failed"
            except Exception as e:
                log.warning(f"    PDF error for {symbol}: {e}")

    # Deduplicate within sw_filings:
    # - NewsRelease: deduplicate by (filing_date, doc_type, synopsis) - same release filed with different pdf_url
    # - Other types: deduplicate by (filing_date, doc_type, pdf_url)
    seen_keys: set = set()
    deduped = []
    for f in sw_filings:
        if f.get("category") == "NewsRelease":
            fkey = (f.get("filing_date",""), f.get("doc_type",""), f.get("synopsis",""))
        else:
            fkey = (f.get("filing_date",""), f.get("doc_type",""), f.get("pdf_url",""))
        if fkey in seen_keys:
            continue
        seen_keys.add(fkey)
        deduped.append(f)
    if len(deduped) < len(sw_filings):
        log.info(f"  Deduped {len(sw_filings) - len(deduped)} duplicate Stockwatch filings")
    sw_filings = deduped

    # Deduplicate Stockwatch filings in update mode
    if is_update and existing_keys:
        before = len(sw_filings)
        sw_filings = [f for f in sw_filings
                      if (f.get("filing_date",""), f.get("doc_type",""), f.get("source","")) not in existing_keys]
        if before != len(sw_filings):
            log.info(f"  Deduplicated: {before - len(sw_filings)} existing filings skipped")
    all_filings.extend(sw_filings)

    # ------------------------------------------------------------------
    log.info(f"  STAGE 3/6: SEDAR+ gap fill (last 14 days)")
    log.info(f"  {'â”€'*50}")
    if party_number:
        log.info(f"  Step 3: SEDAR+ gap fill {gap_from} â†’ {gap_to}")
        gap_filings = sedar_gap_fill(symbol, company_name, party_number, gap_from, gap_to)
        for f in gap_filings:
            f["as_at_date"] = str(as_at_date)
            f["aif_filed"]  = str(aif_date)
        # Deduplicate gap filings too
        if is_update and existing_keys:
            gap_filings = [f for f in gap_filings
                           if (f.get("filing_date",""), f.get("doc_type",""), f.get("source","")) not in existing_keys]
        log.info(f"  SEDAR+ gap: {len(gap_filings)} filings")
        all_filings.extend(gap_filings)
    else:
        log.warning(f"  Step 3: No party number for {symbol} - skipping SEDAR+ gap fill")

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    log.info(f"  STAGE 4/6: News release text fetch")
    log.info(f"  {'â”€'*50}")
    # Fetch news release text from Stockwatch /News/Search
    # Match by date. Only Stockwatch (older) news - SEDAR+ gap news
    # already has PDF text available from the downloaded PDF.
    # ------------------------------------------------------------------
    # Include sedar_gap NewsRelease entries too - Stockwatch /News/Search covers current day
    # so gap news releases should also be matchable via the same text fetch
    all_news = [f for f in all_filings if f.get("category") == "NewsRelease"]

    if all_news:
        log.info(f"  Step 4: Fetching text for {len(all_news)} news releases via /News/Search")
        # /News/Search has news up to today (no 14-day lag like /News/Sedar)
        news_text_map = sw_session.fetch_news_text_for_symbol(sw_sym, sw_from, date.today())
        html_dir = company_dir / "news_html"
        html_dir.mkdir(parents=True, exist_ok=True)
        matched = 0
        for f in all_filings:
            f.setdefault("news_text",        "")
            f.setdefault("news_html_path",   "")
            f.setdefault("news_html_r2_url", "")
            f.setdefault("article_url",      "")
            f.setdefault("article_id",       "")
            if f.get("category") == "NewsRelease":
                date_key = f.get("filing_date", "")[:10]
                # For sedar_gap entries, also try the previous day to handle ET/UTC timezone
                # differences (SEDAR+ timestamps in UTC, Stockwatch in ET)
                alt_key = ""
                if f.get("source") == "sedar_gap" and date_key:
                    try:
                        from datetime import timedelta as _td
                        alt_key = (date.fromisoformat(date_key) - _td(days=1)).isoformat()
                    except Exception:
                        pass
                matched_key = date_key if date_key in news_text_map else (alt_key if alt_key and alt_key in news_text_map else None)
                if matched_key:
                    item = news_text_map[matched_key]
                    f["news_text"]   = item.get("text", "")
                    f["article_url"] = item.get("article_url", "")
                    f["article_id"]  = item.get("article_id", "")
                    # Store headline in synopsis, overwriting generic SEDAR+ doc names
                    existing = (f.get("synopsis") or "").strip()
                    synopsis_is_generic = (not existing
                                           or re.search(r'^news release', existing, re.I)
                                           or existing.endswith('.pdf'))
                    if synopsis_is_generic:
                        f["synopsis"] = item.get("headline", "")
                    # Upgrade sedar_gap source to stockwatch if we got a match
                    if f.get("source") == "sedar_gap":
                        f["source"] = "stockwatch"
                    # Save HTML file, upload to R2 for permanent public link
                    raw_html = item.get("html", "")
                    if raw_html:
                        html_path = html_dir / f"{date_key}.html"
                        html_path.write_text(raw_html, encoding="utf-8")
                        f["news_html_path"]   = str(html_path)
                        f["news_html_r2_url"] = upload_html_to_r2(html_path, symbol)
                    matched += 1
        log.info(f"  News text: {matched}/{len(all_news)} matched")

        # PDF fallback: for any NewsRelease still missing text, download its
        # Sedardoc PDF and extract plain text for the LLM
        no_text = [f for f in all_filings
                   if f.get("category") == "NewsRelease" and not f.get("news_text") and f.get("pdf_url")]
        if no_text:
            log.info(f"  PDF fallback: {len(no_text)} news releases with no text - attempting PDF extraction")
            for f in no_text:
                try:
                    r = req_session.get(f["pdf_url"], timeout=60)
                    if r.status_code == 200 and r.content[:4] == b"%PDF":
                        import fitz as _fitz
                        with _fitz.open(stream=r.content, filetype="pdf") as doc:
                            pages_text = "\n".join(page.get_text() for page in doc)
                        # Strip non-ASCII to avoid encoding issues
                        pages_text = pages_text.encode("ascii", "ignore").decode("ascii")
                        f["news_text"] = pages_text[:15000]
                        # Extract headline from PDF text - collect consecutive ALL-CAPS lines
                        # that form the headline, stopping at known legal/boilerplate openers
                        if not f.get("synopsis"):
                            skip_line = re.compile(
                                r'^(news release|tsx:|tsxv:|otcqx:|otcqb:|cse:|for immediate release|page \d)',
                                re.I)
                            stop_line = re.compile(
                                r'^(not for distribution|not for release|the shelf|cautionary|forward.looking|'
                                r'this press release|this news release|neither this|neither the|'
                                r'no securities|legal notice|disclaimer)',
                                re.I)
                            headline_parts = []
                            collecting = False
                            for line in pages_text.splitlines():
                                line = line.strip()
                                if not line:
                                    # blank line after we started = done
                                    if collecting:
                                        break
                                    continue
                                is_caps = line == line.upper() and re.search(r'[A-Z]', line)
                                if stop_line.match(line):
                                    break
                                if skip_line.match(line):
                                    continue
                                if is_caps and len(line) > 15 and not re.match(r'^[\W\d]+$', line):
                                    headline_parts.append(line)
                                    collecting = True
                                elif collecting:
                                    # non-caps line after headline started = done
                                    break
                            if headline_parts:
                                f["synopsis"] = " ".join(headline_parts)[:200]
                            else:
                                # Fallback: no ALL-CAPS headline - collect first paragraph
                                # of substantial non-boilerplate lines (handles mixed-case PDFs)
                                fallback_parts = []
                                for line in pages_text.splitlines():
                                    line = line.strip()
                                    if not line:
                                        if fallback_parts: break
                                        continue
                                    if stop_line.match(line): break
                                    if skip_line.match(line): continue
                                    if re.search(r'[A-Za-z]', line) and not re.match(r'^[\W\d]+$', line):
                                        if not fallback_parts and len(line) < 20:
                                            continue  # skip very short lead lines
                                        fallback_parts.append(line)
                                        if len(" ".join(fallback_parts)) > 120:
                                            break
                                if fallback_parts:
                                    f["synopsis"] = " ".join(fallback_parts)[:200]
                        # Save PDF locally and upload to R2 for permanent link
                        date_key = f.get("filing_date", "unknown")[:10]
                        pdf_dest = company_dir / "pdfs" / "NewsRelease" / f"{date_key}_{symbol}_NewsRelease_fallback.pdf"
                        pdf_dest.parent.mkdir(parents=True, exist_ok=True)
                        pdf_dest.write_bytes(r.content)
                        f["pdf_path"] = str(pdf_dest)
                        r2 = upload_to_r2(pdf_dest, symbol)
                        if r2:
                            f["r2_url"] = r2
                        log.info(f"    PDF text extracted: {date_key} ({len(f['news_text'])} chars) R2={bool(r2)}")
                    else:
                        log.debug(f"    PDF fetch failed: {f['pdf_url']} status={r.status_code}")
                except Exception as e:
                    log.debug(f"    PDF fallback error {f['pdf_url']}: {e}")
    else:
        for f in all_filings:
            f.setdefault("news_text",        "")
            f.setdefault("news_html_path",   "")
            f.setdefault("news_html_r2_url", "")
            f.setdefault("article_url",      "")
            f.setdefault("article_id",       "")

    # ------------------------------------------------------------------
    # Material change PDF classification (parallel, brief description)
    for f in all_filings:
        f.setdefault("mat_summary", "")
    _mat_cfg   = {}
    try:
        import tomllib as _toml2
        with open(SCRIPT_DIR / "config.toml", "rb") as _f2:
            _mat_cfg = _toml2.load(_f2)
    except Exception:
        pass
    _mat_model   = _mat_cfg.get("llm", {}).get("model", "gpt-4o-mini")
    _mat_workers = _mat_cfg.get("llm", {}).get("max_workers", 20)
    all_filings = run_material_change_classification(symbol, all_filings, _mat_model, min(_mat_workers, 20))

    # ------------------------------------------------------------------
    log.info(f"  STAGE 5/6: LLM classification of news releases")
    log.info(f"  {'â”€'*50}")
    _cfg       = {}
    try:
        import tomllib as _toml
        with open(SCRIPT_DIR / "config.toml", "rb") as _f:
            _cfg = _toml.load(_f)
    except Exception:
        pass
    llm_model   = _cfg.get("llm", {}).get("model", "gpt-4o-mini")
    llm_workers = _cfg.get("llm", {}).get("max_workers", 80)
    all_filings = run_llm_classification(symbol, all_filings, llm_model, llm_workers)

    # ------------------------------------------------------------------
    # Save per-company CSV
    # Update mode: APPEND new rows to existing CSV
    # Full mode: write fresh CSV
    # ------------------------------------------------------------------
    csv_path = company_dir / "filings_log.csv"
    fieldnames = [
        "source", "symbol", "issuer", "filing_date", "doc_type", "industry",
        "category", "synopsis", "article_url", "article_id",
        "pdf_url", "pdf_path", "page_count", "pdf_size_kb",
        "downloaded", "r2_url", "news_text", "news_html_path", "news_html_r2_url", "mat_summary",
        "llm_flag", "llm_summary", "llm_project", "llm_error",
        "as_at_date", "aif_filed",
    ]
    # Write CSV with retry (handles case where file is open in Excel)
    for _attempt in range(3):
        try:
            if is_update and csv_path.exists() and all_filings:
                # UPDATE with new rows: append only
                with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                    writer.writerows(all_filings)
                log.info(f"  Appended {len(all_filings)} new rows to existing CSV")
            elif is_update and not all_filings:
                # UPDATE with no new rows: leave existing CSV untouched
                log.info(f"  No new filings - existing CSV preserved ({csv_path.name})")
            else:
                # FULL run: write fresh CSV
                tmp = csv_path.with_suffix(".tmp")
                with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(all_filings)
                tmp.replace(csv_path)
            break
        except PermissionError:
            if _attempt < 2:
                log.warning(f"  CSV locked (attempt {_attempt+1}/3) - retrying in 5s...")
                time.sleep(5)
            else:
                log.error(f"  CSV write failed after 3 attempts - file may be open in Excel")

    # Save run state
    now_str = datetime.now(TORONTO_TZ).strftime("%Y-%m-%dT%H:%M:%S")
    state = {
        "last_run":          now_str,
        "last_run_date":     date.today().isoformat(),
        "run_mode":          run_mode,
        "aif_filing_date":   str(aif_date),
        "as_at_date":        str(as_at_date),
        "sw_covered_to":     str(sw_to),
        "new_filings_this_run": len(all_filings),
    }

    # Preserve existing presentation data on UPDATE runs; scan on FULL or if missing
    if is_update and prev_state.get("presentation_url"):
        state["presentation_url"]     = prev_state["presentation_url"]
        state["presentation_local"]   = prev_state.get("presentation_local", "")
        state["presentation_size_kb"] = prev_state.get("presentation_size_kb", 0)
        log.info(f"  Presentation: preserved from previous run")
    else:
        log.info(f"  STAGE 6/6: Presentation scan")
        pres = find_presentation_phase(symbol)
        if pres:
            state.update(pres)

    save_company_state(symbol, state)
    log.info(f"  State saved: last_run={now_str}, aif={aif_date}")

    # Summary
    downloaded  = sum(1 for f in all_filings if f.get("downloaded") == "yes")
    relevant    = sum(1 for f in all_filings if f.get("category") in DOWNLOAD_CATEGORIES)
    news        = sum(1 for f in all_filings if f.get("category") == "NewsRelease")
    total       = len(all_filings)

    log.info("=" * 60)
    log.info(f"  COMPLETE: {symbol} {run_mode}")
    log.info(f"  {'â”€'*50}")
    log.info(f"  New filings:     {total}")
    log.info(f"  PDFs downloaded: {downloaded} ({relevant} relevant types)")
    log.info(f"  News releases:   {news} (text fetched)")
    log.info(f"  CSV:             {csv_path.name}")
    log.info(f"  Log:             run.log")
    log.info(f"  Finished: {datetime.now(TORONTO_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    log.info("=" * 60)

    detach_company_log(company_log_handler)

    return {
        "symbol":       symbol,
        "company_name": company_name,
        "run_mode":     run_mode,
        "aif_filed":    str(aif_date),
        "as_at_date":   str(as_at_date),
        "total":        total,
        "relevant":     relevant,
        "downloaded":   downloaded,
        "news":         news,
        "csv_path":     str(csv_path),
    }


# ---------------------------------------------------------------------------
# Presentation finder phase
# ---------------------------------------------------------------------------
def _extract_presentation_date(pdf_path: Path) -> str:
    """
    Extract presentation date from first 1-2 pages of a PDF.
    Returns human string like 'April 2026', 'Q1 2026', or '' if not found.
    """
    MONTHS_EN = ['January','February','March','April','May','June',
                 'July','August','September','October','November','December']
    MONTHS_FR = ['janvier','fevrier','mars','avril','mai','juin',
                 'juillet','aout','septembre','octobre','novembre','decembre']

    text = ''
    try:
        import fitz
        doc = fitz.open(str(pdf_path))
        for i in range(min(2, len(doc))):
            text += doc[i].get_text()
        doc.close()
    except Exception:
        try:
            import pdfplumber
            with pdfplumber.open(str(pdf_path)) as pdf:
                for i in range(min(2, len(pdf.pages))):
                    text += pdf.pages[i].extract_text() or ''
        except Exception:
            return ''

    # Try on raw text and on version with single-char spaces collapsed
    # (handles PDFs that render each char separately: "A P R I L  2 0 2 6")
    collapsed = re.sub(r'(?<=\S) (?=\S)', '', text[:3000])

    for t in [text[:3000], collapsed]:
        # French months
        for i, mfr in enumerate(MONTHS_FR):
            m = re.search(rf'\b{mfr}\.?\s+(20\d{{2}})\b', t, re.IGNORECASE)
            if m:
                return MONTHS_EN[i] + ' ' + m.group(1)
        # English months
        for mn in MONTHS_EN:
            m = re.search(rf'\b{mn[:3]}[a-z]*\.?[,\s\d]{{0,5}}(20\d{{2}})\b', t, re.IGNORECASE)
            if m:
                return mn + ' ' + m.group(1)
        # Quarter
        m = re.search(r'\b(Q[1-4])\s*(20\d{2})\b', t, re.IGNORECASE)
        if m:
            return m.group(1).upper() + ' ' + m.group(2)
        # "First/Second/Third/Fourth Quarter YYYY"
        for word, q in [('first','Q1'),('second','Q2'),('third','Q3'),('fourth','Q4')]:
            m = re.search(rf'\b{word}\s+quarter,?\s*(20\d{{2}})\b', t, re.IGNORECASE)
            if m:
                return q + ' ' + m.group(1)

    return ''


def find_presentation_phase(symbol: str) -> dict:
    """
    Scan the company website for a corporate presentation PDF.
    Returns dict with presentation_url, presentation_local, presentation_size_kb
    or empty dict if nothing found.
    Preserves any existing presentation in state (won't overwrite with nothing).
    """
    # Get website from universe
    universe = load_universe_lookup()
    website = universe.get(symbol, {}).get("website", "").strip()
    if not website or not website.startswith("http"):
        log.info(f"  {symbol}: no website in universe - skipping presentation scan")
        return {}

    log.info(f"  {symbol}: scanning {website} for presentation")

    # Presentation keywords / negatives
    pres_keywords = ["presentation", "corporate", "investor", "deck", "pitch"]
    pres_negative = [
        "mineral-resource-estimate", "mineral_resource_estimate",
        "terms-and-conditions", "purchase-order", "sustainability-report",
        "annual-report", "financial-results", "human-rights", "form-of-proxy",
        "proxy", "mda", "news-release", "circular", "policy", "nr-20",
    ]
    min_size_kb = 500
    timeout = 15

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    # Paths to probe: dedicated IR paths first, then sitemap, then homepage
    from urllib.parse import urljoin as _urljoin
    import xml.etree.ElementTree as _ET

    pdf_candidates = []  # list of (url, score)

    # --- Sitemap ---
    try:
        for sm_path in ["/sitemap.xml", "/sitemap_index.xml"]:
            sm_resp = session.get(_urljoin(website, sm_path), timeout=timeout)
            if sm_resp.status_code != 200:
                continue
            # Parse XML
            try:
                root = _ET.fromstring(sm_resp.content)
                ns = root.tag.split("}")[0].lstrip("{") if "}" in root.tag else ""
                ns_prefix = f"{{{ns}}}" if ns else ""
                for loc in root.iter(f"{ns_prefix}loc"):
                    url = (loc.text or "").strip()
                    if url.lower().endswith(".pdf"):
                        url_lower = url.lower()
                        score = sum(20 for kw in pres_keywords if kw in url_lower)
                        if score > 0 and not any(neg in url_lower for neg in pres_negative):
                            pdf_candidates.append((url, score))
            except Exception:
                pass
    except Exception:
        pass

    # --- IR paths + homepage ---
    ir_paths = [
        "/investors/presentations", "/investor-relations/presentations",
        "/investors/corporate-presentation", "/investors",
        "/investor-relations", "/ir", "/",
    ]
    for path in ir_paths:
        try:
            resp = session.get(_urljoin(website, path), timeout=timeout, allow_redirects=True)
            if resp.status_code != 200:
                continue
            # Find anchors with text and PDF href
            anchor_pat = re.compile(
                r'<a[^>]+href=["\']([^"\']*\.pdf[^"\']*)["\'][^>]*>(.*?)</a>',
                re.IGNORECASE | re.DOTALL
            )
            href_pat = re.compile(r'href=["\']([^"\']*\.pdf[^"\']*)["\']', re.IGNORECASE)
            seen = set()
            for m in anchor_pat.finditer(resp.text):
                href, text = m.group(1), re.sub(r'<[^>]+>', '', m.group(2)).strip().lower()
                full = _urljoin(resp.url, href)
                url_lower = href.lower()
                if full in seen: continue
                seen.add(full)
                score = sum(20 for kw in pres_keywords if kw in url_lower or kw in text)
                if any(neg in url_lower or neg.replace('-',' ') in text for neg in pres_negative):
                    continue
                if score > 0:
                    pdf_candidates.append((full, score + (30 if path != "/" else 5)))
            for href in href_pat.findall(resp.text):
                full = _urljoin(resp.url, href)
                if full in seen: continue
                seen.add(full)
                url_lower = href.lower()
                score = sum(20 for kw in pres_keywords if kw in url_lower)
                if score > 0 and not any(neg in url_lower for neg in pres_negative):
                    pdf_candidates.append((full, score + (30 if path != "/" else 5)))
        except Exception:
            continue

    if not pdf_candidates:
        log.info(f"  {symbol}: no presentation candidates found")
        return {}

    # Sort by score descending, try each until one downloads OK
    pdf_candidates.sort(key=lambda x: x[1], reverse=True)
    for pdf_url, score in pdf_candidates[:5]:
        try:
            clean_url = re.sub(r'(\?v=[^?]+)(?:\?v=[^?]+)+', r'\1', pdf_url)
            pdf_resp = session.get(clean_url, timeout=30)
            if pdf_resp.status_code != 200:
                continue
            if pdf_resp.content[:4] != b'%PDF':
                continue
            size_kb = len(pdf_resp.content) // 1024
            if size_kb < min_size_kb:
                log.debug(f"  {symbol}: PDF too small ({size_kb}KB): {clean_url}")
                continue

            # Save to Results/{SYMBOL}/pdfs/Presentations/
            dest_dir = RESULTS_DIR / symbol / "pdfs" / "Presentations"
            dest_dir.mkdir(parents=True, exist_ok=True)
            from urllib.parse import urlparse as _urlparse
            raw_fname = Path(_urlparse(clean_url).path).name.split("?")[0]
            if not raw_fname or not raw_fname.lower().endswith(".pdf"):
                raw_fname = f"{symbol}_presentation.pdf"
            if not re.search(r'20\d{2}', raw_fname):
                raw_fname = f"{date.today().isoformat()}_{raw_fname}"
            dest_path = dest_dir / raw_fname
            dest_path.write_bytes(pdf_resp.content)

            r2_url = upload_to_r2(dest_path, symbol)
            pres_date = _extract_presentation_date(dest_path)
            log.info(f"  {symbol}: presentation found - {raw_fname} ({size_kb}KB) date={pres_date or 'unknown'} -> R2")
            return {
                "presentation_url":     r2_url,
                "presentation_local":   str(dest_path),
                "presentation_size_kb": size_kb,
                "presentation_date":    pres_date,
            }
        except Exception as e:
            log.debug(f"  {symbol}: PDF download error: {e}")
            continue

    log.info(f"  {symbol}: presentation candidates found but none downloaded successfully")
    return {}


# ---------------------------------------------------------------------------
# Reclassify (re-run LLM on existing news releases without a full re-scrape)
# ---------------------------------------------------------------------------
def reclassify_llm(symbols: list[str] | None = None):
    """
    Re-run LLM classification on news releases that are already stored in
    filings_log.csv, without touching any other pipeline stages.

    Use this after updating prompt.txt to retroactively re-flag all releases.

    symbols: list of ticker strings to process, or None to process ALL companies
             that have a Results/<SYMBOL>/filings_log.csv.
    """
    _cfg = {}
    try:
        with open(SCRIPT_DIR / "config.toml", "rb") as _f:
            _cfg = tomllib.load(_f)
    except Exception:
        pass
    llm_model   = _cfg.get("llm", {}).get("model", "gpt-4o-mini")
    llm_workers = _cfg.get("llm", {}).get("max_workers", 80)

    fieldnames = [
        "source", "symbol", "issuer", "filing_date", "doc_type", "industry",
        "category", "synopsis", "article_url", "article_id",
        "pdf_url", "pdf_path", "page_count", "pdf_size_kb",
        "downloaded", "r2_url", "news_text", "news_html_path", "news_html_r2_url", "mat_summary",
        "llm_flag", "llm_summary", "llm_project", "llm_error",
        "as_at_date", "aif_filed",
    ]

    # Resolve which companies to process
    if symbols:
        targets = [s.upper() for s in symbols]
    else:
        targets = sorted(p.name for p in RESULTS_DIR.iterdir()
                         if p.is_dir() and (p / "filings_log.csv").exists())

    log.info(f"Reclassify: {len(targets)} company/companies to process")

    for sym in targets:
        csv_path = RESULTS_DIR / sym / "filings_log.csv"
        if not csv_path.exists():
            log.warning(f"  {sym}: no filings_log.csv found, skipping")
            continue

        # Attach per-company log file
        company_log_handler = attach_company_log(sym)

        log.info(f"  {sym}: loading {csv_path.name}")
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))

        news_rows = [r for r in rows if r.get("category") == "NewsRelease" and r.get("news_text")]
        log.info(f"  {sym}: {len(news_rows)} news releases with text (of {len(rows)} total rows)")
        if not news_rows:
            log.info(f"  {sym}: nothing to reclassify")
            detach_company_log(company_log_handler)
            continue

        # Clear existing LLM columns so run_llm_classification picks them up
        for r in rows:
            if r.get("category") == "NewsRelease" and r.get("news_text"):
                r["llm_flag"]    = ""
                r["llm_summary"] = ""
                r["llm_project"] = ""
                r["llm_error"]   = ""

        # Run classification
        rows = run_llm_classification(sym, rows, llm_model, llm_workers)

        # Write back
        tmp = csv_path.with_suffix(".tmp")
        with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        tmp.replace(csv_path)

        changed  = sum(1 for r in rows if r.get("llm_flag") == "CHANGED")
        none_    = sum(1 for r in rows if r.get("llm_flag") == "NONE")
        errors   = sum(1 for r in rows if r.get("llm_error"))
        log.info(f"  {sym}: reclassify done - CHANGED={changed}, NONE={none_}, errors={errors}")
        log.info(f"  {sym}: CSV written -> {csv_path.name}")

        detach_company_log(company_log_handler)

    log.info("Reclassify complete")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="MM Company Onboarding")
    ap.add_argument("--companies", type=str, default="custom_run_or_onboarding_list.csv",
                    help="CSV with columns: symbol, company_name, exchange, sedar_party_number")
    ap.add_argument("--symbol",   type=str, default="",
                    help="Onboard a single symbol only")
    ap.add_argument("--limit",    type=int, default=0,
                    help="Limit number of companies (for testing)")
    ap.add_argument("--reclassify", action="store_true",
                    help="Re-run LLM classification only on existing news releases (no scraping). "
                         "Use after updating prompt.txt. Works with --symbol or processes all companies.")
    args = ap.parse_args()

    log.info("=" * 70)
    log.info(f"MM COMPANY ONBOARDING - {datetime.now(TORONTO_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    log.info("=" * 70)

    # --reclassify: re-run LLM only, no scraping
    if args.reclassify:
        symbols = [args.symbol.upper()] if args.symbol else None
        reclassify_llm(symbols)
        return

    # Load companies list
    companies_path = Path(args.companies) if args.companies else COMPANIES_CSV
    if not companies_path.is_absolute():
        companies_path = SCRIPT_DIR / companies_path
    if not companies_path.exists():
        log.error(f"Companies file not found: {companies_path}")
        sys.exit(1)

    with open(companies_path, newline="", encoding="utf-8-sig") as f:
        companies = list(csv.DictReader(f))

    # Load universe for enrichment (company_name, exchange, sedar_party_number)
    universe = load_universe_lookup()
    log.info(f"Universe loaded: {len(universe)} companies")

    if args.symbol:
        sym_upper = args.symbol.upper()
        matched = [c for c in companies if c.get("symbol","").upper() == sym_upper]
        if not matched:
            # Fall back to universe - allows running any universe company without editing the list
            if sym_upper in universe:
                matched = [{"symbol": sym_upper}]
                log.info(f"  '{sym_upper}' not in custom list - sourcing from canadian_universe.csv")
            else:
                log.error(f"Symbol '{sym_upper}' not found in custom list or canadian_universe.csv")
                sys.exit(1)
        companies = matched
    if args.limit:
        companies = companies[:args.limit]

    # Enrich each company from universe (fills blanks; universe is authoritative for name/exchange/party)
    for company in companies:
        sym = company.get("symbol", "").strip().upper()
        if sym in universe:
            u = universe[sym]
            if not company.get("company_name"):  company["company_name"]      = u.get("name", "")
            if not company.get("exchange"):       company["exchange"]          = u.get("exchange", "")
            if not company.get("sedar_party_number"): company["sedar_party_number"] = u.get("sedar_party_number", "")
            if not company.get("sw_symbol"):      company["sw_symbol"]         = u.get("sw_symbol", "")

    log.info(f"Companies to onboard: {len(companies)}")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Init Stockwatch session
    cookies    = get_stockwatch_cookies()
    sw_session = StockwatchSedarSession(cookies, delay=0.5)

    # Requests session for PDF downloads - needs Stockwatch cookies
    # (Stockwatch PDFs return HTML redirect without auth)
    req_session = requests.Session()
    req_session.headers.update({"User-Agent": "Mozilla/5.0"})
    for name, value in cookies.items():
        req_session.cookies.set(name, value, domain="www.stockwatch.com")

    # Lockfile dir - dashboard polls this to show running status for CLI-triggered runs
    LOCK_DIR = SCRIPT_DIR / ".running"
    LOCK_DIR.mkdir(exist_ok=True)

    # Write batch metadata for dashboard banner
    import json as _json
    batch_start = datetime.now().isoformat()
    def _write_batch_meta(completed: int, current_sym: str = ""):
        meta = {
            "total": len(companies),
            "completed": completed,
            "current": current_sym,
            "started_at": batch_start,
        }
        try:
            (LOCK_DIR / "batch_meta.json").write_text(_json.dumps(meta), encoding="utf-8")
        except Exception:
            pass

    _write_batch_meta(0)

    # Run onboarding for each company
    summaries = []
    for i, company in enumerate(companies, 1):
        sym    = company.get("symbol", "").strip().upper()
        name   = company.get("company_name", "").strip()
        exch   = company.get("exchange", "").strip()
        party  = company.get("sedar_party_number", "").strip()
        sw_sym = company.get("sw_symbol", "").strip().upper()

        if not sym:
            log.warning(f"  Row {i}: no symbol, skipping")
            continue

        log.info(f"\n[{i}/{len(companies)}] {sym} - {name}")

        # If no party number, try to find it via find_sedar_parties.py before onboarding
        if not party:
            log.info(f"  {sym}: no SEDAR party number - running find_sedar_parties.py --symbol {sym}")
            finder_script = SCRIPT_DIR.parent / "1. Canadian Master Sync" / "find_sedar_parties.py"
            if finder_script.exists():
                import subprocess as _sp
                result = _sp.run(
                    [sys.executable, str(finder_script), "--symbol", sym],
                    cwd=str(finder_script.parent),
                    timeout=120
                )
                if result.returncode == 0:
                    # Reload universe to pick up newly written party number
                    fresh = load_universe_lookup()
                    party = fresh.get(sym, {}).get("sedar_party_number", "").strip()
                    if party:
                        log.info(f"  {sym}: party number found: {party}")
                        company["sedar_party_number"] = party
                        name = name or fresh.get(sym, {}).get("name", "")
                        exch = exch or fresh.get(sym, {}).get("exchange", "")
                    else:
                        log.warning(f"  {sym}: party number still not found after lookup - SEDAR+ Phase 2 will be skipped")
                else:
                    log.warning(f"  {sym}: find_sedar_parties.py failed (rc={result.returncode}) - SEDAR+ Phase 2 will be skipped")
            else:
                log.warning(f"  {sym}: find_sedar_parties.py not found at {finder_script} - SEDAR+ Phase 2 will be skipped")

        lock_file = LOCK_DIR / f"{sym}.lock"
        lock_file.write_text(datetime.now().isoformat(), encoding="utf-8")
        _write_batch_meta(i - 1, sym)
        try:
            summary = onboard_company(sym, name, exch, party, sw_session, req_session, sw_symbol=company.get("sw_symbol", ""))
            summaries.append(summary)
        except Exception as e:
            log.error(f"  {sym}: onboarding failed: {e}")
            summaries.append({"symbol": sym, "error": str(e)})
        finally:
            lock_file.unlink(missing_ok=True)
            _write_batch_meta(i, "")

    # Write master summary
    summary_path = RESULTS_DIR / "onboarding_summary.csv"
    summary_fields = ["symbol","company_name","aif_filed","as_at_date",
                      "total","relevant","downloaded","news","csv_path","error"]
    with open(summary_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=summary_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(summaries)

    log.info("\n" + "=" * 70)
    log.info(f"ONBOARDING COMPLETE: {len(summaries)} companies processed")
    log.info(f"Summary: {summary_path}")
    log.info("=" * 70)
    # Clean up batch metadata
    try:
        (LOCK_DIR / "batch_meta.json").unlink(missing_ok=True)
    except Exception:
        pass


if __name__ == "__main__":
    main()

