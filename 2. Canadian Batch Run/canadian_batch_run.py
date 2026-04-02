"""
canadian_batch_run.py - Canadian Markets Batch (Stockwatch + SEDAR+ Filings)
=============================================================================
Runs both Canadian data pipelines in sequence for a given date:

  Phase 1: Stockwatch news releases
    - Fetches all Canadian mining news releases from stockwatch.com
    - Extracts full article text
    - Output: articles.csv + text/ folder

  Phase 2: SEDAR+ regulatory filings
    - Searches SEDAR+ for regulatory filings (AIF, NI43-101, MD&A, financials,
      material change reports, news releases, prospectus, M&A, proxy etc.)
    - Downloads PDFs organised by category
    - Captures permanent URLs, page counts, ticker symbols
    - Output: filings.csv + pdfs/ folder

Both phases write into the same Daily/YYYY-MM-DD/HHMM/ output folder.

Usage:
  python canadian_batch_run.py                    # yesterday -> today
  python canadian_batch_run.py 2026-03-27 2026-03-27

Folder structure:
  Daily/YYYY-MM-DD/HHMM/
    articles.csv          - Stockwatch news releases metadata + text path
    text/                 - Full article text (one .txt per release)
    filings.csv           - SEDAR+ filings metadata (ticker, perm_url, pages...)
    pdfs/
      AIF/
      NI43-101/
      MD&A/
      FinancialStatements/
      MaterialChange/
      NewsRelease/
      Prospectus/
      M&A/
      Proxy/
      Other/

  state.json              - last successful run (stockwatch)
  state_filings.json      - last successful run (sedar filings)
  stockwatch_session.json - saved Stockwatch cookies
  canadian_batch_run.log
"""

import os
import re
import csv
import json
import sys
import time
import logging
import traceback
import requests
import pytz
from datetime import datetime, timedelta, date
from pathlib import Path
from bs4 import BeautifulSoup

TORONTO_TZ = pytz.timezone("America/Toronto")

def _normalise_text(s: str) -> str:
    """Normalise unicode characters from SEDAR HTML to clean ASCII-safe equivalents."""
    if not s:
        return s
    # Common replacements: accented chars, special punctuation
    replacements = {
        '\u00e9': 'e',   # é -> e (Québec -> Quebec)
        '\u00e8': 'e',   # è
        '\u00ea': 'e',   # ê
        '\u00eb': 'e',   # ë
        '\u00e0': 'a',   # à
        '\u00e2': 'a',   # â
        '\u00e4': 'a',   # ä
        '\u00f4': 'o',   # ô
        '\u00f6': 'o',   # ö
        '\u00f9': 'u',   # ù
        '\u00fb': 'u',   # û
        '\u00fc': 'u',   # ü
        '\u00ee': 'i',   # î
        '\u00ef': 'i',   # ï
        '\u00e7': 'c',   # ç
        '\u2013': '-',   # en dash
        '\u2014': '-',   # em dash
        '\u2018': "'",   # left single quote
        '\u2019': "'",   # right single quote
        '\u201c': '"',   # left double quote
        '\u201d': '"',   # right double quote
        '\u00ab': '"',   # «
        '\u00bb': '"',   # »
        '\u2026': '...',  # ellipsis
        '\u00a0': ' ',   # non-breaking space
        '\u2012': '-',   # figure dash
        '\u2010': '-',   # hyphen
    }
    for char, replacement in replacements.items():
        s = s.replace(char, replacement)
    return s



# Force UTF-8 on stdout/stderr
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR   = Path(__file__).parent
CONFIG_PATH  = SCRIPT_DIR / "config.toml"
STATE_SW_PATH     = SCRIPT_DIR / "state.json"
STATE_SEDAR_PATH  = SCRIPT_DIR / "state_filings.json"
LOG_PATH     = SCRIPT_DIR / "canadian_batch_run.log"
SESSION_PATH = SCRIPT_DIR / "stockwatch_session.json"

STOCKWATCH_BASE = "https://www.stockwatch.com"
SEARCH_URL      = f"{STOCKWATCH_BASE}/News/Search"
SEDAR_BASE      = "https://www.sedarplus.ca"
CDP_URL         = "http://127.0.0.1:18800"

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
# Config
# ---------------------------------------------------------------------------
def load_config():
    defaults = {
        "default_lookback_days": 1,
        "request_delay_sec": 0.5,
        "output_dir": "Daily",
        "fetch_article_text": True,
        "exchange_filter": "",
        "download_pdfs": True,
        "error_notify": [],
    }
    try:
        import tomllib
        with open(CONFIG_PATH, "rb") as f:
            raw = tomllib.load(f)
        cfg = defaults.copy()
        cfg.update(raw.get("general", {}))
        sw = raw.get("stockwatch", {})
        cfg["fetch_article_text"] = sw.get("fetch_article_text", True)
        cfg["exchange_filter"]    = sw.get("exchange_filter", "")
        sf = raw.get("sedar_filings", {})
        cfg["filing_types"]  = sf.get("filing_types", ALL_FILING_TYPE_CODES)
        cfg["download_pdfs"] = sf.get("download_pdfs", True)
        cfg["error_notify"]  = raw.get("notifications", {}).get("error_notify", [])
        return cfg
    except Exception as e:
        log.warning(f"Config load failed ({e}), using defaults")
        return defaults

# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------
def load_state(path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def save_state(path, state):
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")

# ---------------------------------------------------------------------------
# SEDAR+ filing type maps
# ---------------------------------------------------------------------------
FILING_TYPE_MAP = {
    "ANNUAL_INFORMATION_FORMS":                 "AIF",
    "TECHNICAL_REPORTS_NI_43101":               "NI43-101",
    "TECHNICAL_REPORTS_NI_43101_EM":            "NI43-101",
    "ANNUAL_MDA":                               "MD&A",
    "INTERIM_MDA":                              "MD&A",
    "ANNUAL_FINANCIAL_STATEMENTS":              "FinancialStatements",
    "INTERIM_FINANCIAL_STATEMENTSREPORT":       "FinancialStatements",
    "MATERIAL_CHANGE_REPORT":                   "MaterialChange",
    "NEWS_RELEASES":                            "NewsRelease",
    "BUSINESS_ACQUISITION_REPORT":              "M&A",
    "FILING_STATEMENT":                         "M&A",
    "ANNUAL_REPORT":                            "Other",
    "SHORT_FORM_PROSPECTUS_NI_44101":           "Prospectus",
    "LONG_FORM_PROSPECTUS":                     "Prospectus",
    "SHELF_PROSPECTUS_NI_44102":                "Prospectus",
    "WKSI_SHELF_PROSPECTUS_NI_44102":           "Prospectus",
    "RIGHTS_OFFERING_MATERIAL":                 "Prospectus",
    "LISTED_ISSUER_FINANCING_EXEMPTION":        "Prospectus",
    "CPC_PROSPECTUS_TSXV":                      "Prospectus",
    "CPC_QUALIFYING_TRANSACTION":               "Prospectus",
    "SECURITIES_ACQUISITION_FILINGS_EARLY_WARNING": "M&A",
    "TAKEOVER_BID_FILINGS":                     "M&A",
    "EXEMPT_TAKEOVER_BID_FILINGS":              "M&A",
    "EXEMPT_ISSUER_BID_FILINGS":                "M&A",
    "FORMAL_ISSUER_BID_FILINGS":                "M&A",
    "MANAGEMENT_PROXY_MATERIALS":               "Proxy",
    "PROXY_SOLICITATION_MATERIALS":             "Proxy",
}

FILENAME_CATEGORY_MAP = [
    (re.compile(r"annual information form", re.I),                      "AIF"),
    (re.compile(r"NI 43-101|technical report", re.I),                   "NI43-101"),
    (re.compile(r"annual md|annual management discussion", re.I),        "MD&A"),
    (re.compile(r"interim md|interim management discussion", re.I),      "MD&A"),
    (re.compile(r"annual financial", re.I),                             "FinancialStatements"),
    (re.compile(r"interim financial|interim statements", re.I),          "FinancialStatements"),
    (re.compile(r"material change", re.I),                              "MaterialChange"),
    (re.compile(r"news release|press release", re.I),                   "NewsRelease"),
    (re.compile(r"prospectus", re.I),                                   "Prospectus"),
    (re.compile(r"take.over bid|takeover bid|issuer bid", re.I),        "M&A"),
    (re.compile(r"early warning|business acquisition|filing statement", re.I), "M&A"),
    (re.compile(r"management information circular|proxy", re.I),        "Proxy"),
]

ALL_FILING_TYPE_CODES = list(FILING_TYPE_MAP.keys())

def _classify_filing(filename: str) -> str:
    for pattern, category in FILENAME_CATEGORY_MAP:
        if pattern.search(filename):
            return category
    return "Other"

def _safe_filename(company: str, filename: str) -> str:
    company_safe = re.sub(r'[^\w\s-]', '', company)[:35].strip()
    doc_safe     = re.sub(r'[^\w\s.-]', '', filename)[:50].strip()
    if not doc_safe.lower().endswith(".pdf"):
        doc_safe += ".pdf"
    return f"{company_safe} - {doc_safe}"

# ---------------------------------------------------------------------------
# TMX Ticker Lookup
# ---------------------------------------------------------------------------
_TICKER_CACHE: dict = {}
_TICKER_LOADED = False

def _normalise_name(name: str) -> str:
    n = name.lower()
    n = re.sub(r'\b(inc|corp|ltd|limited|plc|llc|lp|co|company|resources|mining|minerals|gold|silver|metals|energy|capital|ventures|holdings|group|technologies|tech|international|global|canadian|canada)\b', '', n)
    n = re.sub(r'[^a-z0-9]', '', n)
    return n.strip()

def _load_tmx_tickers():
    global _TICKER_LOADED, _TICKER_CACHE
    if _TICKER_LOADED:
        return
    try:
        for exchange in ("tsx", "tsxv"):
            r = requests.get(
                f"https://www.tsx.com/json/company-directory/search/{exchange}/^",
                timeout=15, headers={"User-Agent": "Mozilla/5.0"}
            )
            if r.status_code != 200:
                continue
            for item in r.json().get("results", []):
                ticker = item.get("symbol", "").strip()
                name   = item.get("name", "").strip()
                if ticker and name:
                    key = _normalise_name(name)
                    if key and key not in _TICKER_CACHE:
                        _TICKER_CACHE[key] = ticker
        log.info(f"TMX ticker cache loaded: {len(_TICKER_CACHE)} companies")
    except Exception as e:
        log.warning(f"TMX ticker load failed (non-fatal): {e}")
    _TICKER_LOADED = True

def _lookup_ticker(company_name: str) -> str:
    if not _TICKER_LOADED:
        _load_tmx_tickers()
    if not _TICKER_CACHE:
        return ""
    key = _normalise_name(company_name)
    if not key:
        return ""
    if key in _TICKER_CACHE:
        return _TICKER_CACHE[key]
    for cached_key, ticker in _TICKER_CACHE.items():
        if len(key) >= 6 and (key in cached_key or cached_key in key):
            return ticker
    return ""

# ---------------------------------------------------------------------------
# WhatsApp error notification
# ---------------------------------------------------------------------------
def notify_error(msg: str, phones: list):
    if not phones:
        return
    try:
        import subprocess
        for phone in phones:
            subprocess.run(
                ["openclaw", "message", "--channel", "whatsapp",
                 "--to", phone, "--message", msg],
                capture_output=True, timeout=10
            )
    except Exception:
        pass

# ===========================================================================
# PHASE 1: STOCKWATCH
# ===========================================================================

def _load_stockwatch_credentials() -> tuple[str, str]:
    """Load Stockwatch username/password from credentials file."""
    creds_path = Path(r"C:\Users\Admin\.openclaw\credentials\stockwatch.env")
    username = password = ""
    if creds_path.exists():
        for line in creds_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("STOCKWATCH_USERNAME="):
                username = line.split("=", 1)[1].strip()
            elif line.startswith("STOCKWATCH_PASSWORD="):
                password = line.split("=", 1)[1].strip()
    return username, password


def _stockwatch_browser_login(page) -> bool:
    """Log in to Stockwatch via the CDP browser page. Returns True on success."""
    username, password = _load_stockwatch_credentials()
    if not username or not password:
        log.warning("Stockwatch credentials not found - cannot auto-login")
        return False
    try:
        log.info("Stockwatch session expired - attempting auto-login via browser...")
        page.goto("https://www.stockwatch.com/User/NotLoggedIn", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(1500)
        page.locator("input[name='ctl00$LoginID']").first.fill(username)
        page.locator("input[name='ctl00$Password']").first.fill(password)
        page.locator("input[name='ctl00$cmdLogin']").first.click()
        page.wait_for_timeout(2500)
        if "NotLoggedIn" in page.url or "notloggedin" in page.url.lower():
            log.warning("Auto-login failed - still on NotLoggedIn page")
            return False
        log.info(f"Auto-login successful (redirected to {page.url[:60]})")
        return True
    except Exception as e:
        log.warning(f"Auto-login error: {e}")
        return False


def get_stockwatch_cookies() -> dict:
    """Load Stockwatch cookies from browser or saved session. Auto-logins if session expired."""
    def _extract_cookies(ctx) -> dict:
        return {
            c["name"]: c["value"]
            for c in ctx.cookies()
            if "stockwatch.com" in c.get("domain", "")
        }

    def _cache_cookies(cookies: dict):
        SESSION_PATH.write_text(
            json.dumps({"cookies": cookies, "saved_at": datetime.now().isoformat()}),
            encoding="utf-8"
        )

    try:
        from playwright.sync_api import sync_playwright
        log.info("Getting Stockwatch cookies from browser (CDP)...")
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(CDP_URL)
            ctx = browser.contexts[0]
            cookies = _extract_cookies(ctx)

            if "XXX" not in cookies:
                page = ctx.new_page()
                try:
                    logged_in = _stockwatch_browser_login(page)
                    if logged_in:
                        page.wait_for_timeout(1000)
                        cookies = _extract_cookies(ctx)
                finally:
                    page.close()

            browser.close()

        if "XXX" in cookies:
            log.info(f"Got {len(cookies)} Stockwatch cookies")
            _cache_cookies(cookies)
            return cookies

        log.warning("Browser login failed - falling back to saved session")

    except Exception as e:
        log.warning(f"Browser cookie fetch failed: {e} - trying saved session")

    if SESSION_PATH.exists():
        try:
            data = json.loads(SESSION_PATH.read_text(encoding="utf-8"))
            cookies = data.get("cookies", {})
            log.info(f"Using saved Stockwatch session from {data.get('saved_at','?')}")
            return cookies
        except Exception:
            pass

    raise RuntimeError(
        "No Stockwatch session and auto-login failed. Check credentials at "
        "C:\\Users\\Admin\\.openclaw\\credentials\\stockwatch.env"
    )


class StockwatchSession:
    def __init__(self, cookies: dict, delay: float = 0.5):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": SEARCH_URL,
        })
        for name, value in cookies.items():
            self.session.cookies.set(name, value, domain="www.stockwatch.com")
        self._viewstate = self._viewstate1 = self._viewstate_gen = ""
        self._load_form()

    def _sleep(self):
        time.sleep(self.delay)

    def _load_form(self):
        log.info("Loading Stockwatch search form...")
        resp = self.session.get(SEARCH_URL, timeout=30)
        resp.raise_for_status()
        if "NotLoggedIn" in resp.url:
            raise RuntimeError("Stockwatch session expired")
        soup = BeautifulSoup(resp.text, "html.parser")
        self._viewstate     = (soup.find("input", {"id": "__VIEWSTATE"})          or {}).get("value","")
        self._viewstate1    = (soup.find("input", {"id": "__VIEWSTATE1"})          or {}).get("value","")
        self._viewstate_gen = (soup.find("input", {"id": "__VIEWSTATEGENERATOR"}) or {}).get("value","")
        logged_in = "You are logged in" in resp.text or "trial is good" in resp.text
        log.info(f"  Form loaded (logged_in={logged_in})")
        self._sleep()

    def search_by_date(self, target_date: date, exchange: str = "") -> list:
        date_str = target_date.strftime("%Y%m%d")
        data = [
            ("__EVENTTARGET",""), ("__EVENTARGUMENT",""),
            ("__VIEWSTATEFIELDCOUNT","2"),
            ("__VIEWSTATE", self._viewstate), ("__VIEWSTATE1", self._viewstate1),
            ("__VIEWSTATEGENERATOR", self._viewstate_gen),
            ("ctl00$TextSymbol2",""), ("ctl00$RadioRegion2","RadioCanada2"),
            ("ctl00$setfocus",""), ("ctl00$scrolly",""),
            ("ctl00$redirectto",""), ("ctl00$showcert",""),
            ("ctl00$MainContent$tDate", date_str),
            ("ctl00$MainContent$dEx", exchange),
            ("ctl00$MainContent$dDateSort","timedesc"),
            ("ctl00$MainContent$dDateFeed","C"),
            ("ctl00$MainContent$tDatePrice1",""), ("ctl00$MainContent$tDatePrice2",""),
            ("ctl00$MainContent$bDate.x","0"), ("ctl00$MainContent$bDate.y","0"),
            ("ctl00$MainContent$dTodayRegion","C"),
            ("ctl00$MainContent$tTodayHours","1"),
            ("ctl00$MainContent$tSymbol",""),
            ("ctl00$MainContent$tSymbolFrom",date_str), ("ctl00$MainContent$tSymbolTo",date_str),
            ("ctl00$MainContent$dSymbolFeed","C"), ("ctl00$MainContent$dType","200"),
            ("ctl00$MainContent$tTypeFrom",date_str), ("ctl00$MainContent$tTypeTo",date_str),
            ("ctl00$MainContent$tKeywords",""), ("ctl00$MainContent$dKeywordFeed","swbull"),
            ("ctl00$MainContent$tKeywordFrom",date_str), ("ctl00$MainContent$tKeywordTo",date_str),
            ("ctl00$MainContent$dKeywordSort","hits"), ("ctl00$MainContent$dKeywordStemming","Y"),
            ("ctl00$MainContent$dKeywordType","nat"), ("ctl00$MainContent$dKeywordFuzzy","0"),
            ("ctl00$MainContent$dKeywordPhonic","N"),
        ]
        log.info(f"  Searching Stockwatch: {date_str}, exchange={exchange or 'all'}")
        resp = self.session.post(SEARCH_URL, data=data, timeout=60)
        resp.raise_for_status()
        self._sleep()
        return self._parse_results(resp.text)

    def _parse_results(self, html: str) -> list:
        soup = BeautifulSoup(html, "html.parser")
        header = soup.find("table", class_="gridHeader")
        header_text = header.get_text(" ", strip=True) if header else ""
        count_m = re.search(r"([\d,]+)\s+items?", header_text)
        total = int(count_m.group(1).replace(",","")) if count_m else 0
        log.info(f"  Results: {total} items")
        tbl = soup.find("table", id="MainContent_NewsList_gNews")
        if not tbl:
            return []
        articles = []
        for row in tbl.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) < 6:
                continue
            pub_dt   = cells[0].get_text(strip=True)
            symbol   = re.sub(r'^[CU]:', '', cells[1].get_text(strip=True))
            exchange = cells[2].get_text(strip=True)
            company  = cells[3].get_text(strip=True)
            price    = cells[4].get_text(strip=True)
            news_type= cells[5].get_text(strip=True)
            headline = cells[6].get_text(strip=True) if len(cells) > 6 else ""
            link     = cells[6].find("a") if len(cells) > 6 else cells[5].find("a")
            art_url  = link["href"] if link else ""
            if art_url and not art_url.startswith("http"):
                art_url = f"{STOCKWATCH_BASE}{art_url}"
            art_id_m = re.search(r"/Item/([^/]+)", art_url)
            articles.append({
                "pub_datetime": pub_dt, "symbol": symbol, "exchange": exchange,
                "company_name": company, "price": price, "news_type": news_type,
                "headline": headline, "article_id": art_id_m.group(1) if art_id_m else "",
                "article_url": art_url, "text_fetched": "", "text_path": "",
            })
        news_releases = [a for a in articles if "News Release" in a["news_type"]]
        log.info(f"  Parsed {len(articles)} items, {len(news_releases)} news releases")
        return news_releases

    def fetch_article_text(self, url: str) -> str:
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            body = soup.find("div", class_="News") or soup.find("div", class_="newsclear")
            if body:
                for el in body.find_all(["nav","header","script","style"]):
                    el.decompose()
                return body.get_text("\n", strip=True)
            main = soup.find(id=re.compile(r"MainContent"))
            return main.get_text("\n", strip=True) if main else ""
        except Exception as e:
            log.error(f"  Article fetch failed {url}: {e}")
            return ""


def run_stockwatch(cfg, date_from, date_to, output_dir):
    """Phase 1: fetch Stockwatch news releases."""
    text_dir = output_dir / "text"
    text_dir.mkdir(parents=True, exist_ok=True)

    log.info("-" * 70)
    log.info("PHASE 1: STOCKWATCH NEWS RELEASES")
    log.info("-" * 70)

    cookies = get_stockwatch_cookies()
    sw = StockwatchSession(cookies, delay=cfg["request_delay_sec"])
    exchange = cfg.get("exchange_filter", "")

    all_articles = []
    current = date_from
    while current <= date_to:
        t0 = time.time()
        log.info(f"Stockwatch search: {current}")
        articles = sw.search_by_date(current, exchange=exchange)
        log.info(f"  {len(articles)} releases in {time.time()-t0:.1f}s")
        all_articles.extend(articles)
        current += timedelta(days=1)
        if current <= date_to:
            time.sleep(cfg["request_delay_sec"])

    if not all_articles:
        log.info("Stockwatch: no news releases found")
        return [], False

    # Fetch article text
    text_ok = text_fail = 0
    if cfg.get("fetch_article_text", True):
        log.info(f"Fetching text for {len(all_articles)} releases...")
        for i, article in enumerate(all_articles, 1):
            url = article.get("article_url", "")
            if not url:
                article["text_fetched"] = "no_url"
                continue
            t0 = time.time()
            text = sw.fetch_article_text(url)
            if text:
                safe_name = re.sub(r'[^\w\s-]', '', article["company_name"])[:40].strip()
                dt_str    = article["pub_datetime"].replace(" ","_").replace(":","")
                txt_name  = f"{dt_str} - {safe_name} - {article['article_id'][:20]}.txt"
                txt_path  = text_dir / txt_name
                text = text.replace('\xa0',' ').replace('\u2013','-').replace('\u2014','-').replace('\u201c','"').replace('\u201d','"').replace('\u2019',"'")
                txt_path.write_text(text, encoding="utf-8")
                article["text_fetched"] = "yes"
                article["text_path"]    = str(txt_path.relative_to(SCRIPT_DIR))
                text_ok += 1
            else:
                article["text_fetched"] = "failed"
                text_fail += 1
                log.warning(f"  [{i}] text failed: {article['company_name'][:40]}")
            if i % 10 == 0:
                log.info(f"  Text: {i}/{len(all_articles)} ({text_ok} ok, {text_fail} failed)")

    # Write CSV
    csv_path = output_dir / "articles.csv"
    fieldnames = ["pub_datetime","symbol","exchange","company_name","price","news_type",
                  "headline","article_id","article_url","text_fetched","text_path"]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_articles)

    log.info(f"Stockwatch: {len(all_articles)} releases, {text_ok} text ok, {text_fail} failed -> {csv_path.name}")
    return all_articles, True


# ===========================================================================
# PHASE 2: SEDAR+ FILINGS
# ===========================================================================

class SedarBrowser:
    def __init__(self):
        from playwright.sync_api import sync_playwright
        self._pw      = sync_playwright().start()
        self._browser = self._pw.chromium.connect_over_cdp(CDP_URL)
        self._ctx     = self._browser.contexts[0]
        self._page    = self._get_sedar_page()
        log.info(f"  SEDAR+ browser ready: {self._page.url[:60]}")
        self._ensure_session()

    def _get_sedar_page(self):
        pages = [p for p in self._ctx.pages if "sedarplus" in p.url and "viewInstance/view" in p.url]
        if pages:
            return max(pages, key=lambda p: len(p.content()))
        pg = self._ctx.new_page()
        pg.goto(
            f"{SEDAR_BASE}/csa-party/service/create.html"
            "?targetAppCode=csa-party&service=searchDocuments&_locale=en",
            wait_until="domcontentloaded", timeout=30000
        )
        return pg

    def _ensure_session(self):
        log.info("  Initialising fresh SEDAR+ search session...")
        self._page.goto(
            f"{SEDAR_BASE}/csa-party/service/create.html"
            "?targetAppCode=csa-party&service=searchDocuments&_locale=en",
            wait_until="domcontentloaded", timeout=30000
        )
        self._page.wait_for_selector("#SubmissionDate", timeout=15000)
        self._page.evaluate("""() => {
            if (window._sedarXhrTrackerInstalled) return;
            window._sedarXhrTrackerInstalled = true;
            window._sedarXhrDone = false;
            const orig = XMLHttpRequest.prototype.open;
            XMLHttpRequest.prototype.open = function(m, url) {
                this._url = url;
                this.addEventListener('load', function() {
                    if (this._url && this._url.includes('update.html')) {
                        window._sedarXhrDone = true;
                    }
                });
                orig.apply(this, arguments);
            };
        }""")
        log.info(f"  Session ready: {self._page.url[:60]}")

    def close(self):
        try:
            self._browser.close()
            self._pw.stop()
        except Exception:
            pass

    def _wait_for_results(self):
        try:
            self._page.wait_for_function("() => window._sedarXhrDone === true", timeout=10000)
        except Exception:
            pass

    def _get_fresh_url(self, resource_href: str) -> str:
        url = f"{SEDAR_BASE}{resource_href}" if resource_href.startswith("/") else resource_href
        url = re.sub(r"[&?]id=[^&]*", "", url)
        m = re.search(r"id=([a-f0-9]+)", self._page.url)
        if m:
            url += f"&id={m.group(1)}"
        return url

    def search(self, date_from: date, date_to: date, filing_type: str = "",
               filing_category: str = "CONTINUOUS_DISCLOSURE", page_size: int = 50):
        from_str = date_from.strftime("%d/%m/%Y")
        to_str   = date_to.strftime("%d/%m/%Y")
        ps_val   = {10: 1, 30: 0, 50: 2}.get(page_size, 0)
        self._page.evaluate(f"""() => {{
            document.getElementById('SubmissionDate').value  = '{from_str}';
            document.getElementById('SubmissionDate2').value = '{to_str}';
            document.getElementById('FilingType').value      = '{filing_type}';
            document.getElementById('FilingCategory').value  = '{filing_category}';
            const ps = document.getElementById('nodeW714PageSize');
            if (ps) ps.value = '{ps_val}';
        }}""")
        self._page.evaluate("() => { window._sedarXhrDone = false; }")
        self._page.evaluate("""() => {
            const btn = Array.from(document.querySelectorAll('button'))
                .find(b => b.textContent.trim() === 'Search');
            if (btn) btn.click();
        }""")
        self._wait_for_results()
        return self._parse_results(self._page.content())

    def next_page(self, page_num: int):
        self._page.evaluate("() => { window._sedarXhrDone = false; }")
        self._page.evaluate(f"""() => {{
            const links = Array.from(document.querySelectorAll('a[href="#"]'))
                .filter(a => a.textContent.trim() === '{page_num}');
            if (links.length) links[links.length-1].click();
        }}""")
        self._wait_for_results()
        return self._parse_results(self._page.content())

    def _parse_results(self, html: str):
        soup  = BeautifulSoup(html, "html.parser")
        total = 0
        for el in soup.find_all(string=re.compile(r"Displaying \d+-\d+ of [\d,]+")):
            m = re.search(r"of ([\d,]+)", el)
            if m:
                total = int(m.group(1).replace(",", ""))
                break
        tbl = soup.find("table", attrs={"aria-label": "List of data items"})
        if not tbl:
            return [], total
        filings = []
        for row in (tbl.find("tbody") or tbl).find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 5:
                continue
            company_text = cells[1].get_text(" ", strip=True)
            company_name = re.sub(r"\s*\(\d{9}\)\s*$", "", company_text).strip()
            if " / " in company_name:
                company_name = company_name.split(" / ")[0].strip()
            party_m      = re.search(r"\((\d{9})\)", company_text)
            doc_cell     = cells[2]
            doc_link     = doc_cell.find("a", href=re.compile(r"resource\.html"))
            filename     = doc_link.get_text(strip=True) if doc_link else doc_cell.get_text(strip=True)
            resource_href= doc_link["href"] if doc_link else ""
            if resource_href and resource_href.startswith("/"):
                resource_href = f"{SEDAR_BASE}{resource_href}"
            drm_m        = re.search(r"drmKey=([a-f0-9]+)", resource_href)
            date_text    = cells[3].get_text(" ", strip=True)
            date_match   = re.search(r"(\d{1,2}\s+\w{3}\s+\d{4})\s+(\d{2}:\d{2})", date_text)
            submitted    = ""
            if date_match:
                try:
                    submitted = datetime.strptime(
                        f"{date_match.group(1)} {date_match.group(2)}", "%d %b %Y %H:%M"
                    ).isoformat()
                except ValueError:
                    submitted = date_text[:30]
            filings.append({
                "company_name":  _normalise_text(company_name),
                "party_number":  party_m.group(1) if party_m else "",
                "filename":      _normalise_text(filename),
                "submitted":     submitted,
                "jurisdiction":  _normalise_text(cells[4].get_text(strip=True) if len(cells) > 4 else ""),
                "file_size":     cells[5].get_text(strip=True) if len(cells) > 5 else "",
                "resource_href": resource_href,
                "drm_key":       drm_m.group(1) if drm_m else "",
                "category":      _classify_filing(filename),
                "perm_url":      "",
                "page_count":    0,
                "ticker":        "",
                "pdf_status":    "",
                "pdf_path":      "",
            })
        return filings, total

    def download_pdf(self, url: str, dest_path: Path):
        """Download PDF via in-page fetch(). Returns (ok, perm_url, page_count)."""
        import base64
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        url = re.sub(r"[&?]id=[^&]*", "", url)
        m = re.search(r"id=([a-f0-9]+)", self._page.url)
        if m:
            url += f"&id={m.group(1)}"
        url_js = url.replace("'", "\\'")
        js = f"""async () => {{
            try {{
                const r = await fetch('{url_js}', {{credentials:'include', headers:{{'Accept':'application/pdf,*/*'}}}});
                const ct = r.headers.get('content-type') || '';
                const finalUrl = r.url;
                if (!r.ok) return {{ok:false, status:r.status, error:'HTTP '+r.status, finalUrl}};
                const buf = await r.arrayBuffer();
                const b = new Uint8Array(buf);
                let s = ''; for (let i=0;i<b.length;i++) s+=String.fromCharCode(b[i]);
                return {{ok:true, ct, size:b.length, b64:btoa(s), finalUrl}};
            }} catch(e) {{ return {{ok:false, error:e.toString(), finalUrl:''}}; }}
        }}"""
        try:
            res      = self._page.evaluate(js)
            perm_url = res.get("finalUrl", "") or ""
            if not res.get("ok"):
                log.error(f"    PDF fetch failed: {res.get('error')} {url[:80]}")
                return False, perm_url, 0
            b64 = res.get("b64", "")
            if not b64:
                return False, perm_url, 0
            pdf_bytes = base64.b64decode(b64)
            if len(pdf_bytes) < 100 or pdf_bytes[:4] != b"%PDF":
                log.error(f"    Invalid PDF ({len(pdf_bytes)}B): {url[:80]}")
                return False, perm_url, 0
            dest_path.write_bytes(pdf_bytes)
            page_count = 0
            try:
                import fitz as _fitz
                with _fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
                    page_count = doc.page_count
            except Exception:
                pass
            return True, perm_url, page_count
        except Exception as e:
            log.error(f"    download_pdf error: {e}")
            return False, "", 0


def run_sedar(cfg, date_from, date_to, output_dir):
    """Phase 2: fetch SEDAR+ regulatory filings."""
    log.info("-" * 70)
    log.info("PHASE 2: SEDAR+ REGULATORY FILINGS")
    log.info("-" * 70)

    filing_types  = cfg.get("filing_types", ALL_FILING_TYPE_CODES)
    download_pdfs = cfg.get("download_pdfs", True)
    dl_ok = dl_skip = dl_fail = 0
    all_filings = []

    _load_tmx_tickers()

    browser = SedarBrowser()
    try:
        for ft in filing_types:
            ft_start   = time.time()
            page_num   = 1
            log.info(f"Searching SEDAR+: {ft}")
            while True:
                if page_num == 1:
                    filings, total = browser.search(date_from=date_from, date_to=date_to,
                                                    filing_type=ft, page_size=50)
                else:
                    filings, _ = browser.next_page(page_num)

                for f in filings:
                    f["filing_type"] = ft
                    if f["category"] == "Other":
                        f["category"] = FILING_TYPE_MAP.get(ft, "Other")
                    f["ticker"] = _lookup_ticker(f.get("company_name", ""))
                    if f.get("resource_href"):
                        f["perm_url"] = browser._get_fresh_url(f["resource_href"])

                    if download_pdfs and f.get("perm_url"):
                        pdf_name = _safe_filename(f["company_name"], f["filename"])
                        pdf_path = output_dir / "pdfs" / f["category"] / pdf_name
                        if pdf_path.exists():
                            log.info(f"  [SKIP] {f['company_name'][:35]} - {f['filename'][:35]} (exists)")
                            f["pdf_status"] = "exists"
                            f["pdf_path"]   = str(pdf_path.relative_to(SCRIPT_DIR))
                            dl_skip += 1
                        else:
                            t0 = time.time()
                            log.info(f"  [DL]   {f['company_name'][:35]} - {f['filename'][:35]}")
                            ok, perm_url, pages = browser.download_pdf(f["perm_url"], pdf_path)
                            elapsed = time.time() - t0
                            if ok:
                                size_kb = pdf_path.stat().st_size // 1024
                                log.info(f"         -> OK {size_kb}KB {pages}p in {elapsed:.1f}s")
                                f["pdf_status"] = "downloaded"
                                f["pdf_path"]   = str(pdf_path.relative_to(SCRIPT_DIR))
                                f["perm_url"]   = perm_url or f["perm_url"]
                                f["page_count"] = pages
                                dl_ok += 1
                            else:
                                log.warning(f"         -> FAILED in {elapsed:.1f}s")
                                f["pdf_status"] = "failed"
                                dl_fail += 1
                            time.sleep(cfg["request_delay_sec"])

                all_filings.extend(filings)
                ft_count = sum(1 for x in all_filings if x.get("filing_type") == ft)
                log.info(f"  Page {page_num}: {len(filings)} filings (total={total}, collected={ft_count})")
                if not filings or len(filings) < 50 or ft_count >= total:
                    break
                page_num += 1
                time.sleep(cfg["request_delay_sec"])

            log.info(f"  {ft}: {sum(1 for x in all_filings if x.get('filing_type')==ft)} filings in {time.time()-ft_start:.1f}s")
            time.sleep(cfg["request_delay_sec"])

    finally:
        browser.close()

    if not all_filings:
        log.info("SEDAR+: no filings found")
        return [], False

    # Write CSV
    csv_path   = output_dir / "filings.csv"
    fieldnames = ["company_name","ticker","party_number","filename","submitted",
                  "jurisdiction","file_size","filing_type","category",
                  "drm_key","perm_url","page_count","pdf_status","pdf_path"]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_filings)

    from collections import Counter
    cat_counts = Counter(f.get("category","Other") for f in all_filings)
    log.info(f"SEDAR+: {len(all_filings)} filings -> {csv_path.name}")
    log.info(f"  PDFs: {dl_ok} downloaded, {dl_skip} skipped, {dl_fail} failed")
    for cat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1]):
        log.info(f"  {cat:<25}: {cnt}")
    return all_filings, True


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    cfg = load_config()

    # All dates/times pegged to Toronto time (America/Toronto)
    # TSX/TSXV operates on Toronto time - same as Sydney time for ASX
    now_toronto = datetime.now(TORONTO_TZ)
    today       = now_toronto.date()

    sw_state     = load_state(STATE_SW_PATH)
    sedar_state  = load_state(STATE_SEDAR_PATH)

    last_run_str = sw_state.get("last_run_date") or sedar_state.get("last_run_date")
    if last_run_str:
        try:
            date_from = date.fromisoformat(last_run_str) + timedelta(days=1)
        except ValueError:
            date_from = today - timedelta(days=cfg["default_lookback_days"])
    else:
        date_from = today - timedelta(days=cfg["default_lookback_days"])
    date_to = today

    if len(sys.argv) >= 3:
        try:
            date_from = date.fromisoformat(sys.argv[1])
            date_to   = date.fromisoformat(sys.argv[2])
        except ValueError:
            log.error("Use: python canadian_batch_run.py YYYY-MM-DD YYYY-MM-DD")
            sys.exit(1)

    if date_from > date_to:
        log.info("Already up to date.")
        return

    run_dt     = now_toronto  # Toronto time for run timestamp
    batch_date = date_to.strftime("%Y-%m-%d")
    batch_time = run_dt.strftime("%H%M")
    output_dir = SCRIPT_DIR / cfg["output_dir"] / batch_date / batch_time
    output_dir.mkdir(parents=True, exist_ok=True)

    # Per-batch log file in the output folder
    batch_log_path = output_dir / "log.txt"
    batch_log_handler = logging.FileHandler(batch_log_path, encoding="utf-8")
    batch_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(batch_log_handler)

    log.info("=" * 70)
    log.info(f"CANADIAN BATCH RUN - START {run_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    log.info(f"  Date range : {date_from} to {date_to}")
    log.info(f"  Output     : {output_dir}")
    log.info("=" * 70)

    run_ok = True
    try:
        # Phase 1: Stockwatch
        sw_articles, sw_ok = run_stockwatch(cfg, date_from, date_to, output_dir)

        # Phase 2: SEDAR+
        sedar_filings, sedar_ok = run_sedar(cfg, date_from, date_to, output_dir)

        # Update state
        now_iso = run_dt.strftime('%Y-%m-%dT%H:%M:%S')
        save_state(STATE_SW_PATH,    {"last_run_date": date_to.isoformat(), "last_run_time": now_iso, "last_run_count": len(sw_articles)})
        save_state(STATE_SEDAR_PATH, {"last_run_date": date_to.isoformat(), "last_run_time": now_iso, "last_run_count": len(sedar_filings)})

        elapsed = (datetime.now(TORONTO_TZ) - run_dt).total_seconds()
        log.info("=" * 70)
        log.info(f"CANADIAN BATCH COMPLETE")
        log.info(f"  Stockwatch releases : {len(sw_articles)}")
        log.info(f"  SEDAR+ filings      : {len(sedar_filings)}")
        log.info(f"  Total runtime       : {elapsed:.0f}s")
        log.info("=" * 70)
        log.removeHandler(batch_log_handler)
        batch_log_handler.close()

    except Exception as e:
        tb = traceback.format_exc()
        log.error("=" * 70)
        log.error(f"CANADIAN BATCH FAILED: {type(e).__name__}: {e}")
        for line in tb.splitlines():
            log.error(f"  {line}")
        log.error("=" * 70)
        try:
            log.removeHandler(batch_log_handler)
            batch_log_handler.close()
        except Exception:
            pass
        notify_error(f"Canadian batch error: {type(e).__name__}: {e}", cfg.get("error_notify", []))
        sys.exit(1)


if __name__ == "__main__":
    main()
