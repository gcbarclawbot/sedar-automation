"""
sedar_batch.py - SEDAR+ News Release Fetcher
Fetches news releases filed on SEDAR+ for a given date range,
saves metadata to CSV and downloads PDFs.

Folder structure mirrors other ASX scripts:
  Daily/YYYY-MM-DD/HHMM/
    filings.csv          - metadata for all filings found
    pdfs/                - downloaded PDF files
  state.json             - tracks last successful run
"""

import os
import sys
import re
import csv
import json
import time
import logging
import requests
import traceback
from datetime import datetime, timedelta, date
from pathlib import Path
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "config.toml"
STATE_PATH = SCRIPT_DIR / "state.json"
LOG_PATH = SCRIPT_DIR / "sedar_batch.log"

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
# Config loader (minimal TOML parser - avoids third-party dep)
# ---------------------------------------------------------------------------
def load_config():
    cfg = {
        "default_lookback_days": 1,
        "request_delay_sec": 1.5,
        "page_size": 50,
        "filing_type": "NEWS_RELEASES",
        "filing_category": "CONTINUOUS_DISCLOSURE",
        "output_dir": "Daily",
        "state_file": "state.json",
        "jurisdictions": [],
        "company_name_keywords": [],
        "error_notify": [],
    }
    try:
        import tomllib
        with open(CONFIG_PATH, "rb") as f:
            raw = tomllib.load(f)
    except ImportError:
        # Python < 3.11 fallback
        try:
            import tomli as tomllib
            with open(CONFIG_PATH, "rb") as f:
                raw = tomllib.load(f)
        except ImportError:
            log.warning("No TOML library available, using defaults")
            return cfg

    cfg.update(raw.get("general", {}))
    cfg.update(raw.get("filters", {}))
    cfg["error_notify"] = raw.get("notifications", {}).get("error_notify", [])
    return cfg


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
def load_state():
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_state(state):
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# SEDAR+ session
# ---------------------------------------------------------------------------
SEDAR_BASE = "https://www.sedarplus.ca"
SEARCH_SERVICE_URL = (
    f"{SEDAR_BASE}/csa-party/service/create.html"
    "?targetAppCode=csa-party&service=searchDocuments&_locale=en"
)

# The SEDAR+ system uses a server-side state machine. The session ID is embedded
# in the URL after the initial GET. All subsequent POSTs go to:
#   /csa-party/viewInstance/update.html?id=<SESSION_ID>
# The _VIKEY_ (view instance key) is also needed for update POSTs - parsed from
# the initial page HTML.


class SedarSession:
    def __init__(self, delay: float = 1.5, browser_cookies: dict | None = None):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.sedarplus.ca/home/",
        })
        # Pre-seed cookies from a browser session to bypass bot detection (ShieldSquare)
        if browser_cookies:
            for name, value in browser_cookies.items():
                self.session.cookies.set(name, value, domain="www.sedarplus.ca")
        self.session_id = None
        self.vikey = None
        self.update_url = None
        self._catalyst_token = None   # x-catalyst-session-global header
        self._uzlc = None             # ShieldSquare uzlc tracking header
        # Node IDs - parsed from page (can change between sessions but are consistent within one)
        self.profile_filter_node = None   # e.g. "W667"
        self.profile_ac_node = None       # e.g. "W668"
        self.lookup_node = None           # e.g. "W668" (same as ac in practice)

    def _sleep(self):
        time.sleep(self.delay)

    def init(self):
        """Load the search page and extract session ID + VIKEY."""
        log.info("Initialising SEDAR+ session...")
        resp = self.session.get(SEARCH_SERVICE_URL, allow_redirects=True, verify=False, timeout=30)
        resp.raise_for_status()

        # Extract session ID from URL  e.g. ?id=0c11f8b7998bcd96...
        m = re.search(r"[?&]id=([a-f0-9]+)", resp.url)
        if not m:
            raise RuntimeError(f"Could not extract session ID from URL: {resp.url}")
        self.session_id = m.group(1)
        self.update_url = f"{SEDAR_BASE}/csa-party/viewInstance/update.html?id={self.session_id}"
        log.info(f"Session ID: {self.session_id}")

        # Extract VIKEY from page JS
        m_vikey = re.search(r"viewInstanceKey:'([^']+)'", resp.text)
        if m_vikey:
            self.vikey = m_vikey.group(1)
            log.info(f"VIKEY: {self.vikey}")
        else:
            log.warning("Could not extract VIKEY from page - will attempt without it")

        # Extract profile filter/autocomplete node IDs
        # The form has: nodeW???-filterSQL (search operator) and nodeW???ac (profile text input)
        m_ac = re.search(r'name="(nodeW\d+)ac"', resp.text)
        m_filter = re.search(r'name="(nodeW\d+)-filterSQL"', resp.text)
        if m_ac:
            self.profile_ac_node = m_ac.group(1)   # e.g. "nodeW668"
            log.info(f"Profile AC node: {self.profile_ac_node}")
        if m_filter:
            self.profile_filter_node = m_filter.group(1)  # e.g. "nodeW667"
            log.info(f"Profile filter node: {self.profile_filter_node}")

        self._sleep()
        return self

    def _post(self, data: dict) -> requests.Response:
        # x-catalyst-session-global is stored as a cookie by SEDAR+ - use it as a header too
        catalyst = (
            self._catalyst_token
            or self.session.cookies.get("x-catalyst-session-global", "")
        )
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "text/html, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "x-catalyst-async": "true",
            "x-catalyst-secured": "true",
            "x-security-token": "null",
            "x-catalyst-session-global": catalyst,
            "Origin": "https://www.sedarplus.ca",
            "Referer": f"https://www.sedarplus.ca/csa-party/viewInstance/view.html?id={self.session_id}",
        }
        if self._uzlc:
            headers["uzlc"] = self._uzlc
        resp = self.session.post(
            self.update_url,
            data=data,
            headers=headers,
            verify=False,
            timeout=60,
        )
        resp.raise_for_status()
        self._sleep()
        return resp

    def search_news_releases(
        self,
        date_from: date,
        date_to: date,
        filing_type: str = "NEWS_RELEASES",
        filing_category: str = "CONTINUOUS_DISCLOSURE",
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[list[dict], int]:
        """
        Search for news releases in the given date range.
        Returns (list of filing dicts, total_results).
        POSTs directly to the existing session - no GET needed (avoids bot detection).
        """
        from_str = date_from.strftime("%d/%m/%Y")
        to_str = date_to.strftime("%d/%m/%Y")

        # Page size selector: 0=30, 1=10, 2=50
        data = {
            "nodeW667-filterSQL": "contains",
            "DocumentContent": "",
            "nodeW675-searchOp": "ContainsIgnoreCase",
            "nodeW676-AnyAllFilter": "all",
            "FilingIdentifier": "",
            "FilingCategory": filing_category,
            "FilingType": filing_type,
            "FilingSubType": "",
            "DocumentType": "",
            "SubmissionDate": from_str,
            "SubmissionDate2": to_str,
            "nodeW714PageSize": str({10: 1, 30: 0, 50: 2}.get(page_size, 0)),
            "nodeW715-DownloadAllDocumentsYn": "N",
            "_CBNAME_": "search",
            "_CBVALUE_": "search",
        }
        if self.vikey:
            data["_VIKEY_"] = self.vikey

        log.info(f"Searching: {from_str} to {to_str}, page {page}")
        resp = self._post(data)

        return self._parse_results(resp.text)

    def next_page(self, page_num: int) -> tuple[list[dict], int]:
        """Paginate to next page of results."""
        data = {
            "_CBNAME_": "pageLink",
            "_CBVALUE_": str(page_num),
        }
        if self.vikey:
            data["_VIKEY_"] = self.vikey
        resp = self._post(data)
        return self._parse_results(resp.text)

    def _parse_results(self, html: str) -> tuple[list[dict], int]:
        """Parse the search results HTML fragment and return filing dicts + total count."""
        soup = BeautifulSoup(html, "html.parser")

        # Total results - look for "Displaying X-Y of Z results"
        total = 0
        for el in soup.find_all(string=re.compile(r"Displaying \d+-\d+ of [\d,]+ results")):
            m = re.search(r"of ([\d,]+) results", el)
            if m:
                total = int(m.group(1).replace(",", ""))
                break

        filings = []

        # Each result row - find the main results table
        table = soup.find("table", id=re.compile(r"nodeW\d+"))
        if not table:
            # Fallback: find any table with filing rows
            table = soup.find("table", attrs={"aria-label": "List of data items"})

        if not table:
            log.warning("Could not find results table in response")
            return filings, total

        for row in table.find("tbody").find_all("tr") if table.find("tbody") else []:
            try:
                filing = _parse_row(row)
                if filing:
                    filings.append(filing)
            except Exception as e:
                log.debug(f"Row parse error: {e}")

        log.info(f"Parsed {len(filings)} rows from page (total={total})")
        return filings, total

    def download_pdf(self, doc_url: str, dest_path: Path) -> bool:
        """Download a PDF from a permanent records/document.html URL."""
        try:
            resp = self.session.get(doc_url, verify=False, timeout=60, stream=True)
            resp.raise_for_status()
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            with open(dest_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
            self._sleep()
            return True
        except Exception as e:
            log.error(f"PDF download failed {doc_url}: {e}")
            return False


# ---------------------------------------------------------------------------
# Row parser
# ---------------------------------------------------------------------------
def _parse_row(row) -> dict | None:
    """Extract filing metadata from a table row."""
    cells = row.find_all("td")
    if len(cells) < 5:
        return None

    # Cell 1: company/profile name and link
    profile_cell = cells[1]
    company_name = profile_cell.get_text(" ", strip=True)
    # Extract party number from text like "Barrick Gold (000000923)"
    party_num_m = re.search(r"\((\d{9})\)", company_name)
    party_number = party_num_m.group(1) if party_num_m else ""
    # Clean name - remove the (XXXXXXXXX) suffix
    company_clean = re.sub(r"\s*\(\d{9}\)\s*$", "", company_name).strip()
    # Collapse bilingual names - take English half (before " / ")
    if " / " in company_clean:
        company_clean = company_clean.split(" / ")[0].strip()

    # Cell 2: document name and the resource.html link (session-based) + Generate URL onclick
    doc_cell = cells[2]
    doc_link = doc_cell.find("a", href=re.compile(r"resource\.html"))
    doc_filename = doc_link.get_text(strip=True) if doc_link else doc_cell.get_text(strip=True)

    # Permanent document URL - extracted from the generateUrlBtn onclick data attribute
    # The permanent URL hash is stored in the page as a data attribute on the generate URL button
    gen_btn = row.find("a", class_=re.compile(r"generateUrlBtn"))
    perm_url = ""
    if gen_btn:
        # Look for data-url or the onclick which may contain the hash
        data_url = gen_btn.get("data-url", "")
        if data_url:
            perm_url = data_url if data_url.startswith("http") else f"{SEDAR_BASE}{data_url}"

    # Fallback: resource.html link contains drmKey which we can use to construct the URL
    # In practice we get perm URL by calling the generate URL button - but we can also
    # extract it from the dialog that appears. For batch use, we derive it from drmKey.
    resource_href = doc_link["href"] if doc_link else ""
    drm_key = ""
    if resource_href:
        m_drm = re.search(r"drmKey=([a-f0-9]+)", resource_href)
        if m_drm:
            drm_key = m_drm.group(1)

    # Cell 3: submitted date
    date_cell = cells[3] if len(cells) > 3 else None
    submitted_raw = date_cell.get_text(" ", strip=True) if date_cell else ""
    # Parse "27 Mar 2026 11:13 EDT"
    submitted_dt = _parse_date(submitted_raw)

    # Cell 4: jurisdiction
    juris_cell = cells[4] if len(cells) > 4 else None
    jurisdiction = juris_cell.get_text(strip=True) if juris_cell else ""

    # Cell 5: file size
    size_cell = cells[5] if len(cells) > 5 else None
    file_size = size_cell.get_text(strip=True) if size_cell else ""

    return {
        "company_name": company_clean,
        "party_number": party_number,
        "document_filename": doc_filename,
        "submitted_date": submitted_dt.isoformat() if submitted_dt else submitted_raw,
        "jurisdiction": jurisdiction,
        "file_size": file_size,
        "drm_key": drm_key,
        "perm_url": perm_url,
        "resource_href": resource_href,
    }


def _parse_date(text: str) -> datetime | None:
    """Parse SEDAR+ date strings like '27 Mar 2026 11:13 EDT'."""
    # Short form in row: "27 Mar 2026 11:13 EDT"
    for fmt in ["%d %b %Y %H:%M %Z", "%d %b %Y %H:%M"]:
        try:
            clean = re.sub(r"\s+", " ", text.strip()).split("March")[0].strip()
            return datetime.strptime(clean, fmt)
        except ValueError:
            pass
    # Long form: "March 27 2026 at 11:13:54 Eastern Daylight Time"
    m = re.search(r"(\w+ \d+ \d{4}) at (\d+:\d+:\d+)", text)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)}", "%B %d %Y %H:%M:%S")
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# Resolve permanent URLs
# To get the permanent records/document.html URL for each result, we need to
# either click "Generate URL" (which fires a callback returning the URL in a dialog)
# OR - more efficiently - make a targeted POST to get the URL for a batch of rows.
#
# Investigation shows the permanent URL hash is a SHA-256 of the document's
# internal storage path. The simplest approach: fetch the generate URL callback
# for each row node, which returns the URL.
# ---------------------------------------------------------------------------

def resolve_perm_url(sedar: SedarSession, row_node_id: str) -> str:
    """Fire the generateUrl button callback for a given row node and extract the URL."""
    node_num = re.search(r"W(\d+)", row_node_id)
    if not node_num:
        return ""
    # The Generate URL button node is typically row_node + 64 (based on observed pattern W2873 -> W2937)
    # But this offset is not guaranteed - need to read from page. For now use the direct approach:
    # POST the button callback
    data = {
        "_CBNAME_": "buttonPush",
        "_CBNODE_": f"W{int(node_num.group(1)) + 64}",
        "_CBVALUE_": "generateUrl",
    }
    if sedar.vikey:
        data["_VIKEY_"] = sedar.vikey
    try:
        resp = sedar._post(data)
        m = re.search(r"records/document\.html\?id=([a-f0-9]+)", resp.text)
        if m:
            return f"{SEDAR_BASE}/csa-party/records/document.html?id={m.group(1)}"
    except Exception as e:
        log.debug(f"resolve_perm_url failed for {row_node_id}: {e}")
    return ""


# ---------------------------------------------------------------------------
# Browser bootstrap - get cookies via Playwright to bypass bot detection
# SEDAR+ uses ShieldSquare/PerimeterX which blocks plain requests.
# We use Playwright to do the initial page load, harvest cookies + session ID,
# then hand off to requests for all subsequent calls (much faster).
# ---------------------------------------------------------------------------

def bootstrap_via_playwright() -> tuple[str, str, dict]:
    """
    Launch a headless Chromium via Playwright, load the SEDAR+ search page,
    extract session_id, vikey, and cookies.
    Returns (session_id, vikey, cookies_dict).
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise RuntimeError(
            "playwright not installed. Run: pip install playwright && playwright install chromium"
        )

    log.info("Bootstrapping session via Playwright (headless browser)...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )
        page = ctx.new_page()
        page.goto(SEARCH_SERVICE_URL, wait_until="networkidle", timeout=60000)

        final_url = page.url
        html = page.content()

        # Extract cookies
        raw_cookies = ctx.cookies()
        cookies = {c["name"]: c["value"] for c in raw_cookies}

        browser.close()

    # Extract session ID
    m = re.search(r"[?&]id=([a-f0-9]+)", final_url)
    if not m:
        raise RuntimeError(f"Could not extract session ID from URL after Playwright load: {final_url}")
    session_id = m.group(1)

    # Extract VIKEY
    m_vikey = re.search(r"viewInstanceKey:'([^']+)'", html)
    vikey = m_vikey.group(1) if m_vikey else ""

    log.info(f"Playwright bootstrap done. Session: {session_id[:20]}... VIKEY: {vikey[:16]}...")
    return session_id, vikey, cookies


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
                ["openclaw", "message", "--channel", "whatsapp", "--to", phone, "--message", msg],
                capture_output=True, timeout=10
            )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    cfg = load_config()
    state = load_state()

    # Determine date range
    today = date.today()
    last_run_str = state.get("last_run_date")
    if last_run_str:
        try:
            last_run = date.fromisoformat(last_run_str)
            date_from = last_run + timedelta(days=1)
        except ValueError:
            date_from = today - timedelta(days=cfg["default_lookback_days"])
    else:
        date_from = today - timedelta(days=cfg["default_lookback_days"])

    date_to = today

    if date_from > date_to:
        log.info(f"Already up to date (last run: {last_run_str}). Nothing to do.")
        return

    # Override with CLI args if provided
    if len(sys.argv) >= 3:
        try:
            date_from = date.fromisoformat(sys.argv[1])
            date_to = date.fromisoformat(sys.argv[2])
            log.info(f"Using CLI date range: {date_from} to {date_to}")
        except ValueError:
            log.error(f"Invalid date args. Use YYYY-MM-DD YYYY-MM-DD")
            sys.exit(1)

    # Set up output folder: Daily/YYYY-MM-DD/HHMM/
    run_dt = datetime.now()
    # Use Sydney time for folder naming to stay consistent with other scripts
    # (Perth+2h AEST, Perth+3h AEDT - use local time for simplicity since this
    #  may run on different machines; note in TOOLS.md this should be Sydney time)
    batch_date_str = date_to.strftime("%Y-%m-%d")
    batch_time_str = run_dt.strftime("%H%M")
    output_dir = SCRIPT_DIR / cfg["output_dir"] / batch_date_str / batch_time_str
    output_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir = output_dir / "pdfs"
    pdf_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Output dir: {output_dir}")
    log.info(f"Date range: {date_from} to {date_to}")

    # Suppress SSL warnings (SEDAR+ uses a self-signed cert in their CDN chain)
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    try:
        # Bootstrap via Playwright to get past bot detection, then use requests for speed
        session_id, vikey, cookies = bootstrap_via_playwright()

        sedar = SedarSession(delay=cfg["request_delay_sec"], browser_cookies=cookies)
        # Skip the init() GET - we already have the session ID and VIKEY from Playwright
        sedar.session_id = session_id
        sedar.vikey = vikey
        sedar.update_url = f"{SEDAR_BASE}/csa-party/viewInstance/update.html?id={session_id}"
        # Node IDs for the search form - these are stable across sessions on SEDAR+ v2.6.6
        sedar.profile_filter_node = "nodeW667"
        sedar.profile_ac_node = "nodeW668"
        log.info("Session ready (Playwright bootstrap + requests handoff)")

        all_filings = []
        page = 1
        page_size = int(cfg["page_size"])
        total = None

        while True:
            if page == 1:
                filings, total = sedar.search_news_releases(
                    date_from=date_from,
                    date_to=date_to,
                    filing_type=cfg["filing_type"],
                    filing_category=cfg["filing_category"],
                    page_size=page_size,
                )
            else:
                filings, _ = sedar.next_page(page)

            if not filings:
                log.info(f"No results on page {page}, stopping.")
                break

            # Apply jurisdiction filter if configured
            juris_filter = [j.lower() for j in cfg.get("jurisdictions", [])]
            if juris_filter:
                filings = [f for f in filings if f["jurisdiction"].lower() in juris_filter]

            # Apply company name keyword filter if configured
            kw_filter = [k.lower() for k in cfg.get("company_name_keywords", [])]
            if kw_filter:
                filings = [
                    f for f in filings
                    if any(kw in f["company_name"].lower() for kw in kw_filter)
                ]

            all_filings.extend(filings)
            log.info(f"Page {page}: {len(filings)} filings (total so far: {len(all_filings)}, reported total: {total})")

            if total is not None and len(all_filings) >= total:
                break
            if len(filings) < page_size:
                break

            page += 1

        log.info(f"Total filings collected: {len(all_filings)}")

        # Write CSV
        csv_path = output_dir / "filings.csv"
        fieldnames = [
            "company_name", "party_number", "document_filename",
            "submitted_date", "jurisdiction", "file_size",
            "drm_key", "perm_url", "pdf_downloaded", "pdf_path",
        ]
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_filings)

        log.info(f"Wrote {len(all_filings)} rows to {csv_path}")

        # Download PDFs
        downloaded = 0
        skipped = 0
        failed = 0
        for filing in all_filings:
            perm_url = filing.get("perm_url", "")
            if not perm_url:
                skipped += 1
                filing["pdf_downloaded"] = "no_url"
                filing["pdf_path"] = ""
                continue

            # Safe filename from company + document name
            safe_company = re.sub(r'[^\w\s-]', '', filing["company_name"])[:40].strip()
            safe_doc = re.sub(r'[^\w\s.-]', '', filing["document_filename"])[:60].strip()
            pdf_name = f"{safe_company} - {safe_doc}"
            if not pdf_name.lower().endswith(".pdf"):
                pdf_name += ".pdf"
            pdf_path = pdf_dir / pdf_name

            # Skip if already downloaded
            if pdf_path.exists():
                filing["pdf_downloaded"] = "yes"
                filing["pdf_path"] = str(pdf_path.relative_to(SCRIPT_DIR))
                skipped += 1
                continue

            ok = sedar.download_pdf(perm_url, pdf_path)
            if ok:
                filing["pdf_downloaded"] = "yes"
                filing["pdf_path"] = str(pdf_path.relative_to(SCRIPT_DIR))
                downloaded += 1
            else:
                filing["pdf_downloaded"] = "failed"
                filing["pdf_path"] = ""
                failed += 1

        # Rewrite CSV with download status
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_filings)

        log.info(f"PDFs: {downloaded} downloaded, {skipped} skipped/no-url, {failed} failed")

        # Update state
        state["last_run_date"] = date_to.isoformat()
        state["last_run_time"] = run_dt.isoformat()
        state["last_run_count"] = len(all_filings)
        save_state(state)
        log.info("Done.")

    except Exception as e:
        msg = f"SEDAR+ batch failed: {e}\n{traceback.format_exc()}"
        log.error(msg)
        notify_error(f"SEDAR+ batch error: {e}", cfg.get("error_notify", []))
        sys.exit(1)


if __name__ == "__main__":
    main()
