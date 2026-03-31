"""
find_sedar_parties.py - SEDAR+ Party Number Finder
====================================================
Looks up SEDAR+ party numbers for companies in universe_results.csv.
Writes results to sedar_parties.csv (its own file - never touches universe_results.csv).
Safe to run concurrently with universe_builder.py.

Processes Tier 1 companies first, then Tier 2/3.
Skips companies that already have a party number in sedar_parties.csv.

Usage:
  python find_sedar_parties.py              # all tiers, Tier 1 first
  python find_sedar_parties.py --tier 1     # Tier 1 only
  python find_sedar_parties.py --limit 50   # test run
  python find_sedar_parties.py --symbol ABX # single company
"""

import sys, csv, re, time, logging, argparse
from datetime import datetime
from pathlib import Path
import pytz, requests

# Legal suffix stripping - module-level, used consistently everywhere.
# Word-boundary anchored: 'co' won't match inside 'Commodore', etc.
LEGAL_RE = re.compile(
    r"\b(inc\.?|corp\.?|ltd\.?|limited|llc|lp|plc|co\.?|pty\.?|"
    r"incorporated|corporation|s\.a\.?|n\.v\.?)\b",
    re.IGNORECASE
)

def _strip_legal(s: str) -> str:
    return re.sub(r"\s+", " ", LEGAL_RE.sub(" ", s)).strip()


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SCRIPT_DIR    = Path(__file__).parent
RESULTS_CSV   = SCRIPT_DIR / "canadian_universe.csv"
PARTIES_CSV   = SCRIPT_DIR / "Universe Builder" / "sedar_parties.csv"
LOG_PATH      = SCRIPT_DIR / "find_sedar_parties.log"
CDP_URL       = "http://127.0.0.1:18800"
SEDAR_BASE    = "https://www.sedarplus.ca"
TORONTO_TZ    = pytz.timezone("America/Toronto")

PARTIES_FIELDS = ["symbol", "name", "exchange", "tier",
                  "sedar_party_number", "sedar_party_name", "found_at"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def _ensure_cdp_ready(timeout: int = 30) -> bool:
    import urllib.request, subprocess as _sp
    def _alive():
        try:
            urllib.request.urlopen(f"{CDP_URL}/json", timeout=2)
            return True
        except Exception:
            return False
    if _alive():
        return True
    log.info("CDP not reachable - launching OpenClaw browser...")
    try:
        _sp.Popen(["openclaw", "browser", "start"], shell=True,
                  stdout=_sp.DEVNULL, stderr=_sp.DEVNULL)
    except Exception as e:
        log.warning(f"Failed to launch browser: {e}")
        return False
    import time as _t
    deadline = _t.time() + timeout
    while _t.time() < deadline:
        _t.sleep(2)
        if _alive():
            log.info("CDP ready")
            return True
    log.warning(f"CDP not reachable after {timeout}s")
    return False


def load_universe() -> list:
    """Load universe_results.csv. Returns list of dicts."""
    if not RESULTS_CSV.exists():
        return []
    with open(RESULTS_CSV, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_parties() -> dict:
    """Load sedar_parties.csv. Returns dict: symbol -> {party_number, party_name}."""
    if not PARTIES_CSV.exists():
        return {}
    with open(PARTIES_CSV, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    return {r["symbol"]: r for r in rows if r.get("sedar_party_number")}


def save_party(symbol: str, name: str, exchange: str, tier: str,
               party_number: str, party_name: str):
    """Append one party number result to sedar_parties.csv."""
    write_header = not PARTIES_CSV.exists()
    with open(PARTIES_CSV, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=PARTIES_FIELDS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow({
            "symbol":             symbol,
            "name":               name,
            "exchange":           exchange,
            "tier":               tier,
            "sedar_party_number": party_number,
            "sedar_party_name":   party_name,
            "found_at":           datetime.now(TORONTO_TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
        })


def _name_similarity(our_name: str, candidate_text: str) -> float:
    """
    Score how closely a SEDAR autocomplete result matches our company name.
    Uses difflib SequenceMatcher on normalised strings.
    Returns 0.0-1.0 (higher = better match).

    Strategy:
    - Strip party number (000XXXXXX) from candidate text
    - Strip legal suffixes from both strings
    - Compare with SequenceMatcher ratio
    - Also try matching just against the first part of the candidate
      (SEDAR often shows "Current Name / Former Name (000XXXXXX)")
    """
    from difflib import SequenceMatcher

    def _norm(s: str) -> str:
        s = re.sub(r'\s*\(\d{9}\).*$', '', s)  # strip party number
        s = re.sub(r'\s*/\s*.*$', '', s)        # strip "/ Former Name" part
        s = _strip_legal(s)
        s = re.sub(r'[^\w\s]', ' ', s)
        return re.sub(r'\s+', ' ', s).strip().lower()

    our   = _norm(our_name)
    cand  = _norm(candidate_text)

    # Also try the full candidate text (before stripping / part) in case current name matches better
    cand_full = re.sub(r'\s*\(\d{9}\).*$', '', candidate_text)
    cand_full = re.sub(r'[^\w\s]', ' ', LEGAL_RE.sub(' ', cand_full))
    cand_full = re.sub(r'\s+', ' ', cand_full).strip().lower()

    score1 = SequenceMatcher(None, our, cand).ratio()
    score2 = SequenceMatcher(None, our, cand_full).ratio()
    return max(score1, score2)


def _get_most_recent_filing_date(page, party_number: str) -> str:
    """
    Find the most recent filing date for a SEDAR+ party number.
    Used to disambiguate when similarity scores are close - the ACTIVE party
    will have a recent filing; a dormant/historical party may have more total
    filings but nothing recent.
    Returns ISO date string e.g. "2025-11-28", or "" on failure.
    """
    MONTH_MAP = {
        "jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
        "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12",
    }
    try:
        page.goto(
            f"{SEDAR_BASE}/csa-party/service/create.html"
            "?targetAppCode=csa-party&service=searchDocuments&_locale=en",
            wait_until="domcontentloaded", timeout=20000)
        page.wait_for_selector("#SubmissionDate", timeout=15000)

        pn = page.locator('input[placeholder="Profile name or number"]')
        pn.click()
        page.wait_for_timeout(100)
        pn.type(party_number, delay=40)
        page.wait_for_timeout(1600)

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
            return ""

        # ac._trigger causes a page navigation - wait for it to settle before continuing
        try:
            page.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception:
            pass
        page.wait_for_selector("#SubmissionDate", timeout=10000)
        page.wait_for_timeout(300)

        # Search - no date filter needed when chip is applied
        page.evaluate("""() => {
            window._done = false;
            const orig = XMLHttpRequest.prototype.open;
            XMLHttpRequest.prototype.open = function(m, url) {
                this._url = url;
                this.addEventListener('load', ()=>{ if(this._url?.includes('update.html')) window._done=true; });
                orig.apply(this, arguments);
            };
            Array.from(document.querySelectorAll('button')).find(b=>b.textContent.trim()==='Search')?.click();
        }""")
        try:
            page.wait_for_function("()=>window._done", timeout=8000)
        except Exception:
            pass
        # Wait for table OR no-results message to appear in DOM
        try:
            page.wait_for_selector(
                'table[aria-label="List of data items"], .no-results, [class*="no-result"]',
                timeout=5000)
        except Exception:
            pass
        page.wait_for_timeout(300)

        # Extract date of first result row
        # Table columns: checkbox | profile | document | date | jurisdiction | size | actions
        # Date cell (index 3) contains e.g. "March 30 2026 at 21:54:20 Eastern Daylight Time"
        # OR the shorter form "28 Nov 2025" depending on locale/rendering
        from bs4 import BeautifulSoup
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        tbl  = soup.find("table", attrs={"aria-label": "List of data items"})
        log.debug(f"  [{party_number}] table_found={tbl is not None}, html_len={len(html)}")
        if not tbl:
            return ""
        rows = (tbl.find("tbody") or tbl).find_all("tr")
        if not rows:
            return ""
        cells = rows[0].find_all("td")
        if len(cells) < 4:
            return ""
        date_text = cells[3].get_text(" ", strip=True)

        # Try full month name first: "March 30 2026" or "30 March 2026"
        FULL_MONTHS = {
            "january":"01","february":"02","march":"03","april":"04",
            "may":"05","june":"06","july":"07","august":"08",
            "september":"09","october":"10","november":"11","december":"12",
        }
        # Pattern: "March 30 2026" (month day year)
        m = re.search(r'(\w+)\s+(\d{1,2})\s+(\d{4})', date_text, re.IGNORECASE)
        if m:
            mon_str = m.group(1).lower()
            month   = FULL_MONTHS.get(mon_str) or MONTH_MAP.get(mon_str[:3], "")
            if month:
                return f"{m.group(3)}-{month}-{m.group(2).zfill(2)}"

        # Pattern: "30 Nov 2025" (day abbreviated-month year)
        m = re.search(r'(\d{1,2})\s+(\w{3,})\s+(\d{4})', date_text, re.IGNORECASE)
        if m:
            mon_str = m.group(2).lower()
            month   = FULL_MONTHS.get(mon_str) or MONTH_MAP.get(mon_str[:3], "")
            if month:
                return f"{m.group(3)}-{month}-{m.group(1).zfill(2)}"

        log.debug(f"  Could not parse date from: '{date_text[:60]}'")
        return ""
    except Exception as e:
        log.debug(f"  recent filing check failed for {party_number}: {e}")
        return ""


def lookup_party(page, symbol: str, name: str) -> tuple[str, str]:
    """
    Look up SEDAR+ party number for a company.

    Scoring approach (in order):
    1. For each autocomplete result, compute name similarity score (0-1) vs our company name
    2. If top candidate scores >= 0.85 and is clearly ahead (>0.15 gap), take it immediately
    3. If multiple candidates are close in score (within 0.15), disambiguate by filing count
    4. If no candidate scores >= 0.4, skip this search term and try a shorter one

    This handles cases like "Ero Copper" returning "Cascadero Copper" first -
    the similarity score of "Ero Copper Corp." vs "Ero Copper Corp." (~1.0) will
    beat "Ero Copper Corp." vs "Cascadero Copper Corporation" (~0.55) decisively.
    """
    SIMILARITY_THRESHOLD  = 0.40   # minimum score to even consider a candidate
    CLEAR_WIN_THRESHOLD   = 0.85   # score high enough to accept without filing count check
    CLEAR_WIN_GAP         = 0.15   # gap over next candidate to call it a clear win

    try:
        page.goto(
            f"{SEDAR_BASE}/csa-party/service/create.html"
            "?targetAppCode=csa-party&service=searchDocuments&_locale=en",
            wait_until="domcontentloaded", timeout=20000)
        page.wait_for_selector("#SubmissionDate", timeout=15000)
    except Exception as e:
        log.debug(f"  {symbol}: page load failed: {e}")
        return "", ""

    clean = _strip_legal(name)
    words = clean.split()
    terms = []
    for s in ([name, clean] + [" ".join(words[:i]) for i in range(len(words)-1, 0, -1)]):
        if s and s not in terms:
            terms.append(s)

    pn = page.locator('input[placeholder="Profile name or number"]')

    for term in terms:
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

            # Score every autocomplete result by name similarity
            scored = []
            for i in range(min(count, 10)):
                text = items.nth(i).inner_text()
                m = re.search(r'\((\d{9})\)', text)
                if not m:
                    continue
                pnum  = m.group(1)
                pname = re.sub(r'\s*\(\d{9}\).*$', '', text).strip()
                score = _name_similarity(name, text)
                scored.append((score, pnum, pname, text))

            # Filter out anything below minimum threshold
            scored = [(s, pn_, pname, txt) for s, pn_, pname, txt in scored
                      if s >= SIMILARITY_THRESHOLD]

            if not scored:
                log.debug(f"  {symbol}: no candidates above threshold for '{term}'")
                continue  # try shorter term

            # Sort by score descending
            scored.sort(key=lambda x: x[0], reverse=True)

            best_score, best_pnum, best_pname, best_txt = scored[0]
            second_score = scored[1][0] if len(scored) > 1 else 0.0

            log.debug(f"  {symbol}: top={best_pnum} score={best_score:.2f} "
                      f"second={second_score:.2f} name='{best_pname[:40]}'")

            # Clear winner - high score and well ahead of second place
            if best_score >= CLEAR_WIN_THRESHOLD and (best_score - second_score) >= CLEAR_WIN_GAP:
                log.info(f"  {symbol}: found {best_pnum} (score={best_score:.2f}) -> {best_pname[:50]}")
                return best_pnum, best_pname

            # Ambiguous - candidates too close in score, use filing count to break tie
            close = [(s, pn_, pname) for s, pn_, pname, _ in scored
                     if best_score - s <= CLEAR_WIN_GAP]
            log.info(f"  {symbol}: close={[(round(s,2),p) for s,p,_ in close]}")
            log.info(f"  {symbol}: {len(close)} close candidates (scores "
                     f"{', '.join(f'{s:.2f}' for s,_,_ in close)}) - checking filing counts")

            best_pnum = best_pname = ""
            best_date = ""
            for _, cpnum, cpname in close:
                recent = _get_most_recent_filing_date(page, cpnum)
                log.info(f"  {symbol}: {cpnum} ({cpname[:35]}) -> most recent filing: {recent or 'none'}")
                if recent > best_date:
                    best_date  = recent
                    best_pnum  = cpnum
                    best_pname = cpname
                # Re-navigate for next check
                try:
                    page.goto(
                        f"{SEDAR_BASE}/csa-party/service/create.html"
                        "?targetAppCode=csa-party&service=searchDocuments&_locale=en",
                        wait_until="domcontentloaded", timeout=20000)
                    page.wait_for_selector("#SubmissionDate", timeout=15000)
                    pn = page.locator('input[placeholder="Profile name or number"]')
                except Exception:
                    pass

            if best_pnum:
                log.info(f"  {symbol}: selected {best_pnum} "
                         f"(most recent filing {best_date}, score={best_score:.2f}) "
                         f"-> {best_pname[:50]}")
                return best_pnum, best_pname

        except Exception as e:
            log.debug(f"  {symbol}: search error for '{term}': {e}")
            continue

    log.info(f"  {symbol}: no match found for '{name[:40]}'")
    return "", ""


def lookup_party_audit(page, symbol: str, name: str, known_pnum: str) -> dict:
    """
    Audit-mode lookup: score autocomplete results against known party number.
    Collects ALL candidates across all search terms (deduped by pnum) before scoring.
    Does NOT call _get_most_recent_filing_date for clear wins (score gap >= threshold).
    Returns a result dict: CONFIRMED / WRONG / AMBIGUOUS / NOT_FOUND / NO_STORED / ERROR
    """
    if not known_pnum or known_pnum == "NOT_FOUND":
        return {"symbol": symbol, "outcome": "NO_STORED", "known": known_pnum,
                "top_pnum": "", "top_score": 0, "candidates": [], "notes": ""}

    SIMILARITY_THRESHOLD = 0.40
    CLEAR_WIN_GAP        = 0.15

    try:
        page.goto(
            f"{SEDAR_BASE}/csa-party/service/create.html"
            "?targetAppCode=csa-party&service=searchDocuments&_locale=en",
            wait_until="domcontentloaded", timeout=20000)
        page.wait_for_selector("#SubmissionDate", timeout=15000)
    except Exception as e:
        return {"symbol": symbol, "outcome": "ERROR", "known": known_pnum,
                "top_pnum": "", "top_score": 0, "candidates": [],
                "notes": str(e)[:80]}

    clean = _strip_legal(name)
    words = clean.split()
    terms = []
    for s in ([name, clean] + [" ".join(words[:i]) for i in range(len(words)-1, 0, -1)]):
        if s and s not in terms:
            terms.append(s)

    pn = page.locator('input[placeholder="Profile name or number"]')

    # Try search terms from longest to shortest.
    # Stop as soon as a term returns at least one result above threshold.
    # Only fall through to shorter terms if the current term returns nothing.
    all_candidates = {}  # pnum -> (score, pnum, pname)

    for term in terms:
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

            found_above_threshold = False
            for i in range(min(count, 10)):
                text  = items.nth(i).inner_text()
                m     = re.search(r'\((\d{9})\)', text)
                if not m:
                    continue
                cpnum = m.group(1)
                score = _name_similarity(name, text)
                if score >= SIMILARITY_THRESHOLD:
                    pname = re.sub(r'\s*\(\d{9}\).*$', '', text).strip()
                    if cpnum not in all_candidates or score > all_candidates[cpnum][0]:
                        all_candidates[cpnum] = (score, cpnum, pname)
                    found_above_threshold = True

            if found_above_threshold:
                break  # good results found - don't try shorter terms

        except Exception as e:
            log.debug(f"  {symbol}: audit search error for '{term}': {e}")
            continue

    if not all_candidates:
        return {"symbol": symbol, "outcome": "NOT_FOUND", "known": known_pnum,
                "top_pnum": "", "top_score": 0, "candidates": [],
                "notes": "no autocomplete results"}

    scored = sorted(all_candidates.values(), reverse=True)
    best_score   = scored[0][0]
    best_pnum    = scored[0][1]
    second_score = scored[1][0] if len(scored) > 1 else 0.0
    gap          = best_score - second_score

    cands = [{"pnum": p, "score": round(s, 3), "name": n[:50]}
             for s, p, n in scored[:5]]

    if gap >= CLEAR_WIN_GAP:
        # Clear winner - similarity alone is definitive, no filing date check needed
        if best_pnum == known_pnum:
            return {"symbol": symbol, "outcome": "CONFIRMED",
                    "known": known_pnum, "top_pnum": best_pnum,
                    "top_score": round(best_score, 3),
                    "candidates": cands, "notes": f"score={best_score:.2f} gap={gap:.2f}"}
        else:
            return {"symbol": symbol, "outcome": "WRONG",
                    "known": known_pnum, "top_pnum": best_pnum,
                    "top_score": round(best_score, 3), "candidates": cands,
                    "notes": f"clear winner={best_pnum} (score={best_score:.2f} gap={gap:.2f}) "
                             f"but stored={known_pnum}"}

    # Scores are ambiguous (gap < 0.15) - use most recent filing date to break tie
    close = [(s, p, n) for s, p, n in scored if best_score - s <= CLEAR_WIN_GAP]
    log.info(f"  {symbol}: {len(close)} close candidates "
             f"({', '.join(p for _,p,_ in close)}) - checking filing dates")

    best_filing_pnum = best_filing_pname = ""
    best_filing_date = ""
    for _, cpnum, cpname in close:
        recent = _get_most_recent_filing_date(page, cpnum)
        log.info(f"  {symbol}: {cpnum} ({cpname[:35]}) -> most recent: {recent or 'none'}")
        if recent > best_filing_date:
            best_filing_date  = recent
            best_filing_pnum  = cpnum
            best_filing_pname = cpname
        # Re-navigate for next candidate
        try:
            page.goto(
                f"{SEDAR_BASE}/csa-party/service/create.html"
                "?targetAppCode=csa-party&service=searchDocuments&_locale=en",
                wait_until="domcontentloaded", timeout=20000)
            page.wait_for_selector("#SubmissionDate", timeout=15000)
            pn = page.locator('input[placeholder="Profile name or number"]')
        except Exception:
            pass

    if not best_filing_date:
        return {"symbol": symbol, "outcome": "AMBIGUOUS",
                "known": known_pnum, "top_pnum": best_pnum,
                "top_score": round(best_score, 3), "candidates": cands,
                "notes": "cannot determine dates for either candidate"}

    if best_filing_pnum == known_pnum:
        return {"symbol": symbol, "outcome": "CONFIRMED",
                "known": known_pnum, "top_pnum": best_filing_pnum,
                "top_score": round(best_score, 3), "candidates": cands,
                "notes": f"stored={known_pnum} date={best_filing_date} - active"}
    else:
        return {"symbol": symbol, "outcome": "WRONG",
                "known": known_pnum, "top_pnum": best_filing_pnum,
                "top_score": round(best_score, 3), "candidates": cands,
                "notes": f"active={best_filing_pnum} date={best_filing_date} > stored={known_pnum}"}


def run_recheck(universe: list, symbol_filter: str = "", limit: int = 0):
    """
    Audit all universe companies with stored party numbers.
    Logs a report of CONFIRMED / WRONG / AMBIGUOUS / NOT_FOUND.
    Does NOT write any changes - read only.
    """
    import json as _json

    log.info("=" * 60)
    log.info("RECHECK MODE - auditing stored party numbers (read-only)")
    log.info("=" * 60)

    candidates = [r for r in universe
                  if r.get("sedar_party_number") and
                     r.get("sedar_party_number") != "NOT_FOUND"]
    if symbol_filter:
        candidates = [r for r in candidates
                      if r.get("symbol","").upper() == symbol_filter.upper()]
    if limit:
        candidates = candidates[:limit]

    log.info(f"  Checking {len(candidates)} companies with stored party numbers")

    if not _ensure_cdp_ready():
        log.error("CDP not available")
        return

    try:
        r = requests.get(f"{SEDAR_BASE}/csa-party/", timeout=10)
        if "maintenance" in r.text.lower():
            log.error("SEDAR+ under maintenance")
            return
    except Exception as e:
        log.error(f"SEDAR+ check failed: {e}")
        return

    from playwright.sync_api import sync_playwright

    results     = []
    confirmed   = wrong = ambiguous = not_found = errors = 0

    with sync_playwright() as pw:
        browser = pw.chromium.connect_over_cdp(CDP_URL)
        ctx     = browser.contexts[0]
        page    = ctx.new_page()
        try:
            for i, row in enumerate(candidates, 1):
                sym   = row.get("symbol", "")
                name  = row.get("name", "")
                known = row.get("sedar_party_number", "")

                result = lookup_party_audit(page, sym, name, known)
                results.append(result)

                outcome = result["outcome"]
                if   outcome == "CONFIRMED":  confirmed += 1
                elif outcome == "WRONG":      wrong     += 1; log.warning(f"  âŒ WRONG: {sym} - {result['notes']}")
                elif outcome == "AMBIGUOUS":  ambiguous += 1; log.info(f"  ðŸ”¶ AMBIGUOUS: {sym} - {result['notes']}")
                elif outcome == "NOT_FOUND":  not_found += 1; log.info(f"  â“ NOT_FOUND: {sym}")
                elif outcome == "ERROR":      errors    += 1; log.warning(f"  ðŸ’¥ ERROR: {sym} - {result['notes']}")

                if i % 50 == 0 or i == len(candidates):
                    log.info(f"  Progress: {i}/{len(candidates)} | "
                             f"âœ…{confirmed} âŒ{wrong} ðŸ”¶{ambiguous} â“{not_found}")

        finally:
            try: page.close()
            except Exception: pass
            try: browser.close()
            except Exception: pass

    # Save audit report
    report_path = SCRIPT_DIR / "sedar_party_audit.csv"
    report_fields = ["symbol", "outcome", "known", "top_pnum", "top_score", "notes", "candidates"]
    with open(report_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=report_fields, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            r["candidates"] = _json.dumps(r.get("candidates", []))
            writer.writerow(r)

    log.info("=" * 60)
    log.info(f"AUDIT COMPLETE: {len(candidates)} companies checked")
    log.info(f"  âœ… CONFIRMED:  {confirmed}")
    log.info(f"  âŒ WRONG:      {wrong}")
    log.info(f"  ðŸ”¶ AMBIGUOUS:  {ambiguous}  (need filing date to confirm)")
    log.info(f"  â“ NOT_FOUND:  {not_found}  (name changed on SEDAR?)")
    log.info(f"  ðŸ’¥ ERROR:      {errors}")
    log.info(f"  Report saved:  {report_path.name}")
    log.info("=" * 60)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier",    type=str, default="",  help="Only process this tier (1, 2, or 3)")
    ap.add_argument("--limit",   type=int, default=0)
    ap.add_argument("--symbol",  type=str, default="")
    ap.add_argument("--recheck", action="store_true",
                    help="Audit mode: check all stored party numbers for correctness (read-only)")
    args = ap.parse_args()

    now = datetime.now(TORONTO_TZ)
    log.info("=" * 60)
    log.info(f"SEDAR PARTY FINDER - {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    log.info("=" * 60)

    universe = load_universe()
    if not universe:
        log.error("canadian_universe.csv not found or empty")
        return

    if args.recheck:
        run_recheck(universe, symbol_filter=args.symbol, limit=args.limit)
        elapsed = (datetime.now(TORONTO_TZ) - now).total_seconds()
        log.info(f"DONE in {elapsed:.0f}s")
        return
    already  = load_parties()
    # Also respect party numbers already in universe_results.csv
    for row in universe:
        sym = row.get("symbol", "")
        if sym not in already and row.get("sedar_party_number"):
            already[sym] = {"sedar_party_number": row["sedar_party_number"],
                             "sedar_party_name":   row.get("sedar_party_name", "")}

    log.info(f"Universe: {len(universe)} companies | Already known: {len(already)} party numbers")

    # Build candidate list: in_universe=Y companies without a real party number
    # Also retry NOT_FOUND entries (they may have been added to SEDAR+ since last run)
    have_real = {sym for sym, data in already.items()
                 if data.get("sedar_party_number","") not in ("", "NOT_FOUND")}

    candidates = []
    for tier in ["1", "2", "3"]:
        if args.tier and args.tier != tier:
            continue
        for row in universe:
            sym = row.get("symbol", "")
            if row.get("tier") != tier:
                continue
            if sym in have_real:
                continue
            if args.symbol and sym.upper() != args.symbol.upper():
                continue
            # Only process in-universe companies (or all if no filter)
            if not args.symbol and row.get("in_universe") != "Y":
                continue
            candidates.append(row)

    if args.limit:
        candidates = candidates[:args.limit]

    tier1_count = sum(1 for r in candidates if r.get("tier") == "1")
    log.info(f"To process: {len(candidates)} ({tier1_count} Tier 1 first)")

    if not candidates:
        log.info("Nothing to do - all companies already have party numbers")
        return

    if not _ensure_cdp_ready():
        log.error("CDP not available - cannot run")
        return

    # Check SEDAR+ is accessible
    try:
        r = requests.get(f"{SEDAR_BASE}/csa-party/", timeout=10)
        if "maintenance" in r.text.lower():
            log.error("SEDAR+ appears to be under maintenance - try again later")
            return
        log.info("SEDAR+ accessible")
    except Exception as e:
        log.error(f"SEDAR+ check failed: {e}")
        return

    from playwright.sync_api import sync_playwright
    found = failed = 0

    # Build symbol -> universe row map for writeback
    universe_map = {r["symbol"]: r for r in universe}
    universe_fields = list(universe[0].keys()) if universe else []

    with sync_playwright() as pw:
        browser = pw.chromium.connect_over_cdp(CDP_URL)
        ctx     = browser.contexts[0]
        page    = ctx.new_page()
        try:
            for i, row in enumerate(candidates, 1):
                sym  = row.get("symbol", "")
                name = row.get("name", "")
                tier = row.get("tier", "")
                exch = row.get("exchange", "")

                pnum, pname = lookup_party(page, sym, name)
                if pnum:
                    save_party(sym, name, exch, tier, pnum, pname)
                    # Write back to universe_results.csv immediately
                    if sym in universe_map:
                        universe_map[sym]["sedar_party_number"] = pnum
                        universe_map[sym]["sedar_party_name"]   = pname
                    found += 1
                else:
                    save_party(sym, name, exch, tier, "NOT_FOUND", "")
                    failed += 1

                if i % 10 == 0 or i == len(candidates):
                    log.info(f"Progress: {i}/{len(candidates)} | found={found} failed={failed}")
                    # Checkpoint: save universe_results.csv every 10 companies
                    if universe_fields:
                        with open(RESULTS_CSV, "w", newline="", encoding="utf-8-sig") as f:
                            writer = csv.DictWriter(f, fieldnames=universe_fields, extrasaction="ignore")
                            writer.writeheader()
                            writer.writerows(universe)
                        log.info(f"  Checkpoint: universe_results.csv saved")

        finally:
            try: page.close()
            except Exception: pass
            try: browser.close()
            except Exception: pass

    # Final save of universe_results.csv
    if universe_fields:
        with open(RESULTS_CSV, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=universe_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(universe)
        log.info(f"universe_results.csv updated with {found} new party numbers")

    log.info("=" * 60)
    log.info(f"DONE: {found} found, {failed} not found")
    log.info(f"Results in: {PARTIES_CSV.name}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
