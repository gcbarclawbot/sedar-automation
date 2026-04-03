"""
canadian_master_sync.py - Canadian Listed Company Master Sync
=============================================================
Maintains canadian_universe.csv as the live, enriched record of all mining
universe companies. Uses the TMX Money GraphQL API and TSX directory feeds.

No more canadian_master.csv - the Daily/ snapshots ARE the full directory
history. This script works from the most recent snapshot and writes changes
directly to canadian_universe.csv.

Daily workflow:
  1. Load previous state from most recent Daily/ snapshot
  2. Pull current directory from TMX (all listed TSX + TSXV)
  3. Diff:
     - New listings -> if mining-relevant (Tier 1/2/3), add to universe as PENDING
     - Delistings / suspensions -> update status in universe CSV
     - Re-activations -> restore LISTED status in universe CSV
     - Name changes -> update in universe CSV
  4. Update market data (price, market cap etc.) for all LISTED universe companies
  5. Save full TMX snapshot to Daily/ (before + after)
  6. Log all universe changes to universe_sync_log.csv

Usage:
  python canadian_master_sync.py             # daily sync
  python canadian_master_sync.py --bootstrap # first run (builds snapshot from scratch)
"""

import sys
import csv
import json
import time
import logging
import traceback
import requests
import argparse
import shutil
import pytz
from datetime import datetime, date, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Paths & Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR    = Path(__file__).parent
UNIVERSE_CSV  = SCRIPT_DIR / "canadian_universe.csv"
UNIVERSE_LOG  = SCRIPT_DIR / "universe_sync_log.csv"
DAILY_DIR     = SCRIPT_DIR / "Daily"
LOG_PATH      = SCRIPT_DIR / "canadian_master_sync.log"

TORONTO_TZ    = pytz.timezone("America/Toronto")
TMX_DIR_BASE  = "https://www.tsx.com"
TMX_GQL       = "https://app-money.tmx.com/graphql"
TMX_HEADERS   = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "Origin": "https://money.tmx.com",
    "Referer": "https://money.tmx.com/",
}

DELIST_GRACE_DAYS  = 2
PRICE_BATCH_SIZE   = 50
PRICE_WORKERS      = 8
PROFILE_WORKERS    = 20

# Snapshot fieldnames - full directory (all companies, used for Daily/ snapshots only)
SNAPSHOT_FIELDNAMES = [
    "symbol", "name", "exchange", "status",
    "last_seen", "listed_date", "delisted_date", "pending_since",
    "sector", "industry", "market_cap", "description",
    "price", "price_change_pct", "last_trade_datetime",
    "shares_outstanding", "eps", "pe_ratio", "weeks52high", "weeks52low",
    "website", "full_address", "phone_number", "email", "employees",
    "beta", "price_to_book", "return_on_equity", "return_on_assets",
    "currency", "open_price", "day_high", "day_low",
    "total_debt_to_equity", "dividend_yield", "dividend_amount",
    "ex_dividend_date", "dividend_frequency",
    "mining_flag", "notes", "sedar_party_number", "sedar_party_name",
    "in_universe",  # Y / PENDING / N / blank (not yet classified)
]

# Universe fieldnames - superset: all classification fields + market data + status
UNIVERSE_FIELDNAMES = [
    # Identity
    "symbol", "name", "exchange",
    # Status tracking (kept in sync by this script)
    "status", "last_seen", "listed_date", "delisted_date", "pending_since",
    # TMX profile
    "sector", "industry", "market_cap", "description",
    # Market data (updated daily)
    "price", "price_change_pct", "last_trade_datetime",
    "shares_outstanding", "eps", "pe_ratio", "weeks52high", "weeks52low",
    "website", "full_address", "phone_number", "email", "employees",
    "beta", "price_to_book", "return_on_equity", "return_on_assets",
    "currency", "open_price", "day_high", "day_low",
    "total_debt_to_equity", "dividend_yield", "dividend_amount",
    "ex_dividend_date", "dividend_frequency",
    # SEDAR
    "sedar_party_number", "sedar_party_name",
    # Universe classification (from universe_builder.py)
    "tier", "company_type", "company_type_confidence",
    "primary_commodity", "primary_commodity_confidence",
    "adjacent_category", "reasoning",
    # Filing info
    "filing_type", "filing_date", "filing_doc_name", "filing_url",
    "filing_pdf_path", "filing_text_chars", "sw_symbol", "sedar_only",
    # MD&A cross-check
    "mda_type", "mda_date", "mda_doc_name", "mda_url", "mda_pdf_path", "mda_text_chars",
    "mda_company_type", "mda_primary_commodity", "mda_reasoning", "discrepancy",
    # Overrides
    "override_type", "override_commodity",
    # Sync metadata
    "universe_added_date", "universe_notes",
]

# Tier assignment (same logic as universe_builder.py)
TIER1_INDUSTRIES = {"Mining"}
TIER2_COMBOS = {
    ("Materials", "Steel"), ("Materials", "Chemicals"),
    ("Finance", "Asset Management Services"), ("Finance", "Capital Markets"),
    ("Finance", "Diversified Financial Services"),
    ("Industrials", "Industrial Goods"), ("Industrials", "Industrial Machinery"),
    ("Industrials", "Industrial Equipment Distributors"),
    ("Energy", "Other Energy Sources"), ("Energy", "Thermal Coal"),
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
# TMX Directory API
# ---------------------------------------------------------------------------
def fetch_tmx_dir(endpoint: str) -> list:
    url = f"{TMX_DIR_BASE}/json/company-directory/{endpoint}"
    try:
        r = requests.get(url, headers={"User-Agent": TMX_HEADERS["User-Agent"]}, timeout=15)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", data) if isinstance(data, dict) else data
        return results if isinstance(results, list) else []
    except Exception as e:
        log.warning(f"TMX dir fetch failed ({endpoint}): {e}")
        return []

def fetch_all_listed() -> dict:
    companies = {}
    for exchange in ("tsx", "tsxv"):
        results = fetch_tmx_dir(f"search/{exchange}/^")
        ex_label = "TSX" if exchange == "tsx" else "TSXV"
        for item in results:
            sym  = (item.get("symbol") or "").strip()
            name = (item.get("name") or "").strip()
            if sym:
                companies[sym] = {"symbol": sym, "name": name, "exchange": ex_label}
        log.info(f"  Directory: {len(results)} {ex_label} companies")
    return companies

def fetch_recent_events() -> dict:
    events = {}
    for exchange, ex_label in (("tsx","TSX"), ("tsxv","TSXV")):
        for event_type in ("recent", "delisted", "suspended"):
            for item in fetch_tmx_dir(f"{event_type}/{exchange}"):
                sym = (item.get("symbol") or "").strip()
                ts  = item.get("date")
                dt  = datetime.fromtimestamp(ts, tz=TORONTO_TZ).strftime("%Y-%m-%d") if ts else ""
                if sym:
                    events[sym] = {
                        "symbol": sym, "name": (item.get("name") or "").strip(),
                        "exchange": ex_label, "event": event_type, "event_date": dt,
                    }
    return events

# ---------------------------------------------------------------------------
# TMX GraphQL API
# ---------------------------------------------------------------------------
GQL_FULL_PROFILE = """
query getQuoteBySymbol($symbol:String,$locale:String){
    getQuoteBySymbol(symbol:$symbol,locale:$locale){
        symbol name price priceChange percentChange volume
        MarketCap exchangeName exchangeCode
        sector industry longDescription shortDescription
        eps peRatio shareOutStanding
        weeks52high weeks52low close prevClose datetime
        website fullAddress phoneNumber email employees
        beta priceToBook returnOnEquity returnOnAssets
        currency openPrice dayHigh dayLow
        totalDebtToEquity dividendYield dividendAmount
        exDividendDate dividendFrequency
    }
}"""

GQL_PRICE_BATCH = """
query getQuoteForSymbols($symbols:[String]){
    getQuoteForSymbols(symbols:$symbols){
        symbol longname price priceChange percentChange volume exchange
        weeks52high weeks52low prevClose
    }
}"""

def _gql(operation: str, variables: dict, query: str, retries: int = 2) -> tuple:
    for attempt in range(retries + 1):
        try:
            r = requests.post(
                TMX_GQL,
                json={"operationName": operation, "variables": variables, "query": query},
                headers=TMX_HEADERS, timeout=15
            )
            if r.status_code == 429:
                wait = 5 * (attempt + 1)
                log.warning(f"TMX rate limited - waiting {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            d = r.json()
            return d.get("data"), d.get("errors", [])
        except requests.exceptions.Timeout:
            if attempt < retries: time.sleep(1)
        except requests.exceptions.ConnectionError:
            if attempt < retries: time.sleep(2)
        except Exception:
            break
    return None, [{"message": f"failed after {retries+1} attempts"}]

def fetch_full_profile(symbol: str) -> dict:
    try:
        data, errors = _gql("getQuoteBySymbol", {"symbol": symbol, "locale": "en"}, GQL_FULL_PROFILE)
        return (data or {}).get("getQuoteBySymbol") or {}
    except Exception:
        return {}

def fetch_prices_batch(symbols: list) -> dict:
    try:
        data, _ = _gql("getQuoteForSymbols", {"symbols": symbols}, GQL_PRICE_BATCH)
        results = (data or {}).get("getQuoteForSymbols") or []
        return {r.get("symbol", ""): r for r in results if r.get("symbol")}
    except Exception:
        return {}

# ---------------------------------------------------------------------------
# Snapshot helpers (Daily/ - full directory, not the universe)
# ---------------------------------------------------------------------------
def load_latest_snapshot() -> dict:
    """Load most recent 'after' snapshot from Daily/. Returns {symbol: row}."""
    if not DAILY_DIR.exists():
        return {}
    # Find most recent after snapshot
    candidates = sorted(
        DAILY_DIR.rglob("*snapshot_after*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    # Also check FINAL snapshot
    finals = sorted(
        DAILY_DIR.rglob("*FINAL*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    all_snaps = sorted(candidates + finals, key=lambda p: p.stat().st_mtime, reverse=True)
    if not all_snaps:
        log.warning("No existing snapshot found in Daily/")
        return {}
    snap = all_snaps[0]
    log.info(f"  Loading snapshot: {snap.name}")
    with open(snap, newline="", encoding="utf-8-sig", errors="replace") as f:
        rows = list(csv.DictReader(f))
    return {r["symbol"]: r for r in rows}

def save_snapshot(rows: list, run_dir: Path, label: str, today_str: str,
                  universe_by_sym: dict = None):
    """Save full directory snapshot to Daily/. Stamps in_universe from universe if provided."""
    now_str   = datetime.now(TORONTO_TZ).strftime("%Y-%m-%d_%H%M")
    dest_name = f"canadian_master_snapshot_{label}_{now_str}.csv"
    dest      = run_dir / dest_name
    with open(dest, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=SNAPSHOT_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            snap_row = dict(row)
            if universe_by_sym is not None:
                u = universe_by_sym.get(snap_row.get("symbol",""), {})
                snap_row["in_universe"] = "Y" if u else ""
            writer.writerow(snap_row)
    log.info(f"Snapshot saved: {dest_name}")

# ---------------------------------------------------------------------------
# Universe CSV helpers
# ---------------------------------------------------------------------------
def load_universe() -> tuple:
    """Load canadian_universe.csv. Returns (rows, fieldnames)."""
    if not UNIVERSE_CSV.exists():
        return [], UNIVERSE_FIELDNAMES
    with open(UNIVERSE_CSV, newline="", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)
        fields = reader.fieldnames or UNIVERSE_FIELDNAMES
    # Ensure all expected fields present
    for r in rows:
        for f in UNIVERSE_FIELDNAMES:
            if f not in r:
                r[f] = ""
        # Rows loaded from CSV without in_universe column are existing universe members
        if not r.get("in_universe"):
            r["in_universe"] = "Y"
    return rows, UNIVERSE_FIELDNAMES

def save_universe(rows: list):
    with open(UNIVERSE_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=UNIVERSE_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

def append_universe_log(entries: list):
    if not entries:
        return
    fieldnames = ["date", "symbol", "name", "exchange", "event", "action",
                  "old_value", "new_value", "notes"]
    write_header = not UNIVERSE_LOG.exists()
    with open(UNIVERSE_LOG, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(entries)

def _apply_profile_to_row(row: dict, profile: dict):
    if not profile:
        return
    row["sector"]              = profile.get("sector") or row.get("sector", "")
    row["industry"]            = profile.get("industry") or row.get("industry", "")
    row["market_cap"]          = profile.get("MarketCap") or row.get("market_cap", "")
    row["description"]         = (profile.get("longDescription") or profile.get("shortDescription") or row.get("description",""))[:500]
    row["price"]               = profile.get("price") or row.get("price", "")
    row["price_change_pct"]    = profile.get("percentChange") or row.get("price_change_pct", "")
    row["last_trade_datetime"] = profile.get("datetime") or row.get("last_trade_datetime", "")
    row["shares_outstanding"]  = profile.get("shareOutStanding") or row.get("shares_outstanding", "")
    row["eps"]                 = profile.get("eps") or row.get("eps", "")
    row["pe_ratio"]            = profile.get("peRatio") or row.get("pe_ratio", "")
    row["weeks52high"]         = profile.get("weeks52high") or row.get("weeks52high", "")
    row["weeks52low"]          = profile.get("weeks52low") or row.get("weeks52low", "")
    # New fields
    row["website"]             = profile.get("website") or row.get("website", "")
    row["full_address"]        = profile.get("fullAddress") or row.get("full_address", "")
    row["phone_number"]        = profile.get("phoneNumber") or row.get("phone_number", "")
    row["email"]               = profile.get("email") or row.get("email", "")
    row["employees"]           = profile.get("employees") or row.get("employees", "")
    row["beta"]                = profile.get("beta") or row.get("beta", "")
    row["price_to_book"]       = profile.get("priceToBook") or row.get("price_to_book", "")
    row["return_on_equity"]    = profile.get("returnOnEquity") or row.get("return_on_equity", "")
    row["return_on_assets"]    = profile.get("returnOnAssets") or row.get("return_on_assets", "")
    row["currency"]            = profile.get("currency") or row.get("currency", "")
    row["open_price"]          = profile.get("openPrice") or row.get("open_price", "")
    row["day_high"]            = profile.get("dayHigh") or row.get("day_high", "")
    row["day_low"]             = profile.get("dayLow") or row.get("day_low", "")
    row["total_debt_to_equity"]= profile.get("totalDebtToEquity") or row.get("total_debt_to_equity", "")
    row["dividend_yield"]      = profile.get("dividendYield") or row.get("dividend_yield", "")
    row["dividend_amount"]     = profile.get("dividendAmount") or row.get("dividend_amount", "")
    row["ex_dividend_date"]    = profile.get("exDividendDate") or row.get("ex_dividend_date", "")
    row["dividend_frequency"]  = profile.get("dividendFrequency") or row.get("dividend_frequency", "")

def _apply_price_to_row(row: dict, quote: dict):
    if not quote:
        return
    row["price"]            = quote.get("price") or row.get("price", "")
    row["price_change_pct"] = quote.get("percentChange") or row.get("price_change_pct", "")
    row["weeks52high"]      = quote.get("weeks52high") or row.get("weeks52high", "")
    row["weeks52low"]       = quote.get("weeks52low") or row.get("weeks52low", "")

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
def bootstrap(run_dir: Path, today_str: str):
    log.info("BOOTSTRAP: building snapshot from TMX directory + full enrichment")
    listed = fetch_all_listed()
    symbols = list(listed.keys())
    log.info(f"  {len(symbols)} companies to enrich...")

    def _fetch(sym):
        return sym, fetch_full_profile(sym)

    profiles = {}
    done = 0
    with ThreadPoolExecutor(max_workers=PROFILE_WORKERS) as pool:
        futures = {pool.submit(_fetch, sym): sym for sym in symbols}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                _, profile = future.result()
                if profile:
                    profiles[sym] = profile
            except Exception:
                pass
            done += 1
            if done % 200 == 0 or done == len(symbols):
                log.info(f"  Enriched: {done}/{len(symbols)}")

    rows = []
    for sym, info in listed.items():
        row = {k: "" for k in SNAPSHOT_FIELDNAMES}
        row.update({"symbol": sym, "name": info["name"], "exchange": info["exchange"],
                    "status": "LISTED", "last_seen": today_str})
        if sym in profiles:
            _apply_profile_to_row(row, profiles[sym])
            if profiles[sym].get("name"):
                row["name"] = profiles[sym]["name"]
        rows.append(row)

    rows.sort(key=lambda r: (r["exchange"], r["symbol"]))
    save_snapshot(rows, run_dir, "bootstrap", today_str)
    log.info(f"Bootstrap complete: {len(rows)} companies saved to snapshot")
    return {r["symbol"]: r for r in rows}

# ---------------------------------------------------------------------------
# Daily sync
# ---------------------------------------------------------------------------
def daily_sync(run_dir: Path, today_str: str):
    # Load previous state from most recent snapshot
    prev = load_latest_snapshot()
    if not prev:
        log.warning("No previous snapshot found - running bootstrap")
        prev = bootstrap(run_dir, today_str)

    log.info(f"Previous snapshot: {len(prev)} companies")

    # Load universe
    universe_rows, _ = load_universe()
    universe_by_sym  = {r["symbol"]: r for r in universe_rows}
    log.info(f"Universe: {len(universe_by_sym)} companies")

    # Save snapshot of current universe state before any changes
    save_snapshot(list(prev.values()), run_dir, "before", today_str, universe_by_sym)

    # Fetch current TMX state
    log.info("Fetching TMX directory...")
    tmx_listed = fetch_all_listed()
    tmx_events = fetch_recent_events()

    # Build current snapshot (start from prev, apply changes)
    current = {sym: dict(row) for sym, row in prev.items()}

    log_entries   = []
    univ_changes  = 0

    def _log(sym, name, exch, event, action, old_val="", new_val="", notes=""):
        log_entries.append({
            "date": today_str, "symbol": sym, "name": name, "exchange": exch,
            "event": event, "action": action,
            "old_value": old_val, "new_value": new_val, "notes": notes,
        })

    # ---- 1. New listings ----
    to_enrich = []
    for sym, info in tmx_listed.items():
        name = info["name"]
        exch = info["exchange"]
        if sym not in current:
            # Brand new listing - add to snapshot
            row = {k: "" for k in SNAPSHOT_FIELDNAMES}
            row.update({"symbol": sym, "name": name, "exchange": exch,
                        "status": "LISTED", "last_seen": today_str,
                        "listed_date": tmx_events.get(sym, {}).get("event_date", today_str)})
            current[sym] = row
            to_enrich.append(sym)
            log.info(f"  NEW LISTING: {sym} - {name} ({exch})")
            _log(sym, name, exch, "NEW_LISTING", "ADDED_TO_SNAPSHOT",
                 notes=f"listed_date={row['listed_date']}")

            # Check if mining-relevant -> add to universe as PENDING
            tier = assign_tier(row)
            if tier in ("1","2","3"):
                univ_row = {k: "" for k in UNIVERSE_FIELDNAMES}
                univ_row.update({
                    "symbol": sym, "name": name, "exchange": exch,
                    "status": "LISTED", "last_seen": today_str,
                    "listed_date": row["listed_date"],
                    "tier": tier,
                    "in_universe": "PENDING",
                    "universe_added_date": today_str,
                    "universe_notes": f"New {exch} listing, Tier {tier} - awaiting LLM classification",
                })
                universe_by_sym[sym] = univ_row
                univ_changes += 1
                log.info(f"  UNIVERSE PENDING: {sym} ({exch}) Tier {tier} - new listing")
                _log(sym, name, exch, "UNIVERSE_NEW_LISTING", "ADDED_PENDING",
                     new_val=f"PENDING/Tier{tier}",
                     notes="Mining-relevant new listing - needs LLM classification")
        else:
            row = current[sym]
            row["last_seen"] = today_str

            # Re-activation
            if row.get("status") != "LISTED":
                old_status = row["status"]
                row["status"]        = "LISTED"
                row["delisted_date"] = ""
                row["pending_since"] = ""
                to_enrich.append(sym)
                log.info(f"  RE-LISTED: {sym} (was {old_status})")
                _log(sym, row["name"], row["exchange"], "RE_LISTED",
                     f"STATUS_{old_status}_TO_LISTED", old_val=old_status, new_val="LISTED")
                if sym in universe_by_sym:
                    universe_by_sym[sym]["status"]        = "LISTED"
                    universe_by_sym[sym]["delisted_date"] = ""
                    universe_by_sym[sym]["pending_since"] = ""
                    universe_by_sym[sym]["last_seen"]     = today_str
                    univ_changes += 1
                    _log(sym, row["name"], row["exchange"], "UNIVERSE_RE_LISTED",
                         "STATUS_RESTORED", old_val=old_status, new_val="LISTED")

            # Name change
            if name and row["name"] != name:
                old_name = row["name"]
                row["name"] = name
                to_enrich.append(sym)
                log.info(f"  NAME CHANGE: {sym} '{old_name}' -> '{name}'")
                _log(sym, name, row["exchange"], "NAME_CHANGE", "SNAPSHOT_UPDATED",
                     old_val=old_name, new_val=name)
                if sym in universe_by_sym:
                    universe_by_sym[sym]["name"] = name
                    univ_changes += 1
                    _log(sym, name, row["exchange"], "UNIVERSE_NAME_CHANGE", "UNIVERSE_UPDATED",
                         old_val=old_name, new_val=name)

            # Exchange change
            if row["exchange"] != exch:
                old_ex = row["exchange"]
                row["exchange"] = exch
                _log(sym, row["name"], exch, "EXCHANGE_CHANGE", "SNAPSHOT_UPDATED",
                     old_val=old_ex, new_val=exch)
                if sym in universe_by_sym:
                    universe_by_sym[sym]["exchange"] = exch
                    univ_changes += 1
                    _log(sym, row["name"], exch, "UNIVERSE_EXCHANGE_CHANGE", "UNIVERSE_UPDATED",
                         old_val=old_ex, new_val=exch)

            # Clear stale pending
            if row.get("pending_since"):
                row["pending_since"] = ""

    # ---- 2. Delistings / suspensions ----
    for sym, row in current.items():
        if row.get("status") != "LISTED" or sym in tmx_listed:
            continue
        event_info = tmx_events.get(sym, {})
        event_type = event_info.get("event", "")

        if event_type in ("delisted", "suspended"):
            new_status = "SUSPENDED" if event_type == "suspended" else "DELISTED"
            row["status"]        = new_status
            row["delisted_date"] = event_info.get("event_date", today_str)
            row["pending_since"] = ""
            log.info(f"  {new_status}: {sym} - {row['name']}")
            _log(sym, row["name"], row["exchange"], new_status, f"STATUS_TO_{new_status}",
                 old_val="LISTED", new_val=new_status,
                 notes=event_info.get("event_date",""))
            if sym in universe_by_sym:
                universe_by_sym[sym]["status"]        = new_status
                universe_by_sym[sym]["delisted_date"] = row["delisted_date"]
                universe_by_sym[sym]["pending_since"] = ""
                universe_by_sym[sym]["last_seen"]     = today_str
                univ_changes += 1
                _log(sym, row["name"], row["exchange"], f"UNIVERSE_{new_status}",
                     f"UNIVERSE_STATUS_TO_{new_status}",
                     old_val="LISTED", new_val=new_status)
        else:
            pending_since = (row.get("pending_since") or "").strip()
            if not pending_since:
                row["pending_since"] = today_str
            else:
                try:
                    days = (date.fromisoformat(today_str) - date.fromisoformat(pending_since)).days
                except ValueError:
                    days = 0
                if days >= DELIST_GRACE_DAYS:
                    row["status"]        = "DELISTED"
                    row["delisted_date"] = today_str
                    row["pending_since"] = ""
                    log.info(f"  DELISTED (grace {days}d): {sym} - {row['name']}")
                    _log(sym, row["name"], row["exchange"], "DELISTED",
                         f"STATUS_TO_DELISTED_GRACE_{days}d",
                         old_val="LISTED", new_val="DELISTED",
                         notes=f"pending_since={pending_since}")
                    if sym in universe_by_sym:
                        universe_by_sym[sym]["status"]        = "DELISTED"
                        universe_by_sym[sym]["delisted_date"] = today_str
                        universe_by_sym[sym]["pending_since"] = ""
                        universe_by_sym[sym]["last_seen"]     = today_str
                        univ_changes += 1
                        _log(sym, row["name"], row["exchange"],
                             "UNIVERSE_DELISTED", "UNIVERSE_STATUS_TO_DELISTED",
                             old_val="LISTED", new_val="DELISTED")

    # ---- 3. Enrich new/changed companies ----
    to_enrich = list(set(to_enrich))
    if to_enrich:
        log.info(f"Enriching {len(to_enrich)} new/changed companies...")
        for sym in to_enrich:
            profile = fetch_full_profile(sym)
            if profile:
                _apply_profile_to_row(current[sym], profile)
                if sym in universe_by_sym:
                    _apply_profile_to_row(universe_by_sym[sym], profile)

    # ---- 4. Update market data for LISTED universe companies only ----
    listed_univ_syms = [sym for sym, r in universe_by_sym.items()
                        if r.get("status","LISTED") == "LISTED"]
    log.info(f"Updating market data for {len(listed_univ_syms)} listed universe companies...")

    # 4a. Batch price updates
    batches = [listed_univ_syms[i:i+PRICE_BATCH_SIZE]
               for i in range(0, len(listed_univ_syms), PRICE_BATCH_SIZE)]
    all_quotes = {}
    done = 0
    with ThreadPoolExecutor(max_workers=PRICE_WORKERS) as pool:
        futures = {pool.submit(fetch_prices_batch, batch): batch for batch in batches}
        for future in as_completed(futures):
            try:
                all_quotes.update(future.result())
            except Exception as e:
                log.warning(f"  Price batch failed: {e}")
            done += 1
            if done % 10 == 0 or done == len(batches):
                log.info(f"  Prices: {done}/{len(batches)} batches")

    price_ok = price_fail = 0
    for sym in listed_univ_syms:
        if sym in all_quotes:
            _apply_price_to_row(universe_by_sym[sym], all_quotes[sym])
            price_ok += 1
        else:
            price_fail += 1
    log.info(f"Prices done: {price_ok} ok, {price_fail} failed")

    # 4b. Full profile (market cap, sector, description) for universe companies
    log.info(f"Updating full profiles for {len(listed_univ_syms)} universe companies "
             f"({PROFILE_WORKERS} workers)...")

    def _fetch_profile(sym):
        return sym, fetch_full_profile(sym)

    profile_results = {}
    done = 0
    with ThreadPoolExecutor(max_workers=PROFILE_WORKERS) as pool:
        futures = {pool.submit(_fetch_profile, sym): sym for sym in listed_univ_syms}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                _, profile = future.result()
                if profile:
                    profile_results[sym] = profile
            except Exception as e:
                log.warning(f"  Profile failed for {sym}: {e}")
            done += 1
            if done % 100 == 0 or done == len(listed_univ_syms):
                log.info(f"  Profiles: {done}/{len(listed_univ_syms)}")

    for sym, profile in profile_results.items():
        if sym in universe_by_sym:
            _apply_profile_to_row(universe_by_sym[sym], profile)
            universe_by_sym[sym]["last_seen"] = today_str

    log.info(f"Profiles done: {len(profile_results)}/{len(listed_univ_syms)}")

    # ---- 5. Save everything ----
    updated_snapshot = sorted(current.values(), key=lambda r: (r.get("exchange",""), r.get("symbol","")))
    save_snapshot(updated_snapshot, run_dir, "after", today_str, universe_by_sym)

    updated_universe = sorted(
        (r for r in universe_by_sym.values() if r.get("in_universe") in ("Y","PENDING")),
        key=lambda r: (r.get("exchange",""), r.get("symbol",""))
    )
    save_universe(updated_universe)
    if log_entries:
        append_universe_log(log_entries)

    listed_count    = sum(1 for r in updated_snapshot if r.get("status") == "LISTED")
    delisted_count  = sum(1 for r in updated_snapshot if r.get("status") == "DELISTED")
    suspended_count = sum(1 for r in updated_snapshot if r.get("status") == "SUSPENDED")
    pending_count   = sum(1 for r in updated_universe if r.get("in_universe") == "PENDING")
    in_univ_count   = sum(1 for r in updated_universe if r.get("in_universe") == "Y")

    log.info(f"Snapshot: {listed_count} listed | {delisted_count} delisted | {suspended_count} suspended")
    log.info(f"Universe: {in_univ_count} in universe | {pending_count} pending classification | {univ_changes} changes this run")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", action="store_true")
    args = ap.parse_args()

    now_toronto = datetime.now(TORONTO_TZ)
    today_str   = now_toronto.strftime("%Y-%m-%d")
    run_dir     = DAILY_DIR / today_str / now_toronto.strftime("%H%M")
    run_dir.mkdir(parents=True, exist_ok=True)

    run_handler = logging.FileHandler(run_dir / "log.txt", encoding="utf-8")
    run_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(run_handler)

    log.info("=" * 70)
    log.info(f"CANADIAN MASTER SYNC - {'BOOTSTRAP' if args.bootstrap else 'DAILY'} - {now_toronto.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    log.info(f"  Run dir: {run_dir}")
    log.info("=" * 70)

    try:
        if args.bootstrap:
            bootstrap(run_dir, today_str)
        else:
            daily_sync(run_dir, today_str)

        elapsed = (datetime.now(TORONTO_TZ) - now_toronto).total_seconds()
        log.info("=" * 70)
        log.info(f"DONE in {elapsed:.0f}s")
        log.info("=" * 70)

    except Exception as e:
        tb = traceback.format_exc()
        log.error("=" * 70)
        log.error(f"SYNC FAILED: {type(e).__name__}: {e}")
        for line in tb.splitlines():
            log.error(f"  {line}")
        log.error("=" * 70)
        sys.exit(1)
    finally:
        log.removeHandler(run_handler)
        run_handler.close()


if __name__ == "__main__":
    main()
