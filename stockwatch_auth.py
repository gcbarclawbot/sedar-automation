"""
stockwatch_auth.py - Shared Stockwatch session management
==========================================================
Single source of truth for Stockwatch cookie retrieval and validation.
Used by: universe_builder.py, canadian_batch_run.py, mm_onboarding.py

One canonical session file:
  2. Canadian Batch Run/stockwatch_session.json

All three scripts read/write to this same file so a login by any one
of them refreshes the session for all others.
"""

import json
import logging
import requests
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# Canonical session file location - shared by all scripts
_SEDAR_BASE = Path(__file__).parent
SESSION_PATH = _SEDAR_BASE / "2. Canadian Batch Run" / "stockwatch_session.json"

SW_SEDAR_URL = "https://www.stockwatch.com/News/Sedar"
CDP_URL = "http://127.0.0.1:18800"

CREDENTIALS_PATH = Path(r"C:\Users\Admin\.openclaw\credentials\stockwatch.env")


def _load_credentials() -> tuple[str, str]:
    username = password = ""
    if CREDENTIALS_PATH.exists():
        for line in CREDENTIALS_PATH.read_text(encoding="utf-8").splitlines():
            if line.startswith("STOCKWATCH_USERNAME="):
                username = line.split("=", 1)[1].strip()
            elif line.startswith("STOCKWATCH_PASSWORD="):
                password = line.split("=", 1)[1].strip()
    return username, password


def test_session(cookies: dict) -> bool:
    """
    Returns True if these cookies yield an authenticated Stockwatch session.
    Checks for VIEWSTATE presence (only on authenticated search page).
    """
    try:
        sess = requests.Session()
        sess.headers.update({"User-Agent": "Mozilla/5.0"})
        for k, v in cookies.items():
            sess.cookies.set(k, v, domain="www.stockwatch.com")
        r = sess.get(SW_SEDAR_URL, timeout=15)
        body = r.text
        if "NotLoggedIn" in body or "PowerUserName" in body:
            return False
        soup = BeautifulSoup(body, "html.parser")
        vs = (soup.find("input", {"id": "__VIEWSTATE"}) or {}).get("value", "")
        return bool(vs)
    except Exception:
        return False


def _cache(cookies: dict):
    SESSION_PATH.parent.mkdir(parents=True, exist_ok=True)
    SESSION_PATH.write_text(
        json.dumps({"cookies": cookies, "saved_at": datetime.now().isoformat()}),
        encoding="utf-8"
    )


def _browser_login(page) -> bool:
    """Log in via CDP browser page. Returns True on success."""
    username, password = _load_credentials()
    if not username or not password:
        log.warning("  Stockwatch credentials not found - cannot auto-login")
        return False
    try:
        log.info("  Stockwatch session expired - attempting auto-login via browser...")
        page.goto("https://www.stockwatch.com/User/NotLoggedIn",
                  wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(1500)
        page.locator("input[name='ctl00$PowerUserName']").first.fill(username)
        page.locator("input[name='ctl00$PowerPassword']").first.fill(password)
        page.locator("input[name='ctl00$Login']").first.click()
        page.wait_for_timeout(2500)
        if "NotLoggedIn" in page.url or "notloggedin" in page.url.lower():
            log.warning("  Auto-login failed - still on NotLoggedIn page")
            return False
        log.info(f"  Auto-login successful (redirected to {page.url[:60]})")
        return True
    except Exception as e:
        log.warning(f"  Auto-login error: {e}")
        return False


def get_cookies() -> dict:
    """
    Get valid Stockwatch cookies. Strategy:
      1. Extract from CDP browser - test if valid
      2. If not valid, auto-login via CDP browser, re-extract
      3. Fall back to cached session file (with validation)
      4. Raise if nothing works

    Always writes a valid session back to the canonical SESSION_PATH.
    """
    from playwright.sync_api import sync_playwright

    def _extract(ctx) -> dict:
        return {
            c["name"]: c["value"]
            for c in ctx.cookies()
            if "stockwatch.com" in c.get("domain", "")
        }

    # Ensure CDP is reachable
    import urllib.request, subprocess, time as _time
    def _cdp_alive() -> bool:
        try:
            urllib.request.urlopen(f"{CDP_URL}/json", timeout=2)
            return True
        except Exception:
            return False

    if not _cdp_alive():
        log.info("  CDP not reachable - launching OpenClaw browser...")
        try:
            subprocess.Popen(["openclaw", "browser", "start"], shell=True,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass
        deadline = _time.time() + 30
        while _time.time() < deadline:
            _time.sleep(2)
            if _cdp_alive():
                break

    try:
        log.info("  Getting Stockwatch cookies from browser (CDP)...")
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(CDP_URL)
            ctx = browser.contexts[0]
            cookies = _extract(ctx)

            if not test_session(cookies):
                log.info("  Browser session not authenticated - attempting auto-login...")
                page = ctx.new_page()
                try:
                    if _browser_login(page):
                        page.wait_for_timeout(1500)
                        cookies = _extract(ctx)
                finally:
                    page.close()

            browser.close()

        if test_session(cookies):
            log.info(f"  Got {len(cookies)} Stockwatch cookies (session verified)")
            _cache(cookies)
            return cookies

        log.warning("  Browser login failed - falling back to saved session")

    except Exception as e:
        log.warning(f"  Browser cookie extract failed: {e} - trying saved session")

    # Fall back to cached file
    if SESSION_PATH.exists():
        try:
            data = json.loads(SESSION_PATH.read_text(encoding="utf-8"))
            cookies = data.get("cookies", {})
            if test_session(cookies):
                log.info(f"  Using saved session from {data.get('saved_at','?')} (verified)")
                return cookies
            else:
                log.warning(f"  Saved session from {data.get('saved_at','?')} is expired")
        except Exception:
            pass

    raise RuntimeError(
        "No valid Stockwatch session and auto-login failed. "
        "Check credentials at C:\\Users\\Admin\\.openclaw\\credentials\\stockwatch.env"
    )
