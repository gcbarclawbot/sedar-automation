"""
Refresh the SEDAR+ browser session by connecting to the OpenClaw browser via CDP.
Navigates to the SEDAR+ search page, extracts fresh cookies + session ID,
saves to browser_session.json for use by sedar_batch.py.

Run this whenever sedar_batch.py reports bot detection / session expired.
The OpenClaw browser on port 18800 already has valid ShieldSquare cookies.
"""

import json
import time
import re
from pathlib import Path
from playwright.sync_api import sync_playwright

CDP_URL = "http://127.0.0.1:18800"
SEARCH_URL = (
    "https://www.sedarplus.ca/csa-party/service/create.html"
    "?targetAppCode=csa-party&service=searchDocuments&_locale=en"
)
OUT_FILE = Path(__file__).parent / "browser_session.json"


def refresh():
    print("Connecting to OpenClaw browser via CDP...")
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP_URL)
        ctx = browser.contexts[0]

        # Find existing SEDAR+ page or open a new one
        sedar_page = None
        for pg in ctx.pages:
            if "sedarplus.ca" in pg.url:
                sedar_page = pg
                print(f"Found existing SEDAR+ page: {pg.url[:60]}")
                break

        if not sedar_page:
            print("No SEDAR+ tab open, creating one...")
            sedar_page = ctx.new_page()

        # Navigate to the search form
        print("Navigating to document search...")
        sedar_page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=30000)
        sedar_page.wait_for_selector("input#SubmissionDate", timeout=20000)
        time.sleep(1)

        final_url = sedar_page.url
        html = sedar_page.content()

        # Extract cookies from context
        raw_cookies = ctx.cookies()
        cookies = {
            c["name"]: c["value"]
            for c in raw_cookies
            if "sedarplus.ca" in c.get("domain", "")
        }

        # Extract session ID
        m = re.search(r"[?&]id=([a-f0-9]+)", final_url)
        if not m:
            print(f"ERROR: Could not find session ID in URL: {final_url}")
            return
        session_id = m.group(1)

        # Extract VIKEY
        m_vikey = re.search(r"viewInstanceKey:'([^']+)'", html)
        vikey = m_vikey.group(1) if m_vikey else ""

        # Extract x-catalyst-session-global from JS (set on the page as a cookie-like var)
        m_cat = re.search(r"x-catalyst-session-global['\"]?\s*[,:]\s*['\"]([a-f0-9]+)", html)
        catalyst_token = m_cat.group(1) if m_cat else cookies.get("x-catalyst-session-global", "")

        # Capture uzlc via JS evaluation
        uzlc = sedar_page.evaluate("() => window.__uzlc || ''")

        browser.close()

    result = {
        "sessionId": session_id,
        "vikey": vikey,
        "updateUrl": f"https://www.sedarplus.ca/csa-party/viewInstance/update.html?id={session_id}",
        "cookies": cookies,
        "catalystToken": catalyst_token,
        "uzlc": uzlc,
        "refreshed_at": str(__import__('datetime').datetime.now()),
    }

    OUT_FILE.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"\nSession saved to {OUT_FILE.name}")
    print(f"  Session ID: {session_id[:24]}...")
    print(f"  VIKEY:      {vikey[:20]}...")
    print(f"  Cookies:    {len(cookies)} keys: {list(cookies.keys())}")
    return result


if __name__ == "__main__":
    refresh()
