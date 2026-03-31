"""
Extract the live session state from the existing SEDAR+ browser tab.
Reads BOTH open SEDAR+ pages and picks the best one (viewInstance with results).
"""
import json, re
from pathlib import Path
from playwright.sync_api import sync_playwright

CDP_URL = "http://127.0.0.1:18800"
OUT = Path(__file__).parent / "browser_session.json"

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP_URL)
    ctx = browser.contexts[0]

    # List all pages
    print("All open pages:")
    for pg in ctx.pages:
        print(f"  {pg.url[:90]}")

    # Pick the viewInstance page with the most loaded content (largest HTML = has results)
    candidates = [pg for pg in ctx.pages if "viewInstance/view.html" in pg.url and "sedarplus.ca" in pg.url]
    target = None
    if candidates:
        # Pick the one with most content (has search results loaded)
        best = max(candidates, key=lambda pg: len(pg.content()))
        target = best
        print(f"\nPicked: {target.url[:80]}")

    if not target:
        print("No viewInstance page found")
        browser.close()
        exit(1)

    # Get all cookies
    all_cookies = ctx.cookies()
    cookies = {c["name"]: c["value"] for c in all_cookies if "sedarplus.ca" in c.get("domain", "")}

    # Extract session ID from the URL
    session_id = re.search(r"[?&]id=([a-f0-9]+)", target.url)
    session_id = session_id.group(1) if session_id else ""

    # Get VIKEY from page content
    html = target.content()
    m_vikey = re.search(r"viewInstanceKey:'([^']+)'", html)
    vikey = m_vikey.group(1) if m_vikey else ""

    browser.close()

result = {
    "sessionId": session_id,
    "vikey": vikey,
    "updateUrl": f"https://www.sedarplus.ca/csa-party/viewInstance/update.html?id={session_id}",
    "cookies": cookies,
    "refreshed_at": str(__import__("datetime").datetime.now()),
}

OUT.write_text(json.dumps(result, indent=2), encoding="utf-8")
print(f"\nSaved to {OUT.name}")
print(f"  Session:  {session_id[:28]}...")
print(f"  VIKEY:    {vikey[:20]}...")
print(f"  Catalyst: {cookies.get('x-catalyst-session-global','')[:40]}")
print(f"  Cookies:  {len(cookies)}")
