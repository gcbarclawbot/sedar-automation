# Archive: Direct SEDAR+ Scraper (2026-03-28)

This folder contains the direct SEDAR+ scraping approach - archived when we switched
to the Stockwatch method which is simpler and has better industry filtering.

## Why archived (not deleted)
- Stockwatch is a paid service (21-day trial as of 2026-03-28)
- If the Stockwatch sub lapses, this direct approach can be restored
- Documents all the research into SEDAR+ internals

## What's here
- `sedar_batch.py` - Main scraper using requests + Playwright CDP
- `sedar_utils.py` - Helper utilities
- `sedar_refresh_session.py` - Refresh browser session via CDP
- `get_live_session.py` - Extract state from existing loaded browser tab  
- `test_via_playwright.py` - The working test that confirmed approach works
- `test_session.py`, `debug_search.py` - Debug scripts
- `browser_cookie_extract.py` - Manual cookie extraction via CDP
- `README.md` - Full documentation of the approach

## Key findings (do not lose these)
1. SEDAR+ uses ShieldSquare bot detection - blocks requests AND headless browsers
2. The ONLY working approach: connect Playwright to the existing OpenClaw browser
   (CDP port 18800), use the already-loaded SEDAR+ tab, trigger search via real DOM
3. Permanent PDF URL format: `records/document.html?id=<64-hex>` - session-independent
4. x-catalyst-session-global cookie is the auth token for POST requests
5. The search POST must come from the page's own JS (catCallback framework)
   NOT a raw requests POST - server state machine rejects external POSTs

## To restore
1. Copy all files from this folder back to the parent `8. SEDAR+/` folder
2. Run `python get_live_session.py` to get a fresh browser session
3. Run `python test_via_playwright.py` to verify connectivity
4. Complete `_parse_results()` in sedar_batch.py (parsing 500kb HTML blob)

## Status when archived
- Session init: WORKING
- Search POST via browser JS: WORKING (524kb clean response, no bot detection)
- Result HTML parsing: NOT COMPLETE (stopped here to switch to Stockwatch)
- PDF download: NOT TESTED (permanent URL download worked in isolation)
- Cron job: NOT SET UP
