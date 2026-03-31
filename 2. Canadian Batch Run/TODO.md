# SEDAR+ Scraper - TODO (as of 2026-03-28)

## What's Done
- `stockwatch_batch.py` - COMPLETE AND WORKING
  - Searches /News/Search by date, gets all Canadian news releases
  - Full article text saved as .txt files
  - 68 releases on Mar 27, runs in ~20 seconds
  - Session via CDP from OpenClaw browser

- `sedar_filings_batch.py` - SEARCH WORKS, DOWNLOAD BROKEN
  - Search fires through live browser tab, returns results correctly
  - Parses company name, filename, date, jurisdiction, file size, drm_key
  - CSV writes correctly
  - URL resolution and PDF download NOT working yet

## What's Left for sedar_filings_batch.py

### 1. Fix URL resolution (CRITICAL)
The `resource.html?node=W...&drmKey=...&drr=...` URL in each row IS the download URL.
It just needs the active session cookies. No need for Generate URL button at all.

The `drr=` param contains a session token. With valid session cookies (from CDP browser),
a GET to the resource.html URL should redirect to `records/document.html?id=<hash>` - 
that permanent hash IS the stable URL we want to store.

Fix: In `_parse_results()`, store `resource_href` as-is (absolute URL).
In `resolve_perm_urls()`, do a HEAD request with session cookies and capture `resp.url` 
after redirects - that will be the permanent `records/document.html?id=...` URL.

The resource.html URL already works for downloads (confirmed earlier in session).

### 2. Fix search wait logic
Current: `page.wait_for_timeout(5000)` - too fragile.
Better: Wait for the results table to appear or "Displaying" text:
```python
page.wait_for_selector("text=Displaying", timeout=15000)
```
Or wait for the XHR to complete:
```python
with page.expect_response(lambda r: "update.html" in r.url) as resp:
    page.evaluate("btn.click()")
resp.value.finished()
```

### 3. Session refresh
The SEDAR+ viewInstance session times out after ~30 minutes of inactivity.
Need to re-init session before searching if it's been idle.
Check: `page.evaluate("!!document.getElementById('SubmissionDate')")` - if False, session dead.

### 4. Separate output folders per script
Currently both scripts would write to Daily/YYYY-MM-DD/HHMM/
Proposal (already in filings script): filings go to Daily/YYYY-MM-DD/HHMM/filings/
News releases go to Daily/YYYY-MM-DD/HHMM/news/

### 5. Cron setup (both scripts)
- stockwatch_batch.py: run at ~10:30am Sydney (after ASX batch)
- sedar_filings_batch.py: run at ~10:45am Sydney

## Known Architecture
- Bot detection bypassed by using CDP-connected OpenClaw browser
- The live browser session on port 18800 has valid ShieldSquare cookies
- Search fires through page.evaluate() + button.click(), not raw POST
- Response captured via page.content() after wait
- PDFs download via requests session with CDP-extracted cookies
- Permanent URL: records/document.html?id=<64-hex> - session-independent

## Hybrid Backlog Approach (for future)
For building company filing history:
- Pre-2026-03-13: use Stockwatch /News/Sedar (doc search, lag is fine for historical)
- Post-2026-03-13: use direct SEDAR+ via CDP approach
- Company lookup: Stockwatch /News/Sedar?symbol=TGX&searchtype=C gives all filings per company
