# SEDAR+ News Release Scraper

Fetches news releases filed on SEDAR+ (Canadian securities filings) with the same folder structure and conventions as the other ASX automation scripts.

## Structure

```
8. SEDAR+/
├── config.toml              # Configuration file
├── sedar_batch.py            # Main scraper script
├── sedar_utils.py            # Utility functions  
├── state.json                # Last run state (auto-generated)
├── sedar_batch.log           # Log file
├── Daily/                    # Output folder
│   └── YYYY-MM-DD/
│       └── HHMM/            # Batch time (Sydney time)
│           ├── filings.csv   # Metadata for all filings
│           └── pdfs/         # Downloaded PDFs
└── _backups/                 # Script backups

```

## Usage

**Automatic mode** (uses state.json to fetch since last run):
```bash
python sedar_batch.py
```

**Manual date range**:
```bash
python sedar_batch.py 2026-03-27 2026-03-28
```

## Configuration (config.toml)

Key settings:
- `default_lookback_days`: How far back to search when no prior run exists
- `filing_type`: Set to "NEWS_RELEASES" 
- `jurisdictions`: Filter by province (e.g. ["British Columbia", "Ontario"])
- `company_name_keywords`: Filter by keywords (e.g. ["Mining", "Gold", "Exploration"])
- `error_notify`: WhatsApp numbers for error alerts

## Bot Detection Issue

SEDAR+ uses ShieldSquare/PerimeterX bot detection which blocks automated requests. 

**Current workaround**: The script uses Playwright to do an initial browser session, harvests the anti-bot cookies, then uses requests for all subsequent calls.

**If Playwright fails**: 
1. Run `browser_cookie_extract.py` to get cookies from your live browser session
2. Use those cookies with the script

**Alternative**: Use the browser-based approach in `sedar_interactive.py` which drives the full flow through Playwright.

## Output Format

**filings.csv columns**:
- `company_name`: Cleaned company name (English, no party number)
- `party_number`: 9-digit SEDAR+ party ID  
- `document_filename`: Original filename
- `submitted_date`: ISO format timestamp
- `jurisdiction`: Filing province
- `file_size`: Human readable size
- `perm_url`: Permanent `records/document.html?id=...` URL
- `pdf_downloaded`: yes/no/failed/no_url
- `pdf_path`: Relative path to downloaded PDF

**PDF naming**: `{Company Name} - {Document}.pdf` (cleaned for filesystem safety)

## Cron Integration

To run daily at 10:35am Sydney time (after ASX scripts):

```bash
openclaw cron add --job '{
  "name": "SEDAR+ News Releases", 
  "schedule": {"kind": "cron", "expr": "35 10 * * 1-5", "tz": "Australia/Sydney"},
  "payload": {"kind": "agentTurn", "message": "Run the SEDAR+ news release scraper"},
  "sessionTarget": "isolated",
  "delivery": {"mode": "announce"}
}'
```

## Notes

- All dates use DD/MM/YYYY format for SEDAR+ API calls
- Session IDs expire after ~30 minutes of inactivity
- Rate limit: 1.5 seconds between requests (configurable)
- PDFs are downloaded to `pdfs/` with safe filenames
- Failed downloads are retried on next run
- State tracking prevents duplicate fetches