"""
Utilities for SEDAR+ scraping that can be imported by other scripts.
"""

import re
import json
from pathlib import Path
from datetime import datetime

def load_browser_cookies(cookie_file: str = "browser_cookies.json") -> dict:
    """Load cookies from a JSON file created by browser_cookie_extract.py"""
    try:
        with open(cookie_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def clean_filename(text: str, max_len: int = 100) -> str:
    """Clean text for use as a filename."""
    # Remove/replace invalid filename chars
    clean = re.sub(r'[<>:"/\\|?*]', '', text)
    clean = re.sub(r'[^\w\s.-]', '', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean[:max_len]

def parse_sedar_date(date_str: str) -> datetime | None:
    """Parse various SEDAR+ date formats."""
    if not date_str:
        return None
    
    # Remove extra whitespace
    clean = re.sub(r'\s+', ' ', date_str.strip())
    
    # Try various formats
    formats = [
        "%d %b %Y %H:%M %Z",      # "27 Mar 2026 11:13 EDT"
        "%d %b %Y %H:%M",         # "27 Mar 2026 11:13"
        "%B %d %Y at %H:%M:%S",   # "March 27 2026 at 11:13:54"
        "%d/%m/%Y",               # "27/03/2026"
        "%Y-%m-%d",               # "2026-03-27"
    ]
    
    # Handle the long format: "March 27 2026 at 11:13:54 Eastern Daylight Time"
    m = re.search(r'(\w+ \d+ \d{4}) at (\d+:\d+:\d+)', clean)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)}", "%B %d %Y %H:%M:%S")
        except ValueError:
            pass
    
    # Try standard formats
    for fmt in formats:
        try:
            # Remove timezone names that strptime can't parse
            test_str = re.sub(r'\s+(Eastern|Atlantic|Pacific|Central)\s+(Standard|Daylight)\s+Time', '', clean)
            return datetime.strptime(test_str, fmt)
        except ValueError:
            continue
    
    return None

def extract_party_number(company_text: str) -> str:
    """Extract the 9-digit party number from company name like 'Company Name (000123456)'"""
    m = re.search(r'\((\d{9})\)', company_text)
    return m.group(1) if m else ""

def clean_company_name(company_text: str) -> str:
    """Clean company name - remove party number and handle bilingual names."""
    # Remove party number suffix
    clean = re.sub(r'\s*\(\d{9}\)\s*$', '', company_text).strip()
    
    # Handle bilingual names - take English part (before " / ")
    if ' / ' in clean:
        clean = clean.split(' / ')[0].strip()
    
    return clean

def make_permanent_url(doc_hash: str) -> str:
    """Convert document hash to permanent SEDAR+ URL."""
    return f"https://www.sedarplus.ca/csa-party/records/document.html?id={doc_hash}"

def validate_doc_hash(doc_hash: str) -> bool:
    """Validate that a string looks like a SEDAR+ document hash."""
    return bool(re.match(r'^[a-f0-9]{64}$', doc_hash))

# Common SEDAR+ filing types and categories
FILING_TYPES = {
    'NEWS_RELEASES': 'News releases',
    'MATERIAL_CHANGE_REPORT': 'Material change report', 
    'ANNUAL_FINANCIAL_STATEMENTS': 'Annual financial statements',
    'INTERIM_FINANCIAL_STATEMENTS': 'Interim financial statements/report',
    'ANNUAL_INFORMATION_FORM': 'Annual information form',
    'TECHNICAL_REPORT': 'Technical report(s) (NI 43-101)',
}

FILING_CATEGORIES = {
    'CONTINUOUS_DISCLOSURE': 'Continuous disclosure',
    'SECURITIES_OFFERINGS': 'Securities offerings',
    'APW': 'Applications, pre-filings and waivers',
    'EXEMPT_MARKET': 'Exempt market offerings',
    'THIRD_PARTY_FILINGS': 'Third party filings and securities acquisitions',
}

# Canadian provinces/territories (for jurisdiction filtering)
CANADIAN_JURISDICTIONS = [
    'British Columbia', 'Alberta', 'Saskatchewan', 'Manitoba', 'Ontario', 
    'Quebec', 'New Brunswick', 'Nova Scotia', 'Prince Edward Island', 
    'Newfoundland and Labrador', 'Northwest Territories', 'Nunavut', 'Yukon'
]

# Common mining/resources keywords for filtering
MINING_KEYWORDS = [
    'mining', 'exploration', 'resources', 'gold', 'silver', 'copper', 'zinc',
    'iron', 'nickel', 'platinum', 'palladium', 'uranium', 'lithium', 'cobalt',
    'mineral', 'metals', 'ore', 'drilling', 'prospect', 'deposit', 'claim'
]