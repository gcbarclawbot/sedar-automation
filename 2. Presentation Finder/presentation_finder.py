"""
presentation_finder.py - Corporate Presentation Scanner
======================================================
Scans company websites to find latest investor presentations/corporate decks.
Three-tier approach: sitemap → platform detection → path probing.

Usage:
  python presentation_finder.py              # Sample 10 onboarded companies
  python presentation_finder.py --sample 20  # Custom sample size
  python presentation_finder.py --run-all    # All onboarded companies

Presentations are downloaded to:
  ...\3. Company Onboarding\Results\{SYMBOL}\pdfs\Presentations\
"""

import sys
import csv
import json
import time
import logging
import requests
import argparse
import re
import xml.etree.ElementTree as ET
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

# Import R2 upload from onboarding script
sys.path.insert(0, str(Path(__file__).parent.parent / "3. Company Onboarding"))
try:
    from mm_onboarding import upload_to_r2, R2_BUCKET, R2_PUBLIC_BASE
    HAS_R2 = True
except Exception as e:
    HAS_R2 = False
    logging.getLogger(__name__).warning(f"R2 not available: {e}")

# Configuration
TIMEOUT = 15
MAX_WORKERS = 10

# Rotate UA strings to reduce bot detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]
USER_AGENT = USER_AGENTS[0]

# Common IR URL patterns to probe
IR_PATHS = [
    "/investors/presentations",
    "/investor-relations/presentations", 
    "/ir/presentations",
    "/investors/corporate-presentation",
    "/investors/events-and-presentations",
    "/media/presentations",
    "/presentations",
    "/investors/investor-resources",
    "/corporate/presentations",
    "/about/presentations",
]

# Keywords for presentation identification (URL/filename)
PRESENTATION_KEYWORDS = [
    "presentation", "corporate", "deck", "investor", "overview",
    "company-presentation", "investor-deck", "corporate-deck",
    "investor-overview", "company-overview"
]

# Negative keywords - these are NOT presentations (must be explicit enough to avoid false positives)
NEGATIVE_KEYWORDS = [
    "notice-of-meeting", "notice_of_meeting",
    "management-information-circular", "management_information_circular", "-mic", "_mic",
    "proxy-circular", "proxy_circular",
    "financial-statement", "financial_statement",
    "press-release", "press_release", "news-release", "news_release",
    "technical-report", "technical_report",
    "ni43-101", "ni_43",
    "mineral-resource-estimate", "mineral_resource_estimate",
    "resource-estimate", "resource_estimate",
    "terms-and-conditions", "terms_and_conditions", "purchase-order", "purchase_order",
    "sustainability-report", "sustainability_report",
    "annual-report", "annual_report",
    "financial-results", "financial_results",
    "human-rights", "human_rights",
    "form-of-proxy", "form_of_proxy", "proxy",
    "mda", "md-a", "management-discussion",
    "news-release", "news_release", "nr-20",
    "circular", "information-circular",
    "rights-policy", "policy",
    "conference", "webinar",
]

# Minimum presentation size in KB - real decks are rarely smaller
MIN_PRESENTATION_KB = 500

# Date patterns in filenames/URLs
DATE_PATTERNS = [
    r"20\d{2}[-_]\d{2}",      # 2026-03, 2026_03
    r"20\d{2}",               # 2026
    r"Q[1-4][-_]?20\d{2}",    # Q1-2026, Q1_2026
    r"\d{2}[-_]\d{2}[-_]20\d{2}", # 03-15-2026
]

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ONBOARDING_RESULTS_DIR = Path(__file__).parent.parent / "3. Company Onboarding" / "Results"
UNIVERSE_CSV = Path(__file__).parent.parent / "1. Canadian Master Sync" / "canadian_universe.csv"

def load_companies(sample_size: Optional[int] = None, run_all: bool = False) -> List[Dict]:
    """Load onboarded companies that have a website in the universe CSV."""
    # Get onboarded symbols (have state.json in Results folder)
    onboarded_symbols = set()
    if ONBOARDING_RESULTS_DIR.exists():
        for d in ONBOARDING_RESULTS_DIR.iterdir():
            if d.is_dir() and (d / 'state.json').exists():
                onboarded_symbols.add(d.name.upper())
    
    if not onboarded_symbols:
        raise FileNotFoundError(f"No onboarded companies found in {ONBOARDING_RESULTS_DIR}")
    
    log.info(f"Found {len(onboarded_symbols)} onboarded companies")

    # Load websites from universe CSV
    if not UNIVERSE_CSV.exists():
        raise FileNotFoundError(f"Universe CSV not found: {UNIVERSE_CSV}")
    
    companies = []
    with open(UNIVERSE_CSV, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = row['symbol'].upper()
            if sym not in onboarded_symbols:
                continue
            website = (row.get('website') or '').strip()
            if not website or not website.startswith('http'):
                continue
            companies.append({
                'symbol': sym,
                'name': row['name'],
                'exchange': row['exchange'],
                'website': website,
                'market_cap': row.get('market_cap', ''),
            })
    
    log.info(f"Loaded {len(companies)} onboarded companies with websites")

    if run_all:
        return companies

    if sample_size is None:
        sample_size = 10

    # Sort by market cap descending, sample from mid-tier to avoid Cloudflare-protected mega-caps
    try:
        companies.sort(key=lambda x: float(x['market_cap']) if x['market_cap'] else 0, reverse=True)
    except:
        pass

    mid_tier = companies[20:] if len(companies) > 20 else companies
    import random
    random.seed(42)
    sample = random.sample(mid_tier, min(sample_size, len(mid_tier)))
    sample.sort(key=lambda x: float(x['market_cap']) if x['market_cap'] else 0, reverse=True)
    return sample

class PresentationFinder:
    def __init__(self):
        import random
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        
    def scan_company(self, company: Dict) -> Dict:
        """Scan a single company for presentations."""
        sym = company['symbol']
        result = {
            'symbol': sym,
            'company_name': company['name'],
            'exchange': company['exchange'],
            'website': company['website'],
            'presentation_url': '',
            'pdf_url': '',
            'local_path': '',
            'r2_url': '',
            'found_via': '',
            'score': 0,
            'file_size_kb': 0,
            'last_checked': datetime.now().isoformat(),
            'error': '',
        }
        
        try:
            log.info(f"Scanning {sym} - {company['name']}")
            
            # Tier 1: Sitemap scan
            tier_result = self._scan_sitemap(company['website'])
            if tier_result:
                result.update(tier_result)
                result['found_via'] = 'sitemap'
            
            # Tier 2: Platform detection
            if not result['pdf_url']:
                tier_result = self._detect_platform(company['website'])
                if tier_result:
                    result.update(tier_result)
                    result['found_via'] = 'platform'
                
            # Tier 3: Path probing
            if not result['pdf_url']:
                tier_result = self._probe_paths(company['website'])
                if tier_result:
                    result.update(tier_result)
                    result['found_via'] = 'path_probe'

            if not result['pdf_url']:
                result['found_via'] = 'not_found'
                return result

            # Download the PDF
            local_path, size_kb = self._download_pdf(result['pdf_url'], sym)
            if local_path:
                result['local_path'] = str(local_path)
                result['file_size_kb'] = size_kb
                # Upload to R2
                if HAS_R2:
                    r2_url = upload_to_r2(local_path, sym)
                    result['r2_url'] = r2_url
                log.info(f"  {sym}: {local_path.name} ({size_kb}KB) [{result['found_via']}]")
            else:
                log.warning(f"  {sym}: PDF download failed")
            
        except Exception as e:
            log.warning(f"Error scanning {sym}: {e}")
            result['error'] = str(e)
            
        return result

    def _download_pdf(self, pdf_url: str, symbol: str) -> tuple:
        """Download PDF to Results/{SYMBOL}/pdfs/Presentations/. Returns (path, size_kb)."""
        try:
            # Clean URL (strip duplicate ?v= params sometimes appended by path probing)
            clean_url = re.sub(r'(\?v=[^?]+)(?:\?v=[^?]+)+', r'\1', pdf_url)
            
            resp = self.session.get(clean_url, timeout=30, stream=False)
            if resp.status_code != 200:
                log.debug(f"PDF download {resp.status_code}: {clean_url}")
                return None, 0
            if resp.content[:4] != b'%PDF':
                log.debug(f"Not a PDF: {clean_url}")
                return None, 0
            size_kb = len(resp.content) // 1024
            if size_kb < MIN_PRESENTATION_KB:
                log.debug(f"PDF too small ({size_kb}KB < {MIN_PRESENTATION_KB}KB): {clean_url}")
                return None, 0

            # Build destination path
            dest_dir = ONBOARDING_RESULTS_DIR / symbol / "pdfs" / "Presentations"
            dest_dir.mkdir(parents=True, exist_ok=True)

            # Derive filename from URL, fallback to generic name with today's date
            url_path = urlparse(clean_url).path
            filename = Path(url_path).name
            if not filename or not filename.lower().endswith('.pdf'):
                filename = f"{symbol}_corporate_presentation_{date.today().strftime('%Y-%m-%d')}.pdf"
            # Strip query strings from filename
            filename = filename.split('?')[0]
            # Prefix with today's date if no date in filename
            if not re.search(r'20\d{2}', filename):
                filename = f"{date.today().strftime('%Y-%m-%d')}_{filename}"

            dest_path = dest_dir / filename
            dest_path.write_bytes(resp.content)
            return dest_path, size_kb

        except Exception as e:
            log.debug(f"PDF download error: {e}")
            return None, 0
        
    def _fetch_sitemap_urls(self, sitemap_url: str, depth: int = 0) -> List[str]:
        """Recursively fetch all URLs from a sitemap or sitemap index."""
        if depth > 2:
            return []
        try:
            resp = self.session.get(sitemap_url, timeout=TIMEOUT)
            if resp.status_code != 200:
                return []
            # Strip CDATA and XML namespace issues
            content = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', resp.text, flags=re.DOTALL)
            root = ET.fromstring(content.encode('utf-8'))
            urls = []
            # Sitemap index - recurse into child sitemaps
            for sitemap in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap'):
                loc = sitemap.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                if loc is not None and loc.text:
                    # Only recurse into presentation-relevant sub-sitemaps
                    if any(k in loc.text.lower() for k in ['presentation', 'investor', 'event', 'media']):
                        urls.extend(self._fetch_sitemap_urls(loc.text.strip(), depth + 1))
            # Regular sitemap URLs
            for url in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
                loc = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                if loc is not None and loc.text:
                    urls.append(loc.text.strip())
            return urls
        except Exception as e:
            log.debug(f"Sitemap parse error {sitemap_url}: {e}")
            return []

    def _scan_sitemap(self, website: str) -> Optional[Dict]:
        """Tier 1: Scan sitemap for presentation URLs."""
        try:
            # Discover sitemap URL(s)
            sitemap_urls = []
            for path in ['/sitemap.xml', '/sitemap_index.xml']:
                sitemap_urls.append(urljoin(website, path))
            # Also check robots.txt for sitemap declaration
            try:
                resp = self.session.get(urljoin(website, '/robots.txt'), timeout=TIMEOUT)
                if resp.status_code == 200:
                    for line in resp.text.split('\n'):
                        if line.lower().startswith('sitemap:'):
                            sitemap_urls.insert(0, line.split(':', 1)[1].strip())
            except:
                pass

            all_urls = []
            for sitemap_url in sitemap_urls:
                urls = self._fetch_sitemap_urls(sitemap_url)
                if urls:
                    all_urls.extend(urls)
                    break  # Stop at first working sitemap

            # Score URLs for presentation likelihood
            candidates = []
            for url in all_urls:
                score = self._score_url(url)
                if score > 20:
                    candidates.append((url, score))

            if candidates:
                best_url, best_score = max(candidates, key=lambda x: x[1])
                if best_url.lower().endswith('.pdf'):
                    return {
                        'presentation_url': best_url,
                        'pdf_url': best_url,
                        'score': best_score,
                        'file_size_kb': self._get_pdf_size(best_url)
                    }
                else:
                    pdf_url = self._find_pdf_on_page(best_url)
                    if pdf_url:
                        return {
                            'presentation_url': best_url,
                            'pdf_url': pdf_url,
                            'score': best_score,
                            'file_size_kb': self._get_pdf_size(pdf_url)
                        }

        except Exception as e:
            log.debug(f"Sitemap scan failed: {e}")

        return None
        
    def _detect_platform(self, website: str) -> Optional[Dict]:
        """Tier 2: Detect IR platform and use platform-specific shortcuts."""
        try:
            resp = self.session.get(website, timeout=TIMEOUT)
            if resp.status_code != 200:
                return None
                
            content = resp.text.lower()
            
            # Q4 Inc detection
            if 'q4inc.com' in content or 'q4web.com' in content or '/_next/data/' in content:
                return self._scan_q4_platform(website)
                
            # Other platforms can be added here
            # if 'cision.com' in content:
            #     return self._scan_cision_platform(website)
            
        except Exception as e:
            log.debug(f"Platform detection failed: {e}")
            
        return None
        
    def _scan_q4_platform(self, website: str) -> Optional[Dict]:
        """Scan Q4 Inc platform for presentations."""
        q4_paths = [
            '/presentations',
            '/events-and-presentations', 
            '/investor-relations/presentations',
            '/investors/presentations',
            '/api/v1/presentations',  # Direct API if available
        ]
        
        for path in q4_paths:
            try:
                resp = self.session.get(urljoin(website, path), timeout=TIMEOUT)
                if resp.status_code == 200:
                    pdf_url = self._find_pdf_on_page(resp.url, resp.text)
                    if pdf_url:
                        return {
                            'presentation_url': resp.url,
                            'pdf_url': pdf_url,
                            'score': 80,  # High score for platform detection
                            'file_size_kb': self._get_pdf_size(pdf_url)
                        }
            except:
                continue
                
        return None
        
    def _probe_paths(self, website: str) -> Optional[Dict]:
        """Tier 3: Probe common IR paths + homepage as fallback."""
        best_result = None
        best_score = 0

        # Include homepage as last-resort fallback
        paths_to_try = IR_PATHS + ['/']

        for path in paths_to_try:
            try:
                url = urljoin(website, path)
                # Use GET directly (HEAD often returns 200 even for redirect targets)
                resp = self.session.get(url, timeout=TIMEOUT, allow_redirects=True)

                if resp.status_code == 200:
                    pdf_url = self._find_pdf_on_page(resp.url, resp.text)
                    if pdf_url:
                        bonus = 30 if path != '/' else 5  # Lower bonus for homepage finds
                        score = self._score_url(pdf_url) + bonus
                        if score > best_score:
                            best_score = score
                            best_result = {
                                'presentation_url': resp.url,
                                'pdf_url': pdf_url,
                                'score': score,
                                'file_size_kb': self._get_pdf_size(pdf_url)
                            }
                            # If we found something on a dedicated IR path, stop early
                            if path != '/' and score > 40:
                                return best_result

            except:
                continue

        return best_result
        
    def _find_pdf_on_page(self, page_url: str, content: str = None) -> Optional[str]:
        """Extract PDF links from a page, scoring by both URL and link text."""
        if content is None:
            try:
                resp = self.session.get(page_url, timeout=TIMEOUT)
                if resp.status_code != 200:
                    return None
                content = resp.text
            except:
                return None

        # Find all PDF links WITH their surrounding anchor text
        # Pattern: capture href + the text content of the <a> tag
        anchor_pattern = re.compile(
            r'<a[^>]+href=["\']([^"\']*\.pdf[^"\']*)["\'][^>]*>([^<]*)</a>',
            re.IGNORECASE | re.DOTALL
        )
        # Also capture href-only (no text)
        href_pattern = re.compile(r'href=["\']([^"\']*\.pdf[^"\']*)["\']', re.IGNORECASE)

        candidates = []

        # Score anchors with text (link text adds context for hash-style URLs)
        for match in anchor_pattern.finditer(content):
            link_href = match.group(1)
            link_text = match.group(2).strip()
            full_url = urljoin(page_url, link_href)
            score = self._score_url(full_url)
            # Also score by link text
            text_lower = link_text.lower()
            for keyword in PRESENTATION_KEYWORDS:
                if keyword in text_lower:
                    score += 30  # Strong signal - human labeled it
            # Penalise by link text too
            for neg in NEGATIVE_KEYWORDS:
                if neg.replace('-', ' ') in text_lower or neg.replace('_', ' ') in text_lower:
                    score = -1
                    break
            if score > 0:
                candidates.append((full_url, score))

        # Fallback: href-only links not caught by anchor pattern
        for link in href_pattern.findall(content):
            full_url = urljoin(page_url, link)
            if not any(full_url == c[0] for c in candidates):
                score = self._score_url(full_url)
                if score > 10:
                    candidates.append((full_url, score))

        if candidates:
            return max(candidates, key=lambda x: x[1])[0]

        return None
        
    def _score_url(self, url: str) -> int:
        """Score URL/filename for presentation relevance."""
        url_lower = url.lower()
        score = 0
        
        # Hard reject negative keywords
        for neg in NEGATIVE_KEYWORDS:
            if neg in url_lower:
                return -1
        
        # Keyword scoring
        for keyword in PRESENTATION_KEYWORDS:
            if keyword in url_lower:
                score += 25
                
        # Date scoring (recent = higher score)
        current_year = date.today().year
        for pattern in DATE_PATTERNS:
            matches = re.findall(pattern, url_lower)
            for match in matches:
                try:
                    year = int(re.search(r'20\d{2}', match).group())
                    if year >= current_year - 1:  # Last 2 years
                        score += 20
                    elif year >= current_year - 3:  # Last 4 years
                        score += 10
                except:
                    pass
                    
        # Path scoring
        if '/investor' in url_lower or '/ir/' in url_lower:
            score += 15
            
        if '/corporate' in url_lower:
            score += 10

        # Any PDF in an IR path gets a base score even without keywords
        if score == 0 and url_lower.endswith('.pdf'):
            score = 5  # Low base - better than nothing
            
        return score
        
    def _get_pdf_size(self, pdf_url: str) -> int:
        """Get PDF file size in KB."""
        try:
            resp = self.session.head(pdf_url, timeout=TIMEOUT)
            if resp.status_code == 200:
                size = resp.headers.get('content-length')
                if size:
                    return int(size) // 1024
        except:
            pass
        return 0

def main():
    parser = argparse.ArgumentParser(description="Scan company websites for presentations")
    parser.add_argument('--sample', type=int, default=10, help='Number of companies to sample')
    parser.add_argument('--run-all', action='store_true', help='Scan all companies in universe')
    parser.add_argument('--output', default='presentation_results.csv', help='Output CSV file')
    
    args = parser.parse_args()
    
    # Load companies
    companies = load_companies(sample_size=args.sample, run_all=args.run_all)
    log.info(f"Scanning {len(companies)} companies")
    
    # Create output directory
    output_dir = Path(__file__).parent / "Results"
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / args.output
    
    finder = PresentationFinder()
    results = []
    
    # Process companies with workers
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_company = {executor.submit(finder.scan_company, company): company 
                           for company in companies}
        
        completed = 0
        for future in as_completed(future_to_company):
            result = future.result()
            results.append(result)
            completed += 1
            
            # Progress logging
            if completed % 5 == 0 or completed == len(companies):
                found = sum(1 for r in results if r['pdf_url'])
                log.info(f"Progress: {completed}/{len(companies)} - {found} presentations found")
    
    # Save results
    fieldnames = ['symbol', 'company_name', 'exchange', 'website', 'presentation_url',
                  'pdf_url', 'local_path', 'r2_url', 'found_via', 'score', 'file_size_kb',
                  'last_checked', 'error']
                  
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    # Summary
    found_count = sum(1 for r in results if r['pdf_url'])
    log.info(f"Scan complete: {found_count}/{len(companies)} presentations found")
    log.info(f"Results saved to: {output_file}")
    
    # Show sample results
    for result in results[:3]:
        if result['pdf_url']:
            log.info(f"  {result['symbol']}: {result['pdf_url']} ({result['found_via']})")

if __name__ == "__main__":
    main()