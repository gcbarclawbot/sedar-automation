"""Debug: check what the search POST actually returns."""
import warnings, json
warnings.filterwarnings("ignore")
from datetime import date
from pathlib import Path
from sedar_batch import SedarSession, SEDAR_BASE

sess_file = Path(__file__).parent / "browser_session.json"
sess = json.loads(sess_file.read_text())

s = SedarSession(delay=1.0, browser_cookies=sess["cookies"])
s.session_id = sess["sessionId"]
s.vikey = sess["vikey"]
s.update_url = sess["updateUrl"]
s.profile_filter_node = "nodeW667"
s.profile_ac_node = "nodeW668"

# Try the reset
html = s.reset_search_form()
print("=== AFTER RESET ===")
print("New session ID:", s.session_id[:20])
print("New update URL:", s.update_url[:60])
print("HTML snippet:", html[:500] if html else "NONE")
print()

# Now do the search POST directly
from datetime import date
from_str = "27/03/2026"
to_str = "27/03/2026"
data = {
    "nodeW667-filterSQL": "contains",
    "DocumentContent": "",
    "nodeW675-searchOp": "ContainsIgnoreCase",
    "nodeW676-AnyAllFilter": "all",
    "FilingIdentifier": "",
    "FilingCategory": "CONTINUOUS_DISCLOSURE",
    "FilingType": "NEWS_RELEASES",
    "FilingSubType": "",
    "DocumentType": "",
    "SubmissionDate": from_str,
    "SubmissionDate2": to_str,
    "nodeW714PageSize": "2",  # 50 per page
    "nodeW715-DownloadAllDocumentsYn": "N",
    "_CBNAME_": "search",
    "_CBVALUE_": "search",
    "_VIKEY_": s.vikey,
}
resp = s._post(data)
print("=== SEARCH RESPONSE ===")
print("Status:", resp.status_code if hasattr(resp, 'status_code') else 'N/A')
print("URL:", resp.url if hasattr(resp, 'url') else 'N/A')
print("Content length:", len(resp.text))
print("First 1000 chars:")
print(resp.text[:1000])
print()
# Check for bot detection redirect
if "perfdrive" in resp.text or "shieldsquare" in resp.text:
    print("!!! BOT DETECTION in response !!!")
elif "nodeW" in resp.text:
    print("==> SEDAR+ app HTML detected - looks good")
elif "Displaying" in resp.text:
    print("==> Results found!")
