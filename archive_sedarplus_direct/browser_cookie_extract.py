"""
Extract cookies from a live browser session to bypass SEDAR+ bot detection.

Run this manually in a separate terminal while you have SEDAR+ open in your browser.
It will connect to the browser via CDP and extract the current cookies.

Usage:
1. Open Chrome with: chrome --remote-debugging-port=9222
2. Navigate to https://www.sedarplus.ca/csa-party/service/create.html?targetAppCode=csa-party&service=searchDocuments&_locale=en
3. Let it fully load (wait for the form to appear)
4. Run this script: python browser_cookie_extract.py
5. Copy the output cookies to use with sedar_batch.py
"""

import json
import websocket
import ssl

def get_browser_cookies():
    """Connect to Chrome DevTools and extract SEDAR+ cookies."""
    try:
        # Connect to Chrome DevTools
        import requests
        resp = requests.get("http://localhost:9222/json")
        tabs = resp.json()
        
        # Find SEDAR+ tab
        sedar_tab = None
        for tab in tabs:
            if "sedarplus.ca" in tab.get("url", ""):
                sedar_tab = tab
                break
        
        if not sedar_tab:
            print("No SEDAR+ tab found. Make sure you have it open in Chrome.")
            return
        
        ws_url = sedar_tab["webSocketDebuggerUrl"]
        
        # Create WebSocket connection
        ws = websocket.create_connection(
            ws_url, 
            sslopt={"cert_reqs": ssl.CERT_NONE}
        )
        
        # Get all cookies
        msg = json.dumps({
            "id": 1, 
            "method": "Network.getAllCookies", 
            "params": {}
        })
        ws.send(msg)
        resp = json.loads(ws.recv())
        
        ws.close()
        
        # Filter for SEDAR+ domain cookies
        sedar_cookies = {}
        for cookie in resp.get("result", {}).get("cookies", []):
            if "sedarplus.ca" in cookie.get("domain", ""):
                sedar_cookies[cookie["name"]] = cookie["value"]
        
        print("Extracted cookies for SEDAR+:")
        print(json.dumps(sedar_cookies, indent=2))
        
        # Save to file
        with open("browser_cookies.json", "w") as f:
            json.dump(sedar_cookies, f, indent=2)
        print("\nSaved to browser_cookies.json")
        
        return sedar_cookies
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure Chrome is running with --remote-debugging-port=9222")
        print("And that you have a SEDAR+ tab open.")

if __name__ == "__main__":
    get_browser_cookies()