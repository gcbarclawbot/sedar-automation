"""Extract cookies directly from the live browser tab via CDP WebSocket."""
import requests, json, ssl
import websocket

tabs = requests.get("http://127.0.0.1:18800/json").json()
sedar_tab = next(
    (t for t in tabs if "sedarplus.ca" in t.get("url","") and "viewInstance" in t.get("url","")),
    None
)
if not sedar_tab:
    print("No viewInstance SEDAR+ tab found")
    exit(1)
print("Tab:", sedar_tab["url"][:80])

ws = websocket.create_connection(
    sedar_tab["webSocketDebuggerUrl"],
    sslopt={"cert_reqs": ssl.CERT_NONE}
)
ws.send(json.dumps({"id":1,"method":"Network.getAllCookies","params":{}}))
resp = json.loads(ws.recv())

cookies = {}
for c in resp["result"]["cookies"]:
    if "sedarplus.ca" in c.get("domain",""):
        cookies[c["name"]] = c["value"]

print("\nAll SEDAR+ cookies:")
for k,v in cookies.items():
    print(f"  {k}: {v[:60]}")

# Also get the vikey and session ID from the page JS
ws.send(json.dumps({"id":2,"method":"Runtime.evaluate","params":{"expression":"[window.location.href, document.documentElement.innerHTML.match(/viewInstanceKey:'([^']+)'/)?.[1]]"}}))
resp2 = json.loads(ws.recv())
ws.close()

result = {
    "cookies": cookies,
    "sessionId": "",
    "vikey": "",
}

import re
for c in resp["result"]["cookies"]:
    if c["name"] == "x-catalyst-session-global":
        result["catalystToken"] = c["value"]
        break

print("\ncatalystToken:", result.get("catalystToken","")[:40])

# Save full session
with open("browser_session.json", "w") as f:
    json.dump(result, f, indent=2)
print("\nSaved to browser_session.json")
