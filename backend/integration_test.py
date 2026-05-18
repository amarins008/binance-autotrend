import json
import os
import sys
import urllib.request
import urllib.error

BASE = os.getenv("API_BASE", "http://127.0.0.1:8000")


def req(method, path, body=None, timeout=60):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    r = urllib.request.Request(f"{BASE}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        txt = e.read().decode("utf-8") if e.fp else ""
        try:
            parsed = json.loads(txt) if txt else {}
        except Exception:
            parsed = {"raw": txt}
        return e.code, parsed


def main():
    failures = []

    s, b = req("GET", "/health")
    if s != 200 or not b.get("ok"):
        failures.append(f"/health expected 200 ok=true, got {s} {b}")

    # trade WAIT should return 200 in mock/live
    s, b = req("POST", "/trade", {
        "symbol": "BTCUSDT",
        "side": "WAIT",
        "quantity": 0.001,
        "leverage": 5,
        "marginType": "ISOLATED"
    })
    if s != 200:
        failures.append(f"/trade WAIT expected 200, got {s} {b}")

    s_i, b_i = req("POST", "/intel/analyze", {
        "symbol": "BTCUSDT"
    }, timeout=180)
    if s_i != 200:
        failures.append(f"/intel/analyze expected 200, got {s_i} {b_i}")
    else:
        if b_i.get("signal") not in ("LONG", "SHORT", "WAIT"):
            failures.append(f"/intel/analyze invalid signal: {b_i}")

    if failures:
        print("FAIL")
        for f in failures:
            print("-", f)
        sys.exit(1)

    print("PASS")


if __name__ == "__main__":
    main()
