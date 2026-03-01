#!/usr/bin/env python3

"""
Recheck ONLY Blocked links from Notion DB B
Updates:
- Result
- HTTP Code
- Error
- Last Seen
"""

import os
import socket
import time
from datetime import datetime, timezone
from typing import Optional, Tuple, Set
from urllib.parse import urlparse

import requests

# -------------------------
# ENV
# -------------------------
print("=== RECHECK SCRIPT STARTED ===")

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DB_B_ID = os.environ.get("NOTION_DB_B_ID")

if not NOTION_TOKEN:
    raise ValueError("Missing NOTION_TOKEN")
if not DB_B_ID:
    raise ValueError("Missing NOTION_DB_B_ID")

print("Using DB_B_ID:", DB_B_ID[:8], "...")

TIMEOUT = 40
NOTION_VERSION = "2022-06-28"
USER_AGENT = "Mozilla/5.0 (Marble LinkHealthHub Blocked Recheck)"

# FIX: use a persistent Session for connection pooling across all HTTP calls
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
})

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# Domains that often return anti-bot codes (403/999) even if the page is alive.
DEFAULT_WHITELIST = [
    "linkedin.com",
    "substack.com",
    "economist.com",
    "annualreviews.org",
    "iea.org",
    "ncbi.nlm.nih.gov",
    "axios.com",
    "techfundingnews.com",
    "lesechos.fr",
]

WHITELIST: Set[str] = {
    d.strip().lower()
    for d in os.environ.get("BLOCKED_AS_ACTIVE_DOMAINS", ",".join(DEFAULT_WHITELIST)).split(",")
    if d.strip()
}

WHITELIST_ACTIVE_CODES: Set[int] = {
    int(x)
    for x in os.environ.get("ACTIVE_WHEN_BLOCKED_CODES", "403,999").split(",")
    if x.strip().isdigit()
}


# -------------------------
# Helpers
# -------------------------
def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_whitelisted(url: str) -> bool:
    try:
        netloc = urlparse(url).netloc.lower()
    except Exception:
        return False
    for d in WHITELIST:
        if netloc == d or netloc.endswith("." + d):
            return True
    return False


def classify(url: str, code: Optional[int]) -> str:
    if code is None:
        return "Broken"
    if 200 <= code < 400:
        return "Active"
    if code in WHITELIST_ACTIVE_CODES and is_whitelisted(url):
        return "Active"
    if code in (404, 410):
        return "Broken"
    if code in (401, 403, 429, 999):
        return "Blocked"
    if 400 <= code < 500:
        return "Broken"
    if code >= 500:
        return "Broken"
    return "Broken"


def check_url(url: str) -> Tuple[Optional[int], Optional[str]]:
    """
    Preflight (DNS + TCP) -> HEAD -> fallback GET (stream).
    Returns (status_code | None, error_label | None)
    """
    # ----- Step 0: DNS + TCP handshake
    try:
        parsed = urlparse(url)
        host = parsed.hostname
        scheme = (parsed.scheme or "https").lower()
        port = parsed.port or (443 if scheme == "https" else 80)
        if not host:
            return None, "no_host"
        socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        with socket.create_connection((host, port), timeout=6):
            pass
    except socket.gaierror:
        return None, "dns_error"
    except (socket.timeout, ConnectionRefusedError, OSError):
        return None, "tcp_connect_error"
    except Exception as e:
        return None, type(e).__name__

    # ----- Step 1: HEAD
    head_err: Optional[str] = None
    try:
        r = SESSION.head(url, timeout=TIMEOUT, allow_redirects=True)
        code = r.status_code
        r.close()
        # Some sites block HEAD (403/405); fall through to GET in that case
        if code not in (403, 405):
            return code, None
    except requests.RequestException as e:
        head_err = type(e).__name__

    # ----- Step 2: GET with stream (headers only, fast close)
    try:
        r = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True, stream=True)
        code = r.status_code
        r.close()
        return code, None
    except requests.RequestException as e:
        return None, head_err or type(e).__name__


# -------------------------
# Notion API helpers (with retry on 429 / 5xx)
# -------------------------
def notion_request(method: str, url: str, payload: Optional[dict] = None) -> dict:
    """
    FIX: retry on Notion 429 and 5xx with exponential backoff.
    """
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            if method == "POST":
                r = SESSION.post(url, headers=NOTION_HEADERS, json=payload or {}, timeout=TIMEOUT)
            elif method == "PATCH":
                r = SESSION.patch(url, headers=NOTION_HEADERS, json=payload or {}, timeout=TIMEOUT)
            else:
                raise ValueError(f"Unknown method: {method}")
        except requests.RequestException as e:
            print(f"Notion {method} network error (attempt {attempt}/{max_attempts}): {e}")
            if attempt < max_attempts:
                time.sleep(2 ** attempt)
                continue
            raise

        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", 2 ** attempt))
            print(f"Notion 429. Waiting {retry_after}s (attempt {attempt}/{max_attempts})")
            time.sleep(retry_after)
            continue

        if r.status_code >= 500:
            print(f"Notion {r.status_code} error (attempt {attempt}/{max_attempts}): {r.text[:200]}")
            if attempt < max_attempts:
                time.sleep(2 ** attempt)
                continue

        if not r.ok:
            print("Notion error:", r.status_code, r.text)
        r.raise_for_status()
        return r.json()

    raise RuntimeError(f"Notion {method} {url} failed after {max_attempts} attempts")


def query_blocked() -> list:
    print("Querying Blocked links from Notion...")

    url = f"https://api.notion.com/v1/databases/{DB_B_ID}/query"
    results = []
    # FIX: build a fresh payload dict each iteration to avoid mutation issues
    cursor = None

    while True:
        payload: dict = {
            "filter": {"property": "Result", "select": {"equals": "Blocked"}},
        }
        if cursor:
            payload["start_cursor"] = cursor

        data = notion_request("POST", url, payload)
        batch = data.get("results", [])
        print("Batch size:", len(batch))
        results.extend(batch)

        if not data.get("has_more"):
            break
        cursor = data["next_cursor"]

    return results


# -------------------------
# Main
# -------------------------
def main():
    blocked_rows = query_blocked()
    print(f"Found {len(blocked_rows)} blocked links")

    if not blocked_rows:
        print("No Blocked links found. Exiting.")
        return

    for row in blocked_rows:
        page_id = row["id"]
        props = row["properties"]

        link_url = props.get("URL", {}).get("url")

        if not link_url:
            print("Row missing URL, skipping:", page_id)
            continue

        print("Rechecking:", link_url)

        code, err = check_url(link_url)
        result = classify(link_url, code)

        print(f"→ HTTP: {code} | New Result: {result}")

        update_payload = {
            "properties": {
                "Result": {"select": {"name": result}},
                "HTTP Code": {"number": code if code is not None else None},
                "Error": {
                    "rich_text": [{"text": {"content": err or ""}}]
                },
                "Last Seen": {
                    "date": {"start": iso_now()}
                },
            }
        }

        notion_request("PATCH", f"https://api.notion.com/v1/pages/{page_id}", update_payload)
        print("Updated row:", page_id)
        time.sleep(0.5)

    print("=== RECHECK DONE ===")


if __name__ == "__main__":
    main()
