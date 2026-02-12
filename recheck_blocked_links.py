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

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

# Domains that often return anti-bot codes (403/999) even if the page is alive.
# You can override the list with env BLOCKED_AS_ACTIVE_DOMAINS (comma-separated).
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

# HTTP codes that, when combined with a whitelisted domain, should be treated as Active
WHITELIST_ACTIVE_CODES: Set[int] = {
    int(x)
    for x in os.environ.get("ACTIVE_WHEN_BLOCKED_CODES", "403,999").split(",")
    if x.strip().isdigit()
}


# -------------------------
# Helpers
# -------------------------
def iso_now():
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
    """Return Active/Broken/Blocked.

    - 2xx/3xx => Active
    - 403/999 on a whitelisted domain => Active (they're usually anti-bot)
    - 404/410 => Broken
    - 401/403/429/999 => Blocked (non‑whitelisted)
    - everything else => Broken
    """

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
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    # ----- Step 0: DNS + TCP handshake (non-intrusivo, niente HTTP ancora)
    try:
        parsed = urlparse(url)
        host = parsed.hostname
        scheme = (parsed.scheme or "https").lower()
        port = parsed.port or (443 if scheme == "https" else 80)
        if not host:
            return None, "no_host"

        # DNS
        socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)

        # TCP connect
        with socket.create_connection((host, port), timeout=6):
            pass
    except socket.gaierror:
        return None, "dns_error"
    except (socket.timeout, ConnectionRefusedError, OSError):
        return None, "tcp_connect_error"
    except Exception as e:
        return None, type(e).__name__

    # ----- Step 1: HEAD (leggerissimo)
    try:
        r = SESSION.head(url, timeout=TIMEOUT, allow_redirects=True, headers=headers)
        code = r.status_code
        r.close()
        # Alcuni siti bloccano HEAD (403/405); in quel caso proviamo GET
        if code not in (403, 405):
            return code, None
    except requests.RequestException as e_head:
        head_err = type(e_head).__name__
    else:
        head_err = None

    # ----- Step 2: GET con stream (solo header + chiusura rapida)
    try:
        r = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True, stream=True, headers=headers)
        code = r.status_code
        r.close()
        return code, None
    except requests.RequestException as e_get:
        return None, head_err or type(e_get).__name__


# -------------------------
# Notion API helpers
# -------------------------
def notion_post(url, payload):
    r = requests.post(url, headers=HEADERS, json=payload, timeout=TIMEOUT)
    if not r.ok:
        print("Notion POST error:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()


def notion_patch(url, payload):
    r = requests.patch(url, headers=HEADERS, json=payload, timeout=TIMEOUT)
    if not r.ok:
        print("Notion PATCH error:", r.status_code, r.text)
    r.raise_for_status()


def query_blocked():
    print("Querying Blocked links from Notion...")

    url = f"https://api.notion.com/v1/databases/{DB_B_ID}/query"

    payload = {
        "filter": {
            "property": "Result",
            "select": {"equals": "Blocked"},
        }
    }

    results = []

    while True:
        data = notion_post(url, payload)
        batch = data.get("results", [])
        print("Batch size:", len(batch))
        results.extend(batch)

        if not data.get("has_more"):
            break

        payload["start_cursor"] = data["next_cursor"]

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

        print("→ HTTP:", code, "| New Result:", result)

        update_payload = {
            "properties": {
                "Result": {"select": {"name": result}},
                "HTTP Code": {"number": code} if code is not None else {"number": None},
                "Error": {
                    "rich_text": [
                        {"text": {"content": err or ""}}
                    ]
                },
                "Last Seen": {
                    "date": {"start": iso_now()}
                },
            }
        }

        notion_patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            update_payload
        )

        print("Updated row:", page_id)
        time.sleep(0.5)

    print("=== RECHECK DONE ===")


if __name__ == "__main__":
    main()
