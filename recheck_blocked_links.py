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
import time
from datetime import datetime, timezone
from typing import Optional, Tuple

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


# -------------------------
# Helpers
# -------------------------
def iso_now():
    return datetime.now(timezone.utc).isoformat()


def classify(code: Optional[int]) -> str:
    if code is None:
        return "Broken"
    if 200 <= code < 400:
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
    try:
        r = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True)
        code = r.status_code
        r.close()
        return code, None
    except requests.RequestException as e:
        return None, type(e).__name__


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
        result = classify(code)

        print("→ HTTP:", code, "| New Result:", result)

        update_payload = {
            "properties": {
                "Result": {"select": {"name": result}},
                "HTTP Code": {"number": code} if code else {"number": None},
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
