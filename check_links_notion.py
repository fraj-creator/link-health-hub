import os
import time
from datetime import datetime, timezone
import requests

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")  # optional

NOTION_VERSION = "2022-06-28"
TIMEOUT = 12
USER_AGENT = "Mozilla/5.0 (LinkHealthHub)"

# ---- Notion helpers ----
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

def notion_query_database(database_id: str, start_cursor: str | None = None) -> dict:
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    payload = {}
    if start_cursor:
        payload["start_cursor"] = start_cursor
    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def notion_update_page(page_id: str, properties: dict) -> None:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {"properties": properties}
    r = requests.patch(url, headers=NOTION_HEADERS, json=payload, timeout=TIMEOUT)
    r.raise_for_status()

def get_prop(props: dict, name: str) -> dict:
    return props.get(name, {})

def get_title(props: dict) -> str:
    t = get_prop(props, "Title")
    title_arr = t.get("title", [])
    if title_arr and "plain_text" in title_arr[0]:
        return "".join([x.get("plain_text", "") for x in title_arr]).strip()
    return "(untitled)"

def get_url(props: dict) -> str | None:
    u = get_prop(props, "Primary URL")
    return u.get("url")

def get_status(props: dict) -> str | None:
    s = get_prop(props, "Status")
    sel = s.get("select")
    return sel.get("name") if sel else None

# ---- Link checking ----
def check_url(url: str) -> tuple[int | None, str | None]:
    if not url or not isinstance(url, str):
        return None, "empty_url"
    url = url.strip()
    try:
        r = requests.head(url, allow_redirects=True, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
        code = r.status_code

        # fallback if HEAD is blocked/unreliable
        if code in (403, 405) or code >= 500:
            r = requests.get(url, allow_redirects=True, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT}, stream=True)
            code = r.status_code

        return code, None
    except requests.RequestException as e:
        return None, type(e).__name__

def status_from_code(code: int | None) -> str:
    if code is None:
        return "Broken"
    if 200 <= code < 400:
        return "Active"
    if code in (404, 410):
        return "Broken"
    # 403 often anti-bot (link may still exist)
    if code == 403:
        return "Active"
    return "Broken"

# ---- Slack notify (optional) ----
def slack_notify(text: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=TIMEOUT)
    except requests.RequestException:
        pass

def main():
    start_cursor = None
    checked = 0
    updated = 0
    newly_broken = []

    now_iso = datetime.now(timezone.utc).isoformat()

    while True:
        data = notion_query_database(NOTION_DATABASE_ID, start_cursor=start_cursor)

        for page in data.get("results", []):
            page_id = page["id"]
            props = page.get("properties", {})

            url = get_url(props)
            if not url:
                continue

            current_status = get_status(props)
            if current_status == "Replaced":
                continue

            title = get_title(props)

            code, err = check_url(url)
            new_status = status_from_code(code)

            checked += 1

            props_to_update = {}

            # Update Status only if changed
            if new_status != current_status:
                props_to_update["Status"] = {"select": {"name": new_status}}
                updated += 1

                # Track only "newly broken" (Active -> Broken or None -> Broken)
                if new_status == "Broken" and current_status != "Broken":
                    newly_broken.append((title, url, code))

            # Optional columns (update only if they exist in DB)
            if "Last Checked" in props:
                props_to_update["Last Checked"] = {"date": {"start": now_iso}}

            if "HTTP Code" in props:
                # HTTP Code should be a "Number" property in Notion
                props_to_update["HTTP Code"] = {"number": float(code) if code is not None else None}

            if "Check Error" in props:
                # Check Error could be a "Rich text" property
                props_to_update["Check Error"] = {"rich_text": [{"text": {"content": err or ""}}]}

            if props_to_update:
                notion_update_page(page_id, props_to_update)

            # small sleep to be gentle with rate limits
            time.sleep(0.35)

        if data.get("has_more"):
            start_cursor = data.get("next_cursor")
        else:
            break

    print(f"Done. Checked={checked}, Status updated={updated}")

    if newly_broken:
        lines = [f"⚠️ Link Health Hub: {len(newly_broken)} newly broken link(s):"]
        for t, u, c in newly_broken[:20]:
            code_str = f" ({int(c)})" if c is not None else ""
            lines.append(f"• {t}{code_str} — {u}")
        if len(newly_broken) > 20:
            lines.append(f"…and {len(newly_broken) - 20} more.")
        slack_notify("\n".join(lines))

if __name__ == "__main__":
    main()
