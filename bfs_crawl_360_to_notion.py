#!/usr/bin/env python3
"""
bfs_crawl_360_to_notion.py

BFS crawler (NO sitemap). It:
1) Crawls internal pages starting from SITE_BASE_URL (BFS, max MAX_PAGES)
2) Extracts ONLY clickable links (<a href>) from each HTML page
3) Checks external links (Active/Broken/Blocked) with cache + retry-on-broken
4) Upserts:
   - DB A (Link Health Hub 360): one row per crawled page
   - DB B (Link Occurrences): one row per (source_page_url | link_url), with breadcrumbs + snippet
5) Sends Slack alert ONLY for newly broken links

Required env vars:
  NOTION_TOKEN
  NOTION_DB_A_ID
  NOTION_DB_B_ID
  SITE_BASE_URL

Optional env vars:
  SLACK_WEBHOOK_URL
  SLACK_TEST_WEBHOOK_URL
  SLACK_MODE=prod|test
  MAX_PAGES=120
  CHECK_EXTERNAL=true|false
  CHECK_INTERNAL=true|false
  CRAWL_SLEEP=0.3
  NOTION_SLEEP=0.25
  SKIP_EXTENSIONS=.jpg,.png,...   (override list)
  SKIP_DOMAINS=linkedin.com,...   (mark as Blocked without checking)

IMPORTANT:
- In Notion you must have Select options that match these strings:
  DB A: Status -> "Active", "Broken"
  DB B: Result -> "Active", "Broken", "Blocked"
        Link Type -> "internal", "external"
"""

import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup


# -------------------------
# Config
# -------------------------
TIMEOUT = 12
USER_AGENT = "Mozilla/5.0 (Marble LinkHealthHub BFS Crawler)"
NOTION_VERSION = "2022-06-28"

NOTION_TOKEN = os.environ["NOTION_TOKEN"].strip()
DB_A_ID = os.environ["NOTION_DB_A_ID"].strip()
DB_B_ID = os.environ["NOTION_DB_B_ID"].strip()
SITE_BASE_URL = os.environ["SITE_BASE_URL"].strip()

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip() or None
SLACK_TEST_WEBHOOK_URL = os.environ.get("SLACK_TEST_WEBHOOK_URL", "").strip() or None
SLACK_MODE = os.environ.get("SLACK_MODE", "prod").lower()

MAX_PAGES = int(os.environ.get("MAX_PAGES", "120"))
CHECK_EXTERNAL = os.environ.get("CHECK_EXTERNAL", "true").lower() in ("1", "true", "yes", "y")
CHECK_INTERNAL = os.environ.get("CHECK_INTERNAL", "true").lower() in ("1", "true", "yes", "y")

CRAWL_SLEEP = float(os.environ.get("CRAWL_SLEEP", "0.3"))
NOTION_SLEEP = float(os.environ.get("NOTION_SLEEP", "0.25"))

DEFAULT_SKIP_EXT = [
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
    ".css", ".js", ".mjs", ".map",
    ".woff", ".woff2", ".ttf", ".eot",
    ".mp4", ".mov", ".webm", ".mp3", ".wav",
    ".pdf", ".zip", ".rar", ".7z", ".dmg", ".exe",
    ".json", ".xml", ".rss",
]
SKIP_EXTENSIONS = os.environ.get("SKIP_EXTENSIONS")
if SKIP_EXTENSIONS:
    SKIP_EXT = [x.strip().lower() for x in SKIP_EXTENSIONS.split(",") if x.strip()]
else:
    SKIP_EXT = DEFAULT_SKIP_EXT

SKIP_DOMAINS_ENV = os.environ.get("SKIP_DOMAINS", "")
SKIP_DOMAINS = {d.strip().lower() for d in SKIP_DOMAINS_ENV.split(",") if d.strip()}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}


# -------------------------
# Notion property names (CHANGE HERE if your column names differ)
# -------------------------
# DB A (Link Health Hub 360)
DBA_TITLE = "Title"               # Title type
DBA_PRIMARY_URL = "Primary URL"   # URL type
DBA_STATUS = "Status"             # Select: Active/Broken
DBA_LAST_CRAWLED = "Last Crawled" # Date

# DB B (Link Occurrences)
DBB_NAME = "Name"                      # Title
DBB_SOURCE_CONTENT = "Source Content"  # Relation -> DB A
DBB_URL = "URL"                        # URL
DBB_LINK_TYPE = "Link Type"            # Select: internal/external
DBB_ANCHOR = "Anchor Text"             # Rich text / Text
DBB_CONTEXT = "Context Snippet"        # Rich text / Text
DBB_BREADCRUMB = "Breadcrumb Trail"    # Rich text / Text
DBB_RESULT = "Result"                  # Select: Active/Broken/Blocked
DBB_HTTP = "HTTP Code"                 # Number
DBB_ERROR = "Error"                    # Rich text / Text
DBB_FIRST_SEEN = "First Seen"          # Date
DBB_LAST_SEEN = "Last Seen"            # Date
DBB_FINDING_KEY = "Finding Key"        # Rich text / Text


# -------------------------
# Slack
# -------------------------
def slack_notify(text: str) -> None:
    webhook = SLACK_TEST_WEBHOOK_URL if SLACK_MODE == "test" else SLACK_WEBHOOK_URL
    if not webhook:
        return
    try:
        requests.post(webhook, json={"text": text, "mrkdwn": True}, timeout=TIMEOUT)
    except requests.RequestException:
        pass


# -------------------------
# URL helpers
# -------------------------
def strip_trailing_slash(url: str) -> str:
    if url.endswith("/") and len(url) > 8:
        return url[:-1]
    return url

def normalize_url(base: str, href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("#"):
        return None
    if href.startswith(("mailto:", "tel:", "javascript:")):
        return None
    abs_url = urljoin(base, href)
    abs_url, _frag = urldefrag(abs_url)
    return strip_trailing_slash(abs_url)

def same_domain(url: str, domain: str) -> bool:
    try:
        return urlparse(url).netloc.lower() == domain.lower()
    except Exception:
        return False

def should_ignore_url(url: str) -> bool:
    # ignore Next.js build assets
    if "/_next/" in url:
        return True
    return False

def has_skipped_extension(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in SKIP_EXT)

def is_probably_html_page(url: str) -> bool:
    # used only for enqueueing internal pages
    if should_ignore_url(url):
        return False
    if has_skipped_extension(url):
        return False
    return True

def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


# -------------------------
# Fetch + parse
# -------------------------
def fetch_html(url: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    try:
        r = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True)
        ct = r.headers.get("Content-Type", "")
        status = r.status_code
        if status >= 400:
            return None, None, status
        if "text/html" not in ct:
            return None, None, status

        html = r.text
        title = None
        try:
            soup = BeautifulSoup(html, "lxml")
            if soup.title and soup.title.string:
                title = soup.title.string.strip()
        except Exception:
            title = None

        return html, title, status
    except requests.RequestException:
        return None, None, None

def extract_a_links(page_url: str, html: str) -> List[Dict[str, str]]:
    """
    ONLY <a href>. Also returns anchor_text + context_snippet.
    """
    soup = BeautifulSoup(html, "lxml")
    out: List[Dict[str, str]] = []

    for a in soup.find_all("a", href=True):
        u = normalize_url(page_url, a.get("href"))
        if not u:
            continue
        if should_ignore_url(u):
            continue

        anchor_text = (a.get_text(" ", strip=True) or "").strip()

        parent_text = ""
        try:
            if a.parent:
                parent_text = (a.parent.get_text(" ", strip=True) or "").strip()
        except Exception:
            parent_text = ""

        snippet = (parent_text or anchor_text).replace("\n", " ").strip()
        if len(snippet) > 180:
            snippet = snippet[:177] + "..."

        out.append({"url": u, "anchor_text": anchor_text, "context_snippet": snippet})

    return out


# -------------------------
# Link check (external)
# -------------------------
def check_url(url: str) -> Tuple[Optional[int], Optional[str]]:
    try:
        r = SESSION.head(url, allow_redirects=True, timeout=TIMEOUT)
        code = r.status_code
        # fallback if HEAD blocked/unreliable
        if code in (403, 405) or code >= 500:
            r = SESSION.get(url, allow_redirects=True, timeout=TIMEOUT, stream=True)
            code = r.status_code
        return code, None
    except requests.RequestException as e:
        return None, type(e).__name__

def classify(code: Optional[int]) -> str:
    if code is None:
        return "Broken"
    if 200 <= code < 400:
        return "Active"
    if code in (404, 410):
        return "Broken"
    # anti-bot / login / rate-limit (LinkedIn often 999)
    if code in (401, 403, 429, 999):
        return "Blocked"
    if 400 <= code < 500:
        return "Broken"
    if code >= 500:
        return "Broken"
    return "Broken"

def double_check_broken(url: str, first_code: Optional[int], first_err: Optional[str]) -> Tuple[Optional[int], Optional[str], str]:
    """
    Reduce false positives: if first result looks Broken, retry once.
    """
    first_res = classify(first_code)
    if first_res != "Broken":
        return first_code, first_err, first_res

    time.sleep(0.8)
    code2, err2 = check_url(url)
    res2 = classify(code2)
    # if second check is not Broken, trust it
    if res2 != "Broken":
        return code2, err2, res2
    return first_code, first_err, first_res


# -------------------------
# Notion API helpers
# -------------------------
def notion_query_database(database_id: str, payload: dict) -> dict:
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=TIMEOUT)
    if not r.ok:
        print("Notion query error:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

def notion_create_page(database_id: str, properties: dict) -> dict:
    url = "https://api.notion.com/v1/pages"
    payload = {"parent": {"database_id": database_id}, "properties": properties}
    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=TIMEOUT)
    if not r.ok:
        print("Notion create error:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

def notion_update_page(page_id: str, properties: dict) -> None:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {"properties": properties}
    r = requests.patch(url, headers=NOTION_HEADERS, json=payload, timeout=TIMEOUT)
    if not r.ok:
        print("Notion update error:", r.status_code, r.text)
    r.raise_for_status()

def find_in_db_by_url(db_id: str, url_prop: str, url_value: str) -> Optional[dict]:
    payload = {"filter": {"property": url_prop, "url": {"equals": url_value}}}
    data = notion_query_database(db_id, payload)
    res = data.get("results", [])
    return res[0] if res else None

def find_in_db_by_rich_text(db_id: str, prop: str, equals: str) -> Optional[dict]:
    payload = {"filter": {"property": prop, "rich_text": {"equals": equals}}}
    data = notion_query_database(db_id, payload)
    res = data.get("results", [])
    return res[0] if res else None

def get_select(page: Optional[dict], prop: str) -> Optional[str]:
    if not page:
        return None
    p = (page.get("properties", {}) or {}).get(prop, {})
    sel = p.get("select")
    return sel.get("name") if sel else None

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def rt(text: str) -> list:
    return [{"text": {"content": text}}]


# -------------------------
# Upserts
# -------------------------
def upsert_db_a_page(page_url: str, title: str, page_http: Optional[int]) -> str:
    existing = find_in_db_by_url(DB_A_ID, DBA_PRIMARY_URL, page_url)

    props = {
        DBA_TITLE: {"title": [{"text": {"content": title or page_url}}]},
        DBA_PRIMARY_URL: {"url": page_url},
        DBA_LAST_CRAWLED: {"date": {"start": iso_now()}},
        DBA_STATUS: {"select": {"name": "Broken" if (page_http is None or page_http >= 400) else "Active"}},
    }

    if existing:
        notion_update_page(existing["id"], props)
        return existing["id"]

    created = notion_create_page(DB_A_ID, props)
    return created["id"]

def make_name(anchor: str, link_url: str) -> str:
    a = (anchor or "(no anchor)").strip()
    if len(a) > 60:
        a = a[:57] + "..."
    dom = urlparse(link_url).netloc or link_url
    return f"{a} | {dom}"

def upsert_db_b_occurrence(
    source_page_id: str,
    source_page_url: str,
    link_url: str,
    link_type: str,
    anchor_text: str,
    context_snippet: str,
    breadcrumb_trail: str,
    http_code: Optional[int],
    result: str,
    error: str,
) -> Tuple[bool, Optional[str]]:
    """
    Returns: (newly_broken, previous_result)
    """
    finding_key = f"{source_page_url} | {link_url}"
    existing = find_in_db_by_rich_text(DB_B_ID, DBB_FINDING_KEY, finding_key)
    prev_result = get_select(existing, DBB_RESULT) if existing else None
    newly_broken = (result == "Broken" and prev_result != "Broken")

    props = {
        DBB_NAME: {"title": [{"text": {"content": make_name(anchor_text, link_url)}}]},
        DBB_SOURCE_CONTENT: {"relation": [{"id": source_page_id}]},
        DBB_URL: {"url": link_url},
        DBB_LINK_TYPE: {"select": {"name": link_type}},
        DBB_ANCHOR: {"rich_text": rt(anchor_text or "")},
        DBB_CONTEXT: {"rich_text": rt(context_snippet or "")},
        DBB_BREADCRUMB: {"rich_text": rt(breadcrumb_trail or "")},
        DBB_RESULT: {"select": {"name": result}},
        DBB_ERROR: {"rich_text": rt(error or "")},
        DBB_LAST_SEEN: {"date": {"start": iso_now()}},
        DBB_FINDING_KEY: {"rich_text": rt(finding_key)},
    }

    # Only include HTTP Code if present (avoid Notion API complaining about null)
    if http_code is not None:
        props[DBB_HTTP] = {"number": float(http_code)}

    if existing:
        notion_update_page(existing["id"], props)
    else:
        props[DBB_FIRST_SEEN] = {"date": {"start": iso_now()}}
        notion_create_page(DB_B_ID, props)

    return newly_broken, prev_result


# -------------------------
# Breadcrumb builder
# -------------------------
def build_trail(parent: Dict[str, Optional[str]], page: str) -> str:
    chain = []
    cur = page
    seen = set()
    while cur and cur not in seen:
        seen.add(cur)
        chain.append(cur)
        cur = parent.get(cur)
    chain.reverse()
    return " -> ".join(chain)


# -------------------------
# Main
# -------------------------
def main():
    base = strip_trailing_slash(SITE_BASE_URL)
    domain = urlparse(base).netloc

    queue = deque([base])
    seen_pages = set()

    parent: Dict[str, Optional[str]] = {base: None}
    page_title: Dict[str, str] = {}

    # page_url -> DB A page_id (so we can write DB B relations)
    url_to_a_id: Dict[str, str] = {}

    # Internal link edges remembered until the target is fetched:
    # target_url -> list of {source_url, anchor, snippet, breadcrumb}
    incoming_edges: Dict[str, List[Dict[str, str]]] = {}

    # cache external link checks: url -> (code, err, result)
    link_cache: Dict[str, Tuple[Optional[int], Optional[str], str]] = {}

    newly_broken_rows: List[Dict[str, str]] = []

    pages_crawled = 0
    print(f"Starting BFS from {base} (MAX_PAGES={MAX_PAGES})", flush=True)

    while queue and pages_crawled < MAX_PAGES:
        page = strip_trailing_slash(queue.popleft())

        if page in seen_pages:
            continue
        if not same_domain(page, domain):
            continue
        if not is_probably_html_page(page):
            continue

        seen_pages.add(page)
        pages_crawled += 1

        html, title, page_http = fetch_html(page)
        time.sleep(CRAWL_SLEEP)

        if title:
            page_title[page] = title

        # Upsert the page in DB A even if fetch failed (so you can see broken pages)
        page_id = upsert_db_a_page(page, title or page, page_http)
        url_to_a_id[page] = page_id
        time.sleep(NOTION_SLEEP)

        # If we're tracking internal link results: when we finally fetch a target page,
        # mark all internal links that pointed to it (Active/Broken)
        if CHECK_INTERNAL:
            edges = incoming_edges.pop(page, [])
            if edges:
                target_result = "Broken" if (page_http is None or page_http >= 400) else "Active"
                for e in edges:
                    src_url = e["source_url"]
                    src_id = url_to_a_id.get(src_url)
                    if not src_id:
                        continue

                    # Internal occurrence: source page -> (internal link to target page)
                    upsert_db_b_occurrence(
                        source_page_id=src_id,
                        source_page_url=src_url,
                        link_url=page,
                        link_type="internal",
                        anchor_text=e.get("anchor", ""),
                        context_snippet=e.get("snippet", ""),
                        breadcrumb_trail=e.get("breadcrumb", ""),
                        http_code=page_http,
                        result=target_result,
                        error="",
                    )
                    time.sleep(NOTION_SLEEP)

        if not html:
            continue

        links = extract_a_links(page, html)
        source_breadcrumb = build_trail(parent, page)

        # 1) enqueue internal pages + remember "who pointed to who"
        for item in links:
            link = strip_trailing_slash(item["url"])
            if should_ignore_url(link):
                continue
            if not same_domain(link, domain):
                continue
            if not is_probably_html_page(link):
                continue

            if link not in parent:
                parent[link] = page

            if link not in seen_pages:
                queue.append(link)

            if CHECK_INTERNAL:
                incoming_edges.setdefault(link, []).append({
                    "source_url": page,
                    "anchor": item.get("anchor_text", ""),
                    "snippet": item.get("context_snippet", ""),
                    "breadcrumb": source_breadcrumb,
                })

        # 2) external checks + DB B external occurrences
        if not CHECK_EXTERNAL:
            continue

        for item in links:
            link = strip_trailing_slash(item["url"])
            if should_ignore_url(link):
                continue

            # external only here
            if same_domain(link, domain):
                continue

            # ignore obvious assets even if linked via <a>
            if has_skipped_extension(link):
                continue

            d = domain_of(link)
            if d in SKIP_DOMAINS:
                code, err, result = None, "skipped_domain", "Blocked"
            else:
                if link in link_cache:
                    code, err, result = link_cache[link]
                else:
                    code1, err1 = check_url(link)
                    code, err, result = double_check_broken(link, code1, err1)
                    link_cache[link] = (code, err, result)
                    time.sleep(CRAWL_SLEEP)

            newly_broken, _prev = upsert_db_b_occurrence(
                source_page_id=page_id,
                source_page_url=page,
                link_url=link,
                link_type="external",
                anchor_text=item.get("anchor_text", ""),
                context_snippet=item.get("context_snippet", ""),
                breadcrumb_trail=source_breadcrumb,
                http_code=code,
                result=result,
                error=err or "",
            )
            time.sleep(NOTION_SLEEP)

            if newly_broken:
                newly_broken_rows.append({
                    "source_page": page,
                    "source_title": page_title.get(page, "") or page,
                    "breadcrumb_trail": source_breadcrumb,
                    "link": link,
                    "code": "" if code is None else str(int(code)),
                    "anchor": (item.get("anchor_text") or "")[:60],
                })

    # Slack summary (only newly broken)
    if newly_broken_rows:
        n = len(newly_broken_rows)
        noun = "link" if n == 1 else "links"
        lines = [f"⚠️ Link Health Hub 360 (BFS): {n} newly broken {noun} found ({domain})"]
        for r in newly_broken_rows[:15]:
            src = r["source_title"]
            code = r["code"] or "ERR"
            source_click = f"<{r['source_page']}|Source>"
            link_click = f"<{r['link']}|Link>"
            lines.append(f"• *Breadcrumb*: `{r['breadcrumb_trail']}`")
            if r["anchor"]:
                lines.append(f"  {src} ({code}) — {source_click} — {link_click} — `{r['anchor']}`")
            else:
                lines.append(f"  {src} ({code}) — {source_click} — {link_click}")
        if n > 15:
            lines.append(f"…and {n-15} more.")
        slack_notify("\n".join(lines))

    print(f"Done. Pages crawled={pages_crawled}, external checked={len(link_cache)}", flush=True)


if __name__ == "__main__":
    main()
