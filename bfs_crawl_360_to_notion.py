#!/usr/bin/env python3
"""
Playwright BFS crawler (first 120 pages) -> Notion DB A + DB B

ENV required:
  NOTION_TOKEN
  NOTION_DB_A_ID
  NOTION_DB_B_ID
  SITE_BASE_URL

ENV optional:
  MAX_PAGES=120
  CHECK_EXTERNAL=true|false
  CHECK_INTERNAL=true|false
  CRAWL_SLEEP=0.25
  NOTION_MIN_INTERVAL=0.5
  SKIP_DOMAINS=linkedin.com,...
  EXCLUDE_DOM_AREAS=Footer,Nav,Header   (default Footer,Nav)
  SLACK_WEBHOOK_URL=...
"""

import os
import re
import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urldefrag, urlunparse

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


# -------------------------
# Config
# -------------------------
TIMEOUT = 15
USER_AGENT = "Mozilla/5.0 (Marble LinkHealthHub Playwright BFS)"
NOTION_VERSION = "2022-06-28"

NOTION_TOKEN = os.environ["NOTION_TOKEN"].strip()
DB_A_ID = os.environ["NOTION_DB_A_ID"].strip()
DB_B_ID = os.environ["NOTION_DB_B_ID"].strip()
SITE_BASE_URL = os.environ["SITE_BASE_URL"].strip()

MAX_PAGES = int(os.environ.get("MAX_PAGES", "120"))

CHECK_EXTERNAL = os.environ.get("CHECK_EXTERNAL", "true").lower() in ("1", "true", "yes", "y")
CHECK_INTERNAL = os.environ.get("CHECK_INTERNAL", "true").lower() in ("1", "true", "yes", "y")

CRAWL_SLEEP = float(os.environ.get("CRAWL_SLEEP", "0.25"))
NOTION_MIN_INTERVAL = float(os.environ.get("NOTION_MIN_INTERVAL", "0.5"))

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip() or None

SKIP_DOMAINS = {d.strip().lower() for d in os.environ.get("SKIP_DOMAINS", "linkedin.com").split(",") if d.strip()}

# default: exclude Footer + Nav (riduce spam tipo Numbered nel footer)
EXCLUDE_DOM_AREAS = os.environ.get("EXCLUDE_DOM_AREAS", "Footer,Nav").strip()
EXCLUDE_DOM_AREAS_SET = {x.strip() for x in EXCLUDE_DOM_AREAS.split(",") if x.strip()}

DEFAULT_SKIP_EXT = [
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
    ".css", ".js", ".mjs", ".map",
    ".woff", ".woff2", ".ttf", ".eot",
    ".mp4", ".mov", ".webm", ".mp3", ".wav",
    ".pdf", ".zip", ".rar", ".7z",
]
SKIP_EXT = DEFAULT_SKIP_EXT

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# -------------------------
# Notion property names (MUST match exactly)
# -------------------------
# DB A
DBA_TITLE = "Title"
DBA_PRIMARY_URL = "Primary URL"
DBA_STATUS = "Status"          # Select: Active / Broken / Need Review
DBA_LAST_CRAWLED = "Last Crawled"  # Date (optional)
DBA_PAGES = "Pages"            # optional Select
DBA_CONTENT_TYPE = "Content Type"  # optional Select

# DB B
DBB_NAME = "Name"
DBB_SOURCE_CONTENT = "Source Content"
DBB_URL = "URL"
DBB_LINK_TYPE = "Link Type"    # Select: internal / external
DBB_RESULT = "Result"          # Select: Active / Broken / Blocked
DBB_HTTP = "HTTP Code"
DBB_ERROR = "Error"
DBB_FINDING_KEY = "Finding Key"
DBB_FIRST_SEEN = "First Seen"
DBB_LAST_SEEN = "Last Seen"
DBB_ANCHOR = "Anchor Text"
DBB_CONTEXT = "Context Snippet"
DBB_BREADCRUMB = "Breadcrumb Trail"

# Playwright-ready fields you created
DBB_UI_GROUP = "UI Group"
DBB_UI_ITEM = "UI Item"
DBB_CLICK_PATH = "Click Path"
DBB_DEEP_LINK = "Deep Link"       # URL property
DBB_RENDER_MODE = "Render Mode"   # Select: Static / Playwright
DBB_LOCATOR_CSS = "Locator CSS"   # Text
DBB_DOM_AREA = "DOM Area"         # Select: Main/Header/Footer/Nav/Accordion/Unknown


# -------------------------
# Small helpers
# -------------------------
class RateLimiter:
    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self._last = 0.0

    def wait(self):
        now = time.monotonic()
        dt = now - self._last
        if dt < self.min_interval:
            time.sleep(self.min_interval - dt)
        self._last = time.monotonic()

notion_rl = RateLimiter(NOTION_MIN_INTERVAL)

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def rt(text: str) -> list:
    return [{"text": {"content": text}}]

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
    abs_url, _ = urldefrag(abs_url)
    return strip_trailing_slash(abs_url)

def has_skipped_extension(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in SKIP_EXT)

def should_ignore_url(url: str) -> bool:
    return "/_next/" in url

def domain_of(url: str) -> str:
    return urlparse(url).netloc.lower()

def same_domain(url: str, domain: str) -> bool:
    try:
        return urlparse(url).netloc.lower() == domain.lower()
    except Exception:
        return False

def drop_query(url: str) -> str:
    p = urlparse(url)
    return urlunparse(p._replace(query=""))

def clean_title(title: str) -> str:
    if not title:
        return ""
    t = title.strip()
    # remove repeated Marble branding
    t = re.sub(r"^(Marble\s*[-–]\s*)+", "", t).strip()
    t = re.sub(r"(\s*[-–]\s*Marble)+$", "", t).strip()
    # collapse duplicates like "Marble - Marble"
    t = re.sub(r"(Marble\s*[-–]\s*){2,}", "Marble - ", t).strip()
    return t or title.strip()

def guess_pages_and_type(page_url: str) -> Tuple[Optional[str], Optional[str]]:
    """Heuristic for DB A Pages + Content Type based on URL path."""
    path = urlparse(page_url).path.lower()
    if path == "/" or page_url.rstrip("/") == SITE_BASE_URL.rstrip("/"):
        return "Home", "Website Page"
    if "/community" in path:
        return "Community", "Article"
    if "/companies/" in path:
        return "Companies", "Company"
    if "/companies" in path:
        return "Companies", "Directory"
    if "/opportunities" in path:
        return "Opportunities", "Listing"
    return "Other", "Website Page"


# -------------------------
# Slack
# -------------------------
def slack_notify(text: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text, "mrkdwn": True}, timeout=TIMEOUT)
    except requests.RequestException:
        pass


# -------------------------
# Notion API
# -------------------------
def notion_post(url: str, payload: dict) -> dict:
    notion_rl.wait()
    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=TIMEOUT)
    if not r.ok:
        print("Notion POST error:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

def notion_patch(url: str, payload: dict) -> dict:
    notion_rl.wait()
    r = requests.patch(url, headers=NOTION_HEADERS, json=payload, timeout=TIMEOUT)
    if not r.ok:
        print("Notion PATCH error:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

def notion_query_all(database_id: str) -> List[dict]:
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    results: List[dict] = []
    cursor = None
    while True:
        payload = {}
        if cursor:
            payload["start_cursor"] = cursor
        data = notion_post(url, payload)
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
        time.sleep(0.05)
    return results

def notion_create_page(database_id: str, properties: dict) -> dict:
    url = "https://api.notion.com/v1/pages"
    payload = {"parent": {"database_id": database_id}, "properties": properties}
    return notion_post(url, payload)

def notion_update_page(page_id: str, properties: dict) -> None:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {"properties": properties}
    notion_patch(url, payload)

def prop_url(page: dict, prop: str) -> Optional[str]:
    p = (page.get("properties", {}) or {}).get(prop, {})
    return p.get("url")

def prop_rich_text(page: dict, prop: str) -> str:
    p = (page.get("properties", {}) or {}).get(prop, {})
    t = p.get("rich_text", [])
    if t and isinstance(t, list):
        return "".join([x.get("plain_text", "") for x in t]).strip()
    return ""

def prop_select(page: dict, prop: str) -> Optional[str]:
    p = (page.get("properties", {}) or {}).get(prop, {})
    sel = p.get("select")
    return sel.get("name") if sel else None

def build_db_a_index() -> Dict[str, str]:
    pages = notion_query_all(DB_A_ID)
    idx: Dict[str, str] = {}
    for pg in pages:
        u = prop_url(pg, DBA_PRIMARY_URL)
        if u:
            idx[strip_trailing_slash(u)] = pg["id"]
    return idx

def build_db_b_index() -> Dict[str, Tuple[str, Optional[str]]]:
    pages = notion_query_all(DB_B_ID)
    idx: Dict[str, Tuple[str, Optional[str]]] = {}
    for pg in pages:
        fk = prop_rich_text(pg, DBB_FINDING_KEY)
        if fk:
            idx[fk] = (pg["id"], prop_select(pg, DBB_RESULT))
    return idx


# -------------------------
# HTTP checking (external)
# -------------------------
def check_url(url: str) -> Tuple[Optional[int], Optional[str]]:
    try:
        r = SESSION.head(url, allow_redirects=True, timeout=TIMEOUT)
        code = r.status_code
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
    if code in (401, 403, 429, 999):
        return "Blocked"
    if 400 <= code < 500:
        return "Broken"
    if code >= 500:
        return "Broken"
    return "Broken"

def double_check_broken(url: str, first_code: Optional[int], first_err: Optional[str]) -> Tuple[Optional[int], Optional[str], str]:
    first_res = classify(first_code)
    if first_res != "Broken":
        return first_code, first_err, first_res
    time.sleep(0.8)
    code2, err2 = check_url(url)
    res2 = classify(code2)
    if res2 != "Broken":
        return code2, err2, res2
    return first_code, first_err, first_res


# -------------------------
# Notion upserts
# -------------------------
def upsert_db_a(db_a_index: Dict[str, str], page_url: str, title: str, page_http_ok: bool, broken_count: int) -> str:
    key = strip_trailing_slash(page_url)
    existing_id = db_a_index.get(key)

    # Status logic:
    # - if page itself broken => Broken
    # - else if any broken links in that page => Need Review
    # - else Active
    if not page_http_ok:
        status = "Broken"
    elif broken_count > 0:
        status = "Need Review"
    else:
        status = "Active"

    props = {
        DBA_TITLE: {"title": [{"text": {"content": title or page_url}}]},
        DBA_PRIMARY_URL: {"url": page_url},
        DBA_STATUS: {"select": {"name": status}},
        DBA_LAST_CRAWLED: {"date": {"start": iso_now()}},
    }

    # Optional enrichment (if your DB A has these properties)
    pages_val, type_val = guess_pages_and_type(page_url)
    if pages_val:
        props[DBA_PAGES] = {"select": {"name": pages_val}}
    if type_val:
        props[DBA_CONTENT_TYPE] = {"select": {"name": type_val}}

    if existing_id:
        try:
            notion_update_page(existing_id, props)
        except requests.HTTPError:
            # if optional props cause issues, retry without them
            props.pop(DBA_PAGES, None)
            props.pop(DBA_CONTENT_TYPE, None)
            notion_update_page(existing_id, props)
        return existing_id

    try:
        created = notion_create_page(DB_A_ID, props)
    except requests.HTTPError:
        props.pop(DBA_PAGES, None)
        props.pop(DBA_CONTENT_TYPE, None)
        created = notion_create_page(DB_A_ID, props)

    db_a_index[key] = created["id"]
    return created["id"]

def make_occ_name(anchor: str, link_url: str) -> str:
    dom = urlparse(link_url).netloc or link_url
    a = (anchor or "").strip()
    if not a:
        return dom
    if len(a) > 55:
        a = a[:52] + "..."
    return f"{dom} • {a}"

def upsert_db_b(
    db_b_index: Dict[str, Tuple[str, Optional[str]]],
    source_page_id: str,
    source_page_url: str,
    link_url: str,
    link_type: str,
    result: str,
    http_code: Optional[int],
    error: str,
    anchor_text: str,
    snippet: str,
    breadcrumb: str,
    dom_area: str,
    ui_group: str,
    ui_item: str,
    click_path: str,
    deep_link: str,
    locator_css: str,
) -> bool:
    finding_key = f"{source_page_url} | {link_url}"
    existing = db_b_index.get(finding_key)
    prev_result = existing[1] if existing else None
    newly_broken = (result == "Broken" and prev_result != "Broken")

    # Write only if new or status changed (saves Notion calls)
    should_write = (existing is None) or (prev_result != result)

    if not should_write:
        return newly_broken

    props = {
        DBB_NAME: {"title": [{"text": {"content": make_occ_name(anchor_text, link_url)}}]},
        DBB_SOURCE_CONTENT: {"relation": [{"id": source_page_id}]},
        DBB_URL: {"url": link_url},
        DBB_LINK_TYPE: {"select": {"name": link_type}},
        DBB_RESULT: {"select": {"name": result}},
        DBB_ERROR: {"rich_text": rt(error or "")},
        DBB_FINDING_KEY: {"rich_text": rt(finding_key)},
        DBB_ANCHOR: {"rich_text": rt(anchor_text or "")},
        DBB_CONTEXT: {"rich_text": rt(snippet or "")},
        DBB_BREADCRUMB: {"rich_text": rt(breadcrumb or "")},
        DBB_RENDER_MODE: {"select": {"name": "Playwright"}},
        DBB_LOCATOR_CSS: {"rich_text": rt(locator_css or "")},
        DBB_DOM_AREA: {"select": {"name": dom_area or "Unknown"}},
        DBB_UI_GROUP: {"rich_text": rt(ui_group or "")},
        DBB_UI_ITEM: {"rich_text": rt(ui_item or "")},
        DBB_CLICK_PATH: {"rich_text": rt(click_path or "")},
    }

    # Deep Link is URL property (only if non-empty)
    if deep_link:
        props[DBB_DEEP_LINK] = {"url": deep_link}

    if http_code is not None:
        props[DBB_HTTP] = {"number": float(http_code)}

    now = iso_now()
    if existing:
        props[DBB_LAST_SEEN] = {"date": {"start": now}}
        notion_update_page(existing[0], props)
        db_b_index[finding_key] = (existing[0], result)
    else:
        props[DBB_FIRST_SEEN] = {"date": {"start": now}}
        props[DBB_LAST_SEEN] = {"date": {"start": now}}
        created = notion_create_page(DB_B_ID, props)
        db_b_index[finding_key] = (created["id"], result)

    return newly_broken


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
# Playwright extraction
# -------------------------
JS_EXTRACT_LINKS = """
() => {
  const anchors = Array.from(document.querySelectorAll('a[href]'));
  const out = [];
  for (const a of anchors) {
    const href = a.getAttribute('href') || '';
    const text = (a.innerText || '').trim();
    // snippet from nearest parent
    let snippet = '';
    try {
      const p = a.parentElement;
      snippet = (p ? (p.innerText || '') : text).trim();
      snippet = snippet.replace(/\\s+/g, ' ');
      if (snippet.length > 180) snippet = snippet.slice(0, 177) + '...';
    } catch(e) {}
    // dom area: closest semantic container
    let area = 'Main';
    const foot = a.closest('footer');
    const head = a.closest('header');
    const nav = a.closest('nav');
    if (foot) area = 'Footer';
    else if (nav) area = 'Nav';
    else if (head) area = 'Header';
    // basic css locator
    let loc = '';
    try {
      const h = a.getAttribute('href');
      if (h) loc = `a[href="${h.replace(/"/g, '\\"')}"]`;
    } catch(e) {}
    out.push({href, text, snippet, area, loc});
  }
  return out;
}
"""

def playwright_collect_links(pw_page) -> List[Dict[str, str]]:
    items = pw_page.evaluate(JS_EXTRACT_LINKS)
    out: List[Dict[str, str]] = []
    for it in items:
        out.append({
            "href": it.get("href", ""),
            "anchor_text": it.get("text", ""),
            "snippet": it.get("snippet", ""),
            "dom_area": it.get("area", "Main"),
            "locator_css": it.get("loc", ""),
        })
    return out

def playwright_expand_tabs_and_accordions(pw_page) -> List[Dict[str, str]]:
    """
    Heuristic:
    - Click through tabs (role=tab) if present
    - For each tab panel, click accordion triggers (button[aria-expanded]) to reveal links
    - After opening, capture deep link (page.url()) and associate UI Group/UI Item to links found *after open*
    """
    results: List[Dict[str, str]] = []

    # gather tabs
    tabs = pw_page.locator('[role="tab"]')
    tab_count = 0
    try:
        tab_count = tabs.count()
    except Exception:
        tab_count = 0

    def process_current_view(ui_group: str):
        # accordions: buttons with aria-expanded
        btns = pw_page.locator('button[aria-expanded]')
        try:
            n = btns.count()
        except Exception:
            n = 0

        # limit to avoid infinite UI
        n = min(n, 40)
        for i in range(n):
            try:
                b = btns.nth(i)
                text = (b.inner_text() or "").strip()
                if not text:
                    continue
                # open only if currently closed
                expanded = (b.get_attribute("aria-expanded") or "").lower()
                if expanded == "true":
                    continue
                b.scroll_into_view_if_needed(timeout=2000)
                before_url = pw_page.url
                b.click(timeout=3000)
                pw_page.wait_for_timeout(200)  # small settle

                after_url = pw_page.url
                deep_link = after_url if after_url != before_url else ""

                # now collect links again, but we only keep those in Accordion area
                links_now = playwright_collect_links(pw_page)
                for ln in links_now:
                    if ln.get("dom_area") != "Main":
                        continue
                    # many accordions live in main; we tag as Accordion only when a deep link exists
                    if deep_link:
                        ln2 = dict(ln)
                        ln2["dom_area"] = "Accordion"
                        ln2["ui_group"] = ui_group
                        ln2["ui_item"] = text
                        ln2["deep_link"] = deep_link
                        results.append(ln2)

            except Exception:
                continue

    if tab_count > 0:
        # iterate tabs
        for t in range(min(tab_count, 10)):
            try:
                tab = tabs.nth(t)
                ui_group = (tab.inner_text() or "").strip() or ""
                tab.scroll_into_view_if_needed(timeout=2000)
                tab.click(timeout=3000)
                pw_page.wait_for_timeout(250)
                process_current_view(ui_group)
            except Exception:
                continue
    else:
        process_current_view("")

    return results


# -------------------------
# Main BFS
# -------------------------
def main():
    base = strip_trailing_slash(SITE_BASE_URL)
    domain = urlparse(base).netloc.lower()

    print("Prefetching Notion indices (DB A + DB B)...", flush=True)
    db_a_index = build_db_a_index()
    db_b_index = build_db_b_index()
    print(f"DB A indexed: {len(db_a_index)} rows; DB B indexed: {len(db_b_index)} rows", flush=True)

    queue = deque([base])
    parent: Dict[str, Optional[str]] = {base: None}
    seen = set()
    pages_crawled = 0

    external_cache: Dict[str, Tuple[Optional[int], Optional[str], str]] = {}
    newly_broken_alerts: List[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT, viewport={"width": 1280, "height": 800})
        page = context.new_page()

        print(f"Starting Playwright BFS from {base} (MAX_PAGES={MAX_PAGES})", flush=True)

        while queue and pages_crawled < MAX_PAGES:
            page_url = strip_trailing_slash(queue.popleft())
            page_url = drop_query(page_url)  # reduce duplicates for internal

            if page_url in seen:
                continue
            if not same_domain(page_url, domain):
                continue
            if should_ignore_url(page_url) or has_skipped_extension(page_url):
                continue

            seen.add(page_url)
            pages_crawled += 1
            breadcrumb = build_trail(parent, page_url)

            # navigate
            page_ok = True
            page_title = ""
            try:
                page.goto(page_url, wait_until="networkidle", timeout=25000)
                page.wait_for_timeout(200)
                page_title = clean_title(page.title() or "") or page_url
            except PWTimeoutError:
                page_ok = False
            except Exception:
                page_ok = False

            time.sleep(CRAWL_SLEEP)

            # Extract links from rendered DOM
            found = []
            if page_ok:
                try:
                    found = playwright_collect_links(page)
                except Exception:
                    found = []

            # Expand accordions/tabs to capture deep links (case A)
            accordion_found = []
            if page_ok:
                try:
                    accordion_found = playwright_expand_tabs_and_accordions(page)
                except Exception:
                    accordion_found = []

            # Merge accordion-special (they carry UI fields + deep link)
            # We keep them as additional occurrences (same Finding Key will dedupe)
            merged = found + accordion_found

            # Normalize and classify links; enqueue internal pages
            internal_links = []
            for it in merged:
                href = it.get("href", "")
                absu = normalize_url(page_url, href)
                if not absu:
                    continue
                absu = strip_trailing_slash(absu)

                # ignore assets/next
                if should_ignore_url(absu) or has_skipped_extension(absu):
                    continue

                if same_domain(absu, domain):
                    # internal
                    if absu not in parent:
                        parent[absu] = page_url
                    if absu not in seen:
                        internal_links.append(absu)

            for u in internal_links:
                queue.append(u)

            # Now check/store occurrences and compute broken count for this page
            broken_in_page = 0

            # Create/update DB A after we computed broken count (we will do a two-pass)
            # We'll upsert DB A once at the end, but DB B needs source_page_id. So we do a temp id:
            # If page already exists in DB A, use it; otherwise create minimal first.
            # To keep it simple: create with broken_count=0 then update after. (still OK with rate limits)
            # We'll actually defer status to final by updating DB A again once (2 writes max per page).
            temp_page_id = upsert_db_a(db_a_index, page_url, page_title, page_ok, broken_count=0)

            # Process occurrences
            for it in merged:
                href = it.get("href", "")
                link_url = normalize_url(page_url, href)
                if not link_url:
                    continue
                link_url = strip_trailing_slash(link_url)

                if should_ignore_url(link_url) or has_skipped_extension(link_url):
                    continue

                dom_area = it.get("dom_area", "Main") or "Main"
                if dom_area in EXCLUDE_DOM_AREAS_SET:
                    continue

                anchor_text = it.get("anchor_text", "") or ""
                snippet = it.get("snippet", "") or ""
                locator_css = it.get("locator_css", "") or ""

                ui_group = it.get("ui_group", "") or ""
                ui_item = it.get("ui_item", "") or ""
                deep_link = it.get("deep_link", "") or ""
                click_path = ""
                if ui_group or ui_item:
                    click_path = f"{page_title} → {ui_group} → {ui_item}".strip(" →")
                elif breadcrumb:
                    click_path = breadcrumb

                if same_domain(link_url, domain):
                    link_type = "internal"
                    if not CHECK_INTERNAL:
                        continue
                    # internal is derived by whether it's reachable and what response is when visited
                    # For the first 120 run, we store internal as Active (best-effort) and later runs will correct.
                    result = "Active"
                    code = 200
                    err = ""
                else:
                    link_type = "external"
                    if not CHECK_EXTERNAL:
                        continue

                    d = domain_of(link_url)
                    if d in SKIP_DOMAINS:
                        code, err, result = None, "skipped_domain", "Blocked"
                    else:
                        if link_url in external_cache:
                            code, err, result = external_cache[link_url]
                        else:
                            c1, e1 = check_url(link_url)
                            code, err, result = double_check_broken(link_url, c1, e1)
                            external_cache[link_url] = (code, err, result)
                            time.sleep(CRAWL_SLEEP)

                if result == "Broken":
                    broken_in_page += 1

                newly_broken = upsert_db_b(
                    db_b_index=db_b_index,
                    source_page_id=temp_page_id,
                    source_page_url=page_url,
                    link_url=link_url,
                    link_type=link_type,
                    result=result,
                    http_code=code,
                    error=err or "",
                    anchor_text=anchor_text,
                    snippet=snippet,
                    breadcrumb=breadcrumb,
                    dom_area=dom_area if deep_link else dom_area,
                    ui_group=ui_group,
                    ui_item=ui_item,
                    click_path=click_path,
                    deep_link=deep_link,
                    locator_css=locator_css,
                )

                if newly_broken:
                    newly_broken_alerts.append(f"• {page_title} ({page_url}) -> {link_url}")

            # Final update DB A with correct status (Need Review if broken)
            upsert_db_a(db_a_index, page_url, page_title, page_ok, broken_count=broken_in_page)

            print(f"[{pages_crawled}/{MAX_PAGES}] {page_title} | broken_in_page={broken_in_page} | queue={len(queue)}", flush=True)

        browser.close()

    if newly_broken_alerts:
        msg = "⚠️ Link Health Hub 360 (Playwright BFS): Newly broken links\n" + "\n".join(newly_broken_alerts[:20])
        if len(newly_broken_alerts) > 20:
            msg += f"\n… and {len(newly_broken_alerts)-20} more."
        slack_notify(msg)

    print(f"Done. Pages crawled={pages_crawled}", flush=True)


if __name__ == "__main__":
    main()
