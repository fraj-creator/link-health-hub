#!/usr/bin/env python3
"""
Playwright BFS crawler (first 120 pages) -> Notion DB A + DB B
Schema-safe: checks DB properties and select options; skips missing instead of failing.

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
  STRICT_SCHEMA=true|false   # default false: skip missing props; true: fail fast with a report
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
STRICT_SCHEMA = os.environ.get("STRICT_SCHEMA", "false").lower() in ("1", "true", "yes", "y")

SKIP_DOMAINS = {d.strip().lower() for d in os.environ.get("SKIP_DOMAINS", "linkedin.com").split(",") if d.strip()}

EXCLUDE_DOM_AREAS = os.environ.get("EXCLUDE_DOM_AREAS", "Footer,Nav").strip()
EXCLUDE_DOM_AREAS_SET = {x.strip() for x in EXCLUDE_DOM_AREAS.split(",") if x.strip()}

DEFAULT_SKIP_EXT = [
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
    ".css", ".js", ".mjs", ".map",
    ".woff", ".woff2", ".ttf", ".eot",
    ".mp4", ".mov", ".webm", ".mp3", ".wav",
    ".pdf", ".zip", ".rar", ".7z",
]

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}


# -------------------------
# Notion property names (match your DB names, but schema-safe)
# -------------------------
# DB A
DBA_TITLE = "Title"              # (we auto-detect title prop anyway)
DBA_PRIMARY_URL = "Primary URL"
DBA_STATUS = "Status"            # Select: Active / Broken / Need Review
DBA_LAST_CRAWLED = "Last Crawled"
DBA_PAGES = "Pages"
DBA_CONTENT_TYPE = "Content Type"

# DB B
DBB_NAME = "Name"                # (we auto-detect title prop anyway)
DBB_SOURCE_CONTENT = "Source Content"
DBB_URL = "URL"
DBB_LINK_TYPE = "Link Type"      # Select: internal / external
DBB_RESULT = "Result"            # Select: Active / Broken / Blocked
DBB_HTTP = "HTTP Code"
DBB_ERROR = "Error"
DBB_FINDING_KEY = "Finding Key"
DBB_FIRST_SEEN = "First Seen"
DBB_LAST_SEEN = "Last Seen"
DBB_ANCHOR = "Anchor Text"
DBB_CONTEXT = "Context Snippet"
DBB_BREADCRUMB = "Breadcrumb Trail"

# Playwright fields (optional, schema-safe)
DBB_UI_GROUP = "UI Group"
DBB_UI_ITEM = "UI Item"
DBB_CLICK_PATH = "Click Path"
DBB_DEEP_LINK = "Deep Link"
DBB_RENDER_MODE = "Render Mode"
DBB_LOCATOR_CSS = "Locator CSS"
DBB_DOM_AREA = "DOM Area"


# -------------------------
# Helpers
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
    if href.startswith("#") or href.startswith(("mailto:", "tel:", "javascript:")):
        return None
    abs_url = urljoin(base, href)
    abs_url, _ = urldefrag(abs_url)
    return strip_trailing_slash(abs_url)

def has_skipped_extension(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in DEFAULT_SKIP_EXT)

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
    t = re.sub(r"^(Marble\s*[-–]\s*)+", "", t).strip()
    t = re.sub(r"(\s*[-–]\s*Marble)+$", "", t).strip()
    return t or title.strip()

def guess_pages_and_type(page_url: str) -> Tuple[Optional[str], Optional[str]]:
    path = urlparse(page_url).path.lower()
    base = SITE_BASE_URL.rstrip("/")
    if page_url.rstrip("/") == base:
        return "Home", "Website Page"
    if "/community" in path:
        return "Community", "Article"
    if "/companies/" in path:
        return "Companies", "Company"
    if "/companies" in path:
        return "Companies", "Directory"
    if "/opportunities" in path:
        return "Opportunities", "Listing"
    if "/how-it-works" in path:
        return "How it works", "Website Page"
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
# Notion API + Schema guard
# -------------------------
def notion_get(url: str) -> dict:
    notion_rl.wait()
    r = requests.get(url, headers=NOTION_HEADERS, timeout=TIMEOUT)
    if not r.ok:
        print("Notion GET error:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

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

def notion_get_db_schema(database_id: str) -> dict:
    return notion_get(f"https://api.notion.com/v1/databases/{database_id}")

def schema_props(schema: dict) -> Dict[str, dict]:
    return (schema.get("properties", {}) or {})

def find_title_prop_name(schema: dict) -> str:
    for name, meta in schema_props(schema).items():
        if meta.get("type") == "title":
            return name
    # fallback
    return "Name"

def has_prop(schema: dict, prop_name: str) -> bool:
    return prop_name in schema_props(schema)

def prop_type(schema: dict, prop_name: str) -> Optional[str]:
    meta = schema_props(schema).get(prop_name)
    return meta.get("type") if meta else None

def select_options(schema: dict, prop_name: str) -> List[str]:
    meta = schema_props(schema).get(prop_name) or {}
    if meta.get("type") != "select":
        return []
    sel = meta.get("select") or {}
    opts = sel.get("options") or []
    return [o.get("name") for o in opts if o.get("name")]

def safe_select(schema: dict, prop_name: str, desired: str, fallback: Optional[str] = None) -> Optional[dict]:
    if not has_prop(schema, prop_name):
        return None
    if prop_type(schema, prop_name) != "select":
        return None
    opts = set(select_options(schema, prop_name))
    if desired in opts:
        return {"select": {"name": desired}}
    if fallback and fallback in opts:
        return {"select": {"name": fallback}}
    # if no valid option, skip to avoid validation_error
    return None

def safe_url(schema: dict, prop_name: str, url: str) -> Optional[dict]:
    if not has_prop(schema, prop_name):
        return None
    if prop_type(schema, prop_name) != "url":
        return None
    return {"url": url}

def safe_number(schema: dict, prop_name: str, n: float) -> Optional[dict]:
    if not has_prop(schema, prop_name):
        return None
    if prop_type(schema, prop_name) != "number":
        return None
    return {"number": float(n)}

def safe_date(schema: dict, prop_name: str, iso: str) -> Optional[dict]:
    if not has_prop(schema, prop_name):
        return None
    if prop_type(schema, prop_name) != "date":
        return None
    return {"date": {"start": iso}}

def safe_rich_text(schema: dict, prop_name: str, text: str) -> Optional[dict]:
    if not has_prop(schema, prop_name):
        return None
    if prop_type(schema, prop_name) != "rich_text":
        return None
    return {"rich_text": rt(text)}

def safe_relation(schema: dict, prop_name: str, page_id: str) -> Optional[dict]:
    if not has_prop(schema, prop_name):
        return None
    if prop_type(schema, prop_name) != "relation":
        return None
    return {"relation": [{"id": page_id}]}

def filter_props(schema: dict, props: Dict[str, dict]) -> Dict[str, dict]:
    """Drop any props whose name doesn't exist in schema (avoids 'property does not exist')."""
    existing = schema_props(schema)
    return {k: v for k, v in props.items() if k in existing}

def schema_report_missing(schema: dict, wanted: List[str], label: str) -> None:
    missing = [p for p in wanted if not has_prop(schema, p)]
    if missing:
        print(f"[Schema] {label} missing props: {missing}")


# -------------------------
# Indexes
# -------------------------
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
# External check
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

def double_check_broken(url: str, c1: Optional[int], e1: Optional[str]) -> Tuple[Optional[int], Optional[str], str]:
    r1 = classify(c1)
    if r1 != "Broken":
        return c1, e1, r1
    time.sleep(0.8)
    c2, e2 = check_url(url)
    r2 = classify(c2)
    if r2 != "Broken":
        return c2, e2, r2
    return c1, e1, r1


# -------------------------
# Notion upserts (schema-safe)
# -------------------------
def upsert_db_a(db_a_schema: dict, db_a_index: Dict[str, str], title_prop: str,
               page_url: str, title: str, page_http_ok: bool, broken_count: int) -> str:
    key = strip_trailing_slash(page_url)
    existing_id = db_a_index.get(key)

    if not page_http_ok:
        desired_status = "Broken"
    elif broken_count > 0:
        desired_status = "Need Review"
    else:
        desired_status = "Active"

    props: Dict[str, dict] = {}
    # title
    props[title_prop] = {"title": [{"text": {"content": title or page_url}}]}

    # urls / dates / selects schema-safe
    u = safe_url(db_a_schema, DBA_PRIMARY_URL, page_url)
    if u: props[DBA_PRIMARY_URL] = u

    s = safe_select(db_a_schema, DBA_STATUS, desired_status, fallback="Active")
    if s: props[DBA_STATUS] = s

    d = safe_date(db_a_schema, DBA_LAST_CRAWLED, iso_now())
    if d: props[DBA_LAST_CRAWLED] = d

    # optional Pages + Content Type
    pages_val, type_val = guess_pages_and_type(page_url)
    if pages_val:
        psel = safe_select(db_a_schema, DBA_PAGES, pages_val, fallback="Other")
        if psel: props[DBA_PAGES] = psel
    if type_val:
        csel = safe_select(db_a_schema, DBA_CONTENT_TYPE, type_val, fallback="Website Page")
        if csel: props[DBA_CONTENT_TYPE] = csel

    props = filter_props(db_a_schema, props)

    if existing_id:
        notion_update_page(existing_id, props)
        return existing_id

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
    db_b_schema: dict,
    db_b_index: Dict[str, Tuple[str, Optional[str]]],
    title_prop: str,
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

    should_write = (existing is None) or (prev_result != result)
    if not should_write:
        return newly_broken

    props: Dict[str, dict] = {}
    props[title_prop] = {"title": [{"text": {"content": make_occ_name(anchor_text, link_url)}}]}

    rel = safe_relation(db_b_schema, DBB_SOURCE_CONTENT, source_page_id)
    if rel: props[DBB_SOURCE_CONTENT] = rel

    u = safe_url(db_b_schema, DBB_URL, link_url)
    if u: props[DBB_URL] = u

    lt = safe_select(db_b_schema, DBB_LINK_TYPE, link_type, fallback="external")
    if lt: props[DBB_LINK_TYPE] = lt

    rs = safe_select(db_b_schema, DBB_RESULT, result, fallback="Broken")
    if rs: props[DBB_RESULT] = rs

    if http_code is not None:
        n = safe_number(db_b_schema, DBB_HTTP, float(http_code))
        if n: props[DBB_HTTP] = n

    e = safe_rich_text(db_b_schema, DBB_ERROR, error or "")
    if e: props[DBB_ERROR] = e

    fk = safe_rich_text(db_b_schema, DBB_FINDING_KEY, finding_key)
    if fk: props[DBB_FINDING_KEY] = fk

    a = safe_rich_text(db_b_schema, DBB_ANCHOR, anchor_text or "")
    if a: props[DBB_ANCHOR] = a

    cs = safe_rich_text(db_b_schema, DBB_CONTEXT, snippet or "")
    if cs: props[DBB_CONTEXT] = cs

    bc = safe_rich_text(db_b_schema, DBB_BREADCRUMB, breadcrumb or "")
    if bc: props[DBB_BREADCRUMB] = bc

    # Playwright fields (optional)
    rm = safe_select(db_b_schema, DBB_RENDER_MODE, "Playwright", fallback=None)
    if rm: props[DBB_RENDER_MODE] = rm

    da = safe_select(db_b_schema, DBB_DOM_AREA, dom_area or "Unknown", fallback="Unknown")
    if da: props[DBB_DOM_AREA] = da

    ug = safe_rich_text(db_b_schema, DBB_UI_GROUP, ui_group or "")
    if ug: props[DBB_UI_GROUP] = ug

    ui = safe_rich_text(db_b_schema, DBB_UI_ITEM, ui_item or "")
    if ui: props[DBB_UI_ITEM] = ui

    cp = safe_rich_text(db_b_schema, DBB_CLICK_PATH, click_path or "")
    if cp: props[DBB_CLICK_PATH] = cp

    if deep_link:
        dl = safe_url(db_b_schema, DBB_DEEP_LINK, deep_link)
        if dl: props[DBB_DEEP_LINK] = dl

    lc = safe_rich_text(db_b_schema, DBB_LOCATOR_CSS, locator_css or "")
    if lc: props[DBB_LOCATOR_CSS] = lc

    now = iso_now()
    if existing:
        ls = safe_date(db_b_schema, DBB_LAST_SEEN, now)
        if ls: props[DBB_LAST_SEEN] = ls
        props = filter_props(db_b_schema, props)
        notion_update_page(existing[0], props)
        db_b_index[finding_key] = (existing[0], result)
    else:
        fs = safe_date(db_b_schema, DBB_FIRST_SEEN, now)
        if fs: props[DBB_FIRST_SEEN] = fs
        ls = safe_date(db_b_schema, DBB_LAST_SEEN, now)
        if ls: props[DBB_LAST_SEEN] = ls
        props = filter_props(db_b_schema, props)
        created = notion_create_page(DB_B_ID, props)
        db_b_index[finding_key] = (created["id"], result)

    return newly_broken


# -------------------------
# Breadcrumb
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
    let snippet = '';
    try {
      const p = a.parentElement;
      snippet = (p ? (p.innerText || '') : text).trim();
      snippet = snippet.replace(/\\s+/g, ' ');
      if (snippet.length > 180) snippet = snippet.slice(0, 177) + '...';
    } catch(e) {}
    let area = 'Main';
    const foot = a.closest('footer');
    const head = a.closest('header');
    const nav = a.closest('nav');
    if (foot) area = 'Footer';
    else if (nav) area = 'Nav';
    else if (head) area = 'Header';
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
    return [{
        "href": it.get("href", ""),
        "anchor_text": it.get("text", ""),
        "snippet": it.get("snippet", ""),
        "dom_area": it.get("area", "Main"),
        "locator_css": it.get("loc", ""),
    } for it in items]

def playwright_expand_tabs_and_accordions(pw_page) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    tabs = pw_page.locator('[role="tab"]')
    try:
        tab_count = tabs.count()
    except Exception:
        tab_count = 0

    def process_current(ui_group: str):
        btns = pw_page.locator('button[aria-expanded]')
        try:
            n = min(btns.count(), 40)
        except Exception:
            n = 0

        for i in range(n):
            try:
                b = btns.nth(i)
                text = (b.inner_text() or "").strip()
                if not text:
                    continue
                expanded = (b.get_attribute("aria-expanded") or "").lower()
                if expanded == "true":
                    continue

                b.scroll_into_view_if_needed(timeout=2000)
                before_url = pw_page.url
                b.click(timeout=3000)
                pw_page.wait_for_timeout(200)
                after_url = pw_page.url
                deep_link = after_url if after_url != before_url else ""

                if not deep_link:
                    continue

                links_now = playwright_collect_links(pw_page)
                for ln in links_now:
                    if ln.get("dom_area") in ("Header", "Nav", "Footer"):
                        continue
                    ln2 = dict(ln)
                    ln2["dom_area"] = "Accordion"
                    ln2["ui_group"] = ui_group
                    ln2["ui_item"] = text
                    ln2["deep_link"] = deep_link
                    results.append(ln2)
            except Exception:
                continue

    if tab_count > 0:
        for t in range(min(tab_count, 10)):
            try:
                tab = tabs.nth(t)
                ui_group = (tab.inner_text() or "").strip()
                tab.scroll_into_view_if_needed(timeout=2000)
                tab.click(timeout=3000)
                pw_page.wait_for_timeout(250)
                process_current(ui_group)
            except Exception:
                continue
    else:
        process_current("")

    return results


# -------------------------
# Main
# -------------------------
def main():
    base = strip_trailing_slash(SITE_BASE_URL)
    domain = urlparse(base).netloc.lower()

    print("Fetching Notion DB schemas (A + B)...", flush=True)
    db_a_schema = notion_get_db_schema(DB_A_ID)
    db_b_schema = notion_get_db_schema(DB_B_ID)

    db_a_title_prop = find_title_prop_name(db_a_schema)
    db_b_title_prop = find_title_prop_name(db_b_schema)

    # (Optional) Print missing properties report (but do not fail unless STRICT_SCHEMA)
    wanted_a = [DBA_PRIMARY_URL, DBA_STATUS, DBA_LAST_CRAWLED, DBA_PAGES, DBA_CONTENT_TYPE]
    wanted_b = [
        DBB_SOURCE_CONTENT, DBB_URL, DBB_LINK_TYPE, DBB_RESULT, DBB_HTTP, DBB_ERROR,
        DBB_FINDING_KEY, DBB_FIRST_SEEN, DBB_LAST_SEEN, DBB_ANCHOR, DBB_CONTEXT, DBB_BREADCRUMB,
        DBB_UI_GROUP, DBB_UI_ITEM, DBB_CLICK_PATH, DBB_DEEP_LINK, DBB_RENDER_MODE, DBB_LOCATOR_CSS, DBB_DOM_AREA
    ]
    schema_report_missing(db_a_schema, wanted_a, "DB A")
    schema_report_missing(db_b_schema, wanted_b, "DB B")

    if STRICT_SCHEMA:
        missing_a = [p for p in wanted_a if not has_prop(db_a_schema, p)]
        missing_b = [p for p in wanted_b if not has_prop(db_b_schema, p)]
        if missing_a or missing_b:
            raise SystemExit("STRICT_SCHEMA=true and some properties are missing. Create them in Notion then rerun.")

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
            page_url = drop_query(page_url)

            if page_url in seen:
                continue
            if not same_domain(page_url, domain):
                continue
            if should_ignore_url(page_url) or has_skipped_extension(page_url):
                continue

            seen.add(page_url)
            pages_crawled += 1
            breadcrumb = build_trail(parent, page_url)

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

            merged: List[Dict[str, str]] = []
            if page_ok:
                try:
                    merged = playwright_collect_links(page)
                except Exception:
                    merged = []

                try:
                    merged += playwright_expand_tabs_and_accordions(page)
                except Exception:
                    pass

            # enqueue internal
            internal_links = []
            for it in merged:
                absu = normalize_url(page_url, it.get("href", ""))
                if not absu:
                    continue
                absu = strip_trailing_slash(absu)
                if should_ignore_url(absu) or has_skipped_extension(absu):
                    continue
                if same_domain(absu, domain):
                    if absu not in parent:
                        parent[absu] = page_url
                    if absu not in seen:
                        internal_links.append(absu)

            for u in internal_links:
                queue.append(u)

            # We need DB A page id for relations; create/update later with broken_count computed
            page_id = upsert_db_a(
                db_a_schema=db_a_schema,
                db_a_index=db_a_index,
                title_prop=db_a_title_prop,
                page_url=page_url,
                title=page_title,
                page_http_ok=page_ok,
                broken_count=0,
            )

            broken_in_page = 0

            for it in merged:
                link_url = normalize_url(page_url, it.get("href", ""))
                if not link_url:
                    continue
                link_url = strip_trailing_slash(link_url)

                if should_ignore_url(link_url) or has_skipped_extension(link_url):
                    continue

                dom_area = (it.get("dom_area", "Main") or "Main").strip()
                if dom_area in EXCLUDE_DOM_AREAS_SET:
                    continue

                anchor_text = it.get("anchor_text", "") or ""
                snippet = it.get("snippet", "") or ""
                locator_css = it.get("locator_css", "") or ""

                ui_group = it.get("ui_group", "") or ""
                ui_item = it.get("ui_item", "") or ""
                deep_link = it.get("deep_link", "") or ""
                click_path = f"{page_title} → {ui_group} → {ui_item}".strip(" →") if (ui_group or ui_item) else breadcrumb

                if same_domain(link_url, domain):
                    if not CHECK_INTERNAL:
                        continue
                    link_type = "internal"
                    result = "Active"
                    code = 200
                    err = ""
                else:
                    if not CHECK_EXTERNAL:
                        continue
                    link_type = "external"
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
                    db_b_schema=db_b_schema,
                    db_b_index=db_b_index,
                    title_prop=db_b_title_prop,
                    source_page_id=page_id,
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

            # final DB A status update: Need Review if broken links in page
            upsert_db_a(
                db_a_schema=db_a_schema,
                db_a_index=db_a_index,
                title_prop=db_a_title_prop,
                page_url=page_url,
                title=page_title,
                page_http_ok=page_ok,
                broken_count=broken_in_page,
            )

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
