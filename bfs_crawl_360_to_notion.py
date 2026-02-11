#!/usr/bin/env python3
import os, re, time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urldefrag, urlunparse

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ---------------- Config ----------------
TIMEOUT = 15
USER_AGENT = "Mozilla/5.0 (Marble LinkHealthHub Playwright BFS)"
NOTION_VERSION = "2022-06-28"

NOTION_TOKEN = os.environ["NOTION_TOKEN"].strip()
DB_A_ID = os.environ["NOTION_DB_A_ID"].strip()
DB_B_ID = os.environ["NOTION_DB_B_ID"].strip()
SITE_BASE_URL = os.environ["SITE_BASE_URL"].strip()

MAX_PAGES = int(os.environ.get("MAX_PAGES", "120"))
CHECK_EXTERNAL = os.environ.get("CHECK_EXTERNAL", "true").lower() in ("1","true","yes","y")
CHECK_INTERNAL = os.environ.get("CHECK_INTERNAL", "true").lower() in ("1","true","yes","y")

CRAWL_SLEEP = float(os.environ.get("CRAWL_SLEEP", "0.25"))
NOTION_MIN_INTERVAL = float(os.environ.get("NOTION_MIN_INTERVAL", "0.5"))

# Backfill: aggiorna righe già esistenti se hanno campi vuoti (Link Type, DOM Area, Pages, Content Type)
BACKFILL_MISSING = os.environ.get("BACKFILL_MISSING", "true").lower() in ("1","true","yes","y")

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip() or None
SKIP_DOMAINS = {d.strip().lower() for d in os.environ.get("SKIP_DOMAINS", "linkedin.com").split(",") if d.strip()}
SKIP_URL_CONTAINS = [s.strip().lower() for s in os.environ.get("SKIP_URL_CONTAINS", "").split(",") if s.strip()]

EXCLUDE_DOM_AREAS_SET = {x.strip() for x in os.environ.get("EXCLUDE_DOM_AREAS", "Footer,Nav").split(",") if x.strip()}

DEFAULT_SKIP_EXT = [
    ".jpg",".jpeg",".png",".gif",".webp",".svg",".ico",
    ".css",".js",".mjs",".map",
    ".woff",".woff2",".ttf",".eot",
    ".mp4",".mov",".webm",".mp3",".wav",
    ".pdf",".zip",".rar",".7z",
]

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# ---------------- Notion names (desired) ----------------
# DB A
DBA_PRIMARY_URL = "Primary URL"
DBA_STATUS = "Status"           # Select: Active/Broken/Need Review
DBA_LAST_CRAWLED = "Last Crawled"
DBA_PAGES = "Pages"             # Select (you group by this)
DBA_CONTENT_TYPE = "Content Type"  # Select

# DB B
DBB_SOURCE_CONTENT = "Source Content"
DBB_URL = "URL"
DBB_LINK_TYPE = "Link Type"     # Select: External/Internal
DBB_RESULT = "Result"           # Select: Active/Broken/Blocked
DBB_HTTP = "HTTP Code"
DBB_ERROR = "Error"
DBB_FINDING_KEY = "Finding Key"
DBB_FIRST_SEEN = "First Seen"
DBB_LAST_SEEN = "Last Seen"
DBB_ANCHOR = "Anchor Text"
DBB_CONTEXT = "Context Snippet"
DBB_BREADCRUMB = "Breadcrumb Trail"

# Optional Playwright fields (if present)
DBB_UI_GROUP = "UI Group"
DBB_UI_ITEM = "UI Item"
DBB_CLICK_PATH = "Click Path"
DBB_DEEP_LINK = "Deep Link"       # URL
DBB_RENDER_MODE = "Render Mode"   # Select Static/Playwright
DBB_LOCATOR_CSS = "Locator CSS"
DBB_DOM_AREA = "DOM Area"         # Select Main/Header/Footer/Nav/Accordion/Unknown

# You added Pages also in DB B (optional)
DBB_PAGES = "Pages"               # Select (optional)

# ---------------- Rate limiter ----------------
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
    if href.startswith("#") or href.startswith(("mailto:","tel:","javascript:")):
        return None
    abs_url = urljoin(base, href)
    abs_url, _ = urldefrag(abs_url)
    return strip_trailing_slash(abs_url)

def has_skipped_extension(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in DEFAULT_SKIP_EXT)

def should_ignore_url(url: str) -> bool:
    return "/_next/" in url

def drop_query(url: str) -> str:
    p = urlparse(url)
    return urlunparse(p._replace(query=""))

def domain_of(url: str) -> str:
    return urlparse(url).netloc.lower()

def same_domain(url: str, domain: str) -> bool:
    try:
        return urlparse(url).netloc.lower() == domain.lower()
    except Exception:
        return False

def clean_title(title: str) -> str:
    if not title:
        return ""
    t = title.strip()
    t = re.sub(r"^(Marble\s*[-–]\s*)+", "", t).strip()
    t = re.sub(r"(\s*[-–]\s*Marble)+$", "", t).strip()
    return t or title.strip()

def classify_page(url: str) -> Tuple[str, str]:
    """
    Returns:
      pages (Select value)
      content_type (Select value)
    """
    path = urlparse(url).path.strip("/").lower()

    if path == "":
        return "Home", "Website Page"

    # Community
    if path == "community":
        return "Community", "Website Page"
    if path.startswith("community/"):
        return "Article", "Article"

    # Companies
    if path == "companies":
        return "Companies", "Directory"
    if path.startswith("companies/"):
        return "Companies", "Company"

    # Opportunities
    if path.startswith("opportunities"):
        return "Opportunities", "Listing"

    # Core pages
    if path.startswith("how-it-works"):
        return "How It Works", "Website Page"
    if path.startswith("careers"):
        return "Careers", "Website Page"
    if path.startswith("what-we-look-for"):
        return "What We Look For", "Website Page"
    if "faq" in path:
        return "FAQ", "Website Page"
    if "privacy" in path:
        return "Privacy", "Website Page"
    if "terms" in path:
        return "Terms", "Website Page"
    if path.startswith("about"):
        return "About", "Website Page"

    return "Other", "Website Page"

# ---------------- Slack ----------------
def slack_notify(text: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text, "mrkdwn": True}, timeout=TIMEOUT)
    except requests.RequestException:
        pass

# ---------------- Notion API ----------------
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
    return notion_post("https://api.notion.com/v1/pages", {"parent": {"database_id": database_id}, "properties": properties})

def notion_update_page(page_id: str, properties: dict) -> None:
    notion_patch(f"https://api.notion.com/v1/pages/{page_id}", {"properties": properties})

def get_db_schema(database_id: str) -> dict:
    return notion_get(f"https://api.notion.com/v1/databases/{database_id}")

def db_props(schema: dict) -> Dict[str, dict]:
    return (schema.get("properties", {}) or {})

def resolve_prop(schema: dict, desired: str) -> Optional[str]:
    """
    Resolve property name even if Notion has trailing spaces / case mismatch.
    Example: 'DOM Area' will match 'DOM Area '.
    """
    props = db_props(schema)
    if desired in props:
        return desired
    d = desired.strip()
    for k in props.keys():
        if k.strip() == d:
            return k
    for k in props.keys():
        if k.strip().lower() == d.lower():
            return k
    return None

def find_title_prop(schema: dict) -> str:
    for name, meta in db_props(schema).items():
        if meta.get("type") == "title":
            return name
    return "Name"

def prop_type(schema: dict, prop_name: str) -> Optional[str]:
    meta = db_props(schema).get(prop_name)
    return meta.get("type") if meta else None

def select_options_full(schema: dict, prop_name: str) -> List[dict]:
    meta = db_props(schema).get(prop_name) or {}
    if meta.get("type") != "select":
        return []
    sel = meta.get("select") or {}
    return sel.get("options") or []

def ensure_select_options(database_id: str, schema: dict, prop_name: str, required: List[str]) -> dict:
    """Add missing select options ONCE (safe). Returns refreshed schema."""
    actual = resolve_prop(schema, prop_name)
    if not actual:
        return schema
    if prop_type(schema, actual) != "select":
        return schema

    existing_opts = select_options_full(schema, actual)
    existing_names = {o.get("name") for o in existing_opts if o.get("name")}

    missing = [x for x in required if x not in existing_names]
    if not missing:
        return schema

    new_opts = list(existing_opts)
    for m in missing:
        new_opts.append({"name": m, "color": "gray"})

    payload = {"properties": {actual: {"select": {"options": new_opts}}}}
    notion_patch(f"https://api.notion.com/v1/databases/{database_id}", payload)
    return get_db_schema(database_id)

def safe_select(schema: dict, prop_name: str, value: str, fallback: Optional[str] = None) -> Optional[dict]:
    actual = resolve_prop(schema, prop_name)
    if not actual or prop_type(schema, actual) != "select":
        return None
    names = {o.get("name") for o in select_options_full(schema, actual)}
    if value in names:
        return {actual: {"select": {"name": value}}}
    if fallback and fallback in names:
        return {actual: {"select": {"name": fallback}}}
    return None

def safe_url(schema: dict, prop_name: str, value: str) -> Optional[dict]:
    actual = resolve_prop(schema, prop_name)
    if not actual or prop_type(schema, actual) != "url":
        return None
    return {actual: {"url": value}}

def safe_number(schema: dict, prop_name: str, value: float) -> Optional[dict]:
    actual = resolve_prop(schema, prop_name)
    if not actual or prop_type(schema, actual) != "number":
        return None
    return {actual: {"number": float(value)}}

def safe_date(schema: dict, prop_name: str, value_iso: str) -> Optional[dict]:
    actual = resolve_prop(schema, prop_name)
    if not actual or prop_type(schema, actual) != "date":
        return None
    return {actual: {"date": {"start": value_iso}}}

def safe_rich_text(schema: dict, prop_name: str, text: str) -> Optional[dict]:
    actual = resolve_prop(schema, prop_name)
    if not actual or prop_type(schema, actual) != "rich_text":
        return None
    return {actual: {"rich_text": rt(text)}}

def safe_relation(schema: dict, prop_name: str, page_id: str) -> Optional[dict]:
    actual = resolve_prop(schema, prop_name)
    if not actual or prop_type(schema, actual) != "relation":
        return None
    return {actual: {"relation": [{"id": page_id}]}}  # one relation item

# ---------------- Read helpers ----------------
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

# ---------------- HTTP checks ----------------
def check_url(url: str) -> Tuple[Optional[int], Optional[str]]:
    try:
        r = SESSION.head(url, allow_redirects=True, timeout=TIMEOUT)
        code = r.status_code
        if code in (403,405) or code >= 500:
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
    if code in (404,410):
        return "Broken"
    if code in (401,403,429,999):
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

# ---------------- Breadcrumb ----------------
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

# ---------------- Playwright extraction ----------------
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

def pw_collect(page) -> List[Dict[str, str]]:
    items = page.evaluate(JS_EXTRACT_LINKS)
    return [{
        "href": it.get("href",""),
        "anchor_text": it.get("text",""),
        "snippet": it.get("snippet",""),
        "dom_area": it.get("area","Main"),
        "locator_css": it.get("loc",""),
    } for it in items]

def pw_expand_tabs_accordions(page) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    tabs = page.locator('[role="tab"]')
    try:
        tab_count = tabs.count()
    except Exception:
        tab_count = 0

    def process_group(ui_group: str):
        btns = page.locator('button[aria-expanded]')
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
                if (b.get_attribute("aria-expanded") or "").lower() == "true":
                    continue
                b.scroll_into_view_if_needed(timeout=2000)
                before = page.url
                b.click(timeout=3000)
                page.wait_for_timeout(200)
                after = page.url
                deep = after if after != before else ""
                if not deep:
                    continue
                links_now = pw_collect(page)
                for ln in links_now:
                    if ln.get("dom_area") in ("Header","Nav","Footer"):
                        continue
                    ln2 = dict(ln)
                    ln2["dom_area"] = "Accordion"
                    ln2["ui_group"] = ui_group
                    ln2["ui_item"] = text
                    ln2["deep_link"] = deep
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
                page.wait_for_timeout(250)
                process_group(ui_group)
            except Exception:
                continue
    else:
        process_group("")

    return results

# ---------------- Index builders ----------------
def build_db_a_index(db_a_schema: dict) -> Dict[str, str]:
    pages = notion_query_all(DB_A_ID)
    idx: Dict[str, str] = {}
    a_url = resolve_prop(db_a_schema, DBA_PRIMARY_URL)
    if not a_url:
        return idx
    for pg in pages:
        u = prop_url(pg, a_url)
        if u:
            idx[strip_trailing_slash(u)] = pg["id"]
    return idx

def build_db_b_index(db_b_schema: dict) -> Dict[str, dict]:
    pages = notion_query_all(DB_B_ID)
    idx: Dict[str, dict] = {}

    fk_name = resolve_prop(db_b_schema, DBB_FINDING_KEY)
    res_name = resolve_prop(db_b_schema, DBB_RESULT)
    lt_name = resolve_prop(db_b_schema, DBB_LINK_TYPE)
    da_name = resolve_prop(db_b_schema, DBB_DOM_AREA)
    pg_name = resolve_prop(db_b_schema, DBB_PAGES)

    for pg in pages:
        fk = prop_rich_text(pg, fk_name) if fk_name else ""
        if not fk:
            continue
        idx[fk] = {
            "id": pg["id"],
            "result": prop_select(pg, res_name) if res_name else None,
            "has_link_type": bool(prop_select(pg, lt_name)) if lt_name else False,
            "has_dom_area": bool(prop_select(pg, da_name)) if da_name else False,
            "has_pages": bool(prop_select(pg, pg_name)) if pg_name else False,
        }
    return idx

# ---------------- Upserts ----------------
def upsert_db_a(db_a_schema: dict, db_a_title: str, db_a_index: Dict[str,str],
               page_url: str, title: str, page_ok: bool, broken_count: int) -> str:
    key = strip_trailing_slash(page_url)
    existing = db_a_index.get(key)

    pages_val, ctype_val = classify_page(page_url)

    status_val = "Broken" if not page_ok else ("Need Review" if broken_count > 0 else "Active")

    props = {}
    props[db_a_title] = {"title": [{"text": {"content": title or page_url}}]}

    props.update(safe_url(db_a_schema, DBA_PRIMARY_URL, page_url) or {})
    props.update(safe_date(db_a_schema, DBA_LAST_CRAWLED, iso_now()) or {})
    props.update(safe_select(db_a_schema, DBA_STATUS, status_val, fallback="Active") or {})
    props.update(safe_select(db_a_schema, DBA_PAGES, pages_val, fallback="Other") or {})
    props.update(safe_select(db_a_schema, DBA_CONTENT_TYPE, ctype_val, fallback="Website Page") or {})

    if existing:
        notion_update_page(existing, props)
        return existing

    created = notion_create_page(DB_A_ID, props)
    db_a_index[key] = created["id"]
    return created["id"]

def make_occ_name(anchor: str, url: str) -> str:
    dom = urlparse(url).netloc or url
    a = (anchor or "").strip()
    if not a:
        return dom
    if len(a) > 55:
        a = a[:52] + "..."
    return f"{dom} • {a}"

def upsert_db_b(db_b_schema: dict, db_b_title: str, db_b_index: Dict[str,dict],
               source_page_id: str, source_page_url: str,
               link_url: str, link_type: str, result: str,
               http_code: Optional[int], error: str,
               anchor: str, snippet: str, breadcrumb: str,
               dom_area: str,
               ui_group: str, ui_item: str, click_path: str,
               deep_link: str, locator_css: str,
               source_pages_val: str) -> bool:

    finding_key = f"{source_page_url} | {link_url}"
    existing = db_b_index.get(finding_key)
    prev_result = existing["result"] if existing else None
    newly_broken = (result == "Broken" and prev_result != "Broken")

    # decide whether to write (avoid Notion spam)
    should_write = False
    if not existing:
        should_write = True
    elif prev_result != result:
        should_write = True
    elif BACKFILL_MISSING:
        # backfill only if some important fields were missing
        if not existing.get("has_link_type") or not existing.get("has_dom_area") or (resolve_prop(db_b_schema, DBB_PAGES) and not existing.get("has_pages")):
            should_write = True

    if not should_write:
        return newly_broken

    props = {}
    props[db_b_title] = {"title": [{"text": {"content": make_occ_name(anchor, link_url)}}]}

    props.update(safe_relation(db_b_schema, DBB_SOURCE_CONTENT, source_page_id) or {})
    props.update(safe_url(db_b_schema, DBB_URL, link_url) or {})
    props.update(safe_rich_text(db_b_schema, DBB_FINDING_KEY, finding_key) or {})
    props.update(safe_rich_text(db_b_schema, DBB_ANCHOR, anchor or "") or {})
    props.update(safe_rich_text(db_b_schema, DBB_CONTEXT, snippet or "") or {})
    props.update(safe_rich_text(db_b_schema, DBB_BREADCRUMB, breadcrumb or "") or {})
    props.update(safe_rich_text(db_b_schema, DBB_ERROR, error or "") or {})

    # Selects (note: your DB uses 'External'/'Internal' capitalized)
    props.update(safe_select(db_b_schema, DBB_LINK_TYPE, link_type, fallback="External") or {})
    props.update(safe_select(db_b_schema, DBB_RESULT, result, fallback="Broken") or {})
    props.update(safe_select(db_b_schema, DBB_DOM_AREA, dom_area or "Unknown", fallback="Unknown") or {})
    props.update(safe_select(db_b_schema, DBB_RENDER_MODE, "Playwright", fallback="Playwright") or {})

    # Pages in DB B (optional): inherit from source page classification
    props.update(safe_select(db_b_schema, DBB_PAGES, source_pages_val, fallback="Other") or {})

    if http_code is not None:
        props.update(safe_number(db_b_schema, DBB_HTTP, float(http_code)) or {})

    props.update(safe_rich_text(db_b_schema, DBB_UI_GROUP, ui_group or "") or {})
    props.update(safe_rich_text(db_b_schema, DBB_UI_ITEM, ui_item or "") or {})
    props.update(safe_rich_text(db_b_schema, DBB_CLICK_PATH, click_path or "") or {})
    props.update(safe_rich_text(db_b_schema, DBB_LOCATOR_CSS, locator_css or "") or {})
    if deep_link:
        props.update(safe_url(db_b_schema, DBB_DEEP_LINK, deep_link) or {})

    now = iso_now()
    if existing:
        props.update(safe_date(db_b_schema, DBB_LAST_SEEN, now) or {})
        notion_update_page(existing["id"], props)
        existing["result"] = result
        existing["has_link_type"] = existing["has_link_type"] or bool(resolve_prop(db_b_schema, DBB_LINK_TYPE))
        existing["has_dom_area"] = existing["has_dom_area"] or bool(resolve_prop(db_b_schema, DBB_DOM_AREA))
        existing["has_pages"] = existing.get("has_pages", False) or bool(resolve_prop(db_b_schema, DBB_PAGES))
    else:
        props.update(safe_date(db_b_schema, DBB_FIRST_SEEN, now) or {})
        props.update(safe_date(db_b_schema, DBB_LAST_SEEN, now) or {})
        created = notion_create_page(DB_B_ID, props)
        db_b_index[finding_key] = {"id": created["id"], "result": result, "has_link_type": True, "has_dom_area": True, "has_pages": True}

    return newly_broken

# ---------------- Main ----------------
def main():
    base = strip_trailing_slash(SITE_BASE_URL)
    domain = urlparse(base).netloc.lower()

    print("Fetching DB schemas…", flush=True)
    db_a_schema = get_db_schema(DB_A_ID)
    db_b_schema = get_db_schema(DB_B_ID)

    db_a_title = find_title_prop(db_a_schema)
    db_b_title = find_title_prop(db_b_schema)

    # Ensure select options (so values actually get written instead of skipped)
    print("Ensuring Select options…", flush=True)

    # DB A: Status
    db_a_schema = ensure_select_options(DB_A_ID, db_a_schema, DBA_STATUS, ["Active","Broken","Need Review"])
    # DB A: Pages (your grouping)
    db_a_schema = ensure_select_options(DB_A_ID, db_a_schema, DBA_PAGES, [
        "Home","Community","Article","Companies","Opportunities","How It Works","Careers",
        "What We Look For","FAQ","Privacy","Terms","About","Other"
    ])
    # DB A: Content Type
    db_a_schema = ensure_select_options(DB_A_ID, db_a_schema, DBA_CONTENT_TYPE, [
        "Website Page","Directory","Company","Listing","Article"
    ])

    # DB B: Result, Link Type, Render Mode, DOM Area
    db_b_schema = ensure_select_options(DB_B_ID, db_b_schema, DBB_RESULT, ["Active","Broken","Blocked"])
    db_b_schema = ensure_select_options(DB_B_ID, db_b_schema, DBB_LINK_TYPE, ["External","Internal"])
    db_b_schema = ensure_select_options(DB_B_ID, db_b_schema, DBB_RENDER_MODE, ["Static","Playwright"])
    db_b_schema = ensure_select_options(DB_B_ID, db_b_schema, DBB_DOM_AREA, ["Main","Header","Footer","Nav","Accordion","Unknown"])

    # DB B optional Pages
    if resolve_prop(db_b_schema, DBB_PAGES):
        db_b_schema = ensure_select_options(DB_B_ID, db_b_schema, DBB_PAGES, [
            "Home","Community","Article","Companies","Opportunities","How It Works","Careers",
            "What We Look For","FAQ","Privacy","Terms","About","Other"
        ])

    print("Prefetching indices…", flush=True)
    db_a_index = build_db_a_index(db_a_schema)
    db_b_index = build_db_b_index(db_b_schema)
    print(f"DB A indexed: {len(db_a_index)} rows; DB B indexed: {len(db_b_index)} rows", flush=True)

    queue = deque([base])
    parent: Dict[str, Optional[str]] = {base: None}
    seen = set()
    pages_crawled = 0

    external_cache: Dict[str, Tuple[Optional[int], Optional[str], str]] = {}
    internal_cache: Dict[str, Tuple[Optional[int], Optional[str], str]] = {}

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
            pages_val, _ctype = classify_page(page_url)

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
                    merged = pw_collect(page)
                except Exception:
                    merged = []
                try:
                    merged += pw_expand_tabs_accordions(page)
                except Exception:
                    pass

            # enqueue internal pages
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
                        queue.append(absu)

            # create/update DB A (first pass)
            page_id = upsert_db_a(db_a_schema, db_a_title, db_a_index, page_url, page_title, page_ok, broken_count=0)

            broken_in_page = 0
            unique_links_in_page = set()

            for it in merged:
                link_url = normalize_url(page_url, it.get("href", ""))
                if not link_url:
                    continue
                link_url = strip_trailing_slash(link_url)

                if should_ignore_url(link_url) or has_skipped_extension(link_url):
                    continue
                if any(x in link_url.lower() for x in SKIP_URL_CONTAINS):
                    continue

                dom_area = (it.get("dom_area", "Main") or "Main").strip()
                if dom_area in EXCLUDE_DOM_AREAS_SET:
                    continue

                # dedupe same link multiple times in same page (footer repeats inside main etc)
                if link_url in unique_links_in_page:
                    continue
                unique_links_in_page.add(link_url)

                anchor_text = it.get("anchor_text", "") or ""
                snippet = it.get("snippet", "") or ""
                locator_css = it.get("locator_css", "") or ""

                ui_group = it.get("ui_group", "") or ""
                ui_item = it.get("ui_item", "") or ""
                deep_link = it.get("deep_link", "") or ""
                click_path = f"{page_title} → {ui_group} → {ui_item}".strip(" →") if (ui_group or ui_item) else breadcrumb

                # Determine type + check
                if same_domain(link_url, domain):
                    link_type = "Internal"
                    if not CHECK_INTERNAL:
                        continue
                    if link_url in internal_cache:
                        code, err, result = internal_cache[link_url]
                    else:
                        c1, e1 = check_url(link_url)
                        code, err, result = double_check_broken(link_url, c1, e1)
                        internal_cache[link_url] = (code, err, result)
                        time.sleep(CRAWL_SLEEP)
                else:
                    link_type = "External"
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
                    db_b_schema, db_b_title, db_b_index,
                    source_page_id=page_id,
                    source_page_url=page_url,
                    link_url=link_url,
                    link_type=link_type,
                    result=result,
                    http_code=code,
                    error=err or "",
                    anchor=anchor_text,
                    snippet=snippet,
                    breadcrumb=breadcrumb,
                    dom_area=("Accordion" if deep_link else dom_area),
                    ui_group=ui_group,
                    ui_item=ui_item,
                    click_path=click_path,
                    deep_link=deep_link,
                    locator_css=locator_css,
                    source_pages_val=pages_val,
                )
                if newly_broken:
                    newly_broken_alerts.append(f"• {page_title} ({page_url}) -> {link_url}")

            # final DB A update with Need Review if broken links found
            upsert_db_a(db_a_schema, db_a_title, db_a_index, page_url, page_title, page_ok, broken_count=broken_in_page)

            print(f"[{pages_crawled}/{MAX_PAGES}] {page_title} | Pages={pages_val} | broken_in_page={broken_in_page} | queue={len(queue)}", flush=True)

        browser.close()

    if newly_broken_alerts:
        msg = "⚠️ Link Health Hub 360 (Playwright BFS): Newly broken links\n" + "\n".join(newly_broken_alerts[:20])
        if len(newly_broken_alerts) > 20:
            msg += f"\n… and {len(newly_broken_alerts)-20} more."
        slack_notify(msg)

    print(f"Done. Pages crawled={pages_crawled}", flush=True)

if __name__ == "__main__":
    main()
