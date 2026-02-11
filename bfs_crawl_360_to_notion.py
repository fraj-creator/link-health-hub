#!/usr/bin/env python3
"""
Playwright BFS crawler -> Notion DB A (pages) + DB B (link occurrences)
- MAX_PAGES default 120
- Popola DB A: Pages (da URL) + Content Type + Status + Last Crawled + Companies (da /companies/<slug>)
- Popola DB B: Link Type (External/Internal) + Result + DOM Area + ecc.
- Schema-smart: risolve nomi property anche con spazi finali e aggiunge automaticamente le option mancanti delle Select/Multi-select
- Retry + backoff su Notion per evitare timeout

ENV required:
  NOTION_TOKEN
  NOTION_DB_A_ID
  NOTION_DB_B_ID
  SITE_BASE_URL

ENV optional:
  MAX_PAGES=120
  CHECK_EXTERNAL=true|false
  CHECK_INTERNAL=true|false
  BACKFILL_MISSING=true|false
  CRAWL_SLEEP=0.35
  NOTION_MIN_INTERVAL=0.9
  SKIP_DOMAINS=linkedin.com,...
  EXCLUDE_DOM_AREAS=Footer,Nav
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
TIMEOUT = 45  # Notion/HTTP timeout
USER_AGENT = "Mozilla/5.0 (Marble LinkHealthHub Playwright BFS)"
NOTION_VERSION = "2022-06-28"

NOTION_TOKEN = os.environ["NOTION_TOKEN"].strip()
DB_A_ID = os.environ["NOTION_DB_A_ID"].strip()
DB_B_ID = os.environ["NOTION_DB_B_ID"].strip()
SITE_BASE_URL = os.environ["SITE_BASE_URL"].strip()

MAX_PAGES = int(os.environ.get("MAX_PAGES", "120"))
CHECK_EXTERNAL = os.environ.get("CHECK_EXTERNAL", "true").lower() in ("1", "true", "yes", "y")
CHECK_INTERNAL = os.environ.get("CHECK_INTERNAL", "true").lower() in ("1", "true", "yes", "y")
BACKFILL_MISSING = os.environ.get("BACKFILL_MISSING", "true").lower() in ("1", "true", "yes", "y")

CRAWL_SLEEP = float(os.environ.get("CRAWL_SLEEP", "0.35"))
NOTION_MIN_INTERVAL = float(os.environ.get("NOTION_MIN_INTERVAL", "0.9"))

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip() or None
SKIP_DOMAINS = {d.strip().lower() for d in os.environ.get("SKIP_DOMAINS", "linkedin.com").split(",") if d.strip()}
EXCLUDE_DOM_AREAS_SET = {x.strip() for x in os.environ.get("EXCLUDE_DOM_AREAS", "Footer,Nav").split(",") if x.strip()}

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
# Notion property names (desired)
# -------------------------
# DB A
DBA_PRIMARY_URL = "Primary URL"
DBA_STATUS = "Status"              # Select: Active / Broken / Need Review
DBA_LAST_CRAWLED = "Last Crawled"  # Date
DBA_PAGES = "Pages"                # Select or Multi-select (tu raggruppi per questa)
DBA_CONTENT_TYPE = "Content Type"  # Select or Multi-select
DBA_COMPANY = "Companies"          # Select or Multi-select (nuova: nome company da /companies/<slug>)

# DB B
DBB_SOURCE_CONTENT = "Source Content"   # Relation -> DB A
DBB_URL = "URL"                         # URL
DBB_LINK_TYPE = "Link Type"             # Select/Multi-select: External/Internal
DBB_RESULT = "Result"                   # Select/Multi-select: Active/Broken/Blocked
DBB_HTTP = "HTTP Code"                  # Number
DBB_ERROR = "Error"                     # Rich text
DBB_FINDING_KEY = "Finding Key"         # Rich text
DBB_FIRST_SEEN = "First Seen"           # Date
DBB_LAST_SEEN = "Last Seen"             # Date
DBB_ANCHOR = "Anchor Text"              # Rich text
DBB_CONTEXT = "Context Snippet"         # Rich text
DBB_BREADCRUMB = "Breadcrumb Trail"     # Rich text

# Optional Playwright fields in DB B (if exist)
DBB_UI_GROUP = "UI Group"
DBB_UI_ITEM = "UI Item"
DBB_CLICK_PATH = "Click Path"
DBB_DEEP_LINK = "Deep Link"            # URL
DBB_RENDER_MODE = "Render Mode"        # Select/Multi-select: Static/Playwright
DBB_LOCATOR_CSS = "Locator CSS"
DBB_DOM_AREA = "DOM Area"              # Select/Multi-select: Main/Header/Footer/Nav/Accordion/Unknown
DBB_PAGES = "Pages"                    # (optional) se l’hai aggiunto anche in DB B


# -------------------------
# Rate limiter
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


# -------------------------
# Small helpers
# -------------------------
def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def rt(text: str) -> list:
    return [{"text": {"content": text}}]

def strip_trailing_slash(url: str) -> str:
    if url.endswith("/") and len(url) > 8:
        return url[:-1]
    return url

def drop_query(url: str) -> str:
    p = urlparse(url)
    return urlunparse(p._replace(query=""))

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

def same_domain(url: str, domain: str) -> bool:
    try:
        return urlparse(url).netloc.lower() == domain.lower()
    except Exception:
        return False

def domain_of(url: str) -> str:
    return urlparse(url).netloc.lower()

def clean_title(title: str) -> str:
    if not title:
        return ""
    t = title.strip()
    t = re.sub(r"^(Marble\s*[-–]\s*)+", "", t).strip()
    t = re.sub(r"(\s*[-–]\s*Marble)+$", "", t).strip()
    return t or title.strip()

def slug_to_company_name(slug: str) -> str:
    s = slug.strip().strip("/")
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    # title-case, ma conserva parole corte/brand come vuoi (qui semplice)
    return s.title() if s else ""


# -------------------------
# Classificazione Pages/Content Type/Company da URL
# -------------------------
def classify_from_url(page_url: str) -> Tuple[str, str, str]:
    """
    Returns:
      pages_value (Select/Multi-select value per DB A.Pages)
      content_type_value (Select/Multi-select value per DB A.Content Type)
      company_value (Select/Multi-select value per DB A.Companies, oppure "" se non è company page)
    """
    base = strip_trailing_slash(SITE_BASE_URL)
    u = strip_trailing_slash(page_url)
    if u == base:
        return "Home", "Website Page", ""

    path = urlparse(u).path.strip("/").lower()

    # community
    if path == "community":
        return "Community", "Website Page", ""
    if path.startswith("community/"):
        return "Article", "Article", ""

    # companies
    if path == "companies":
        return "Companies", "Directory", ""
    if path.startswith("companies/"):
        slug = path.split("/", 1)[1]
        return "Companies", "Company", slug_to_company_name(slug)

    # opportunities
    if path.startswith("opportunities"):
        return "Opportunities", "Listing", ""

    # core pages
    if path.startswith("how-it-works"):
        return "How It Works", "Website Page", ""
    if path.startswith("careers"):
        return "Careers", "Website Page", ""
    if path.startswith("about"):
        return "About", "Website Page", ""
    if path.startswith("faq") or "faq" in path:
        return "FAQ", "Website Page", ""
    if path.startswith("what-we-look-for"):
        return "What We Look For", "Website Page", ""
    if path.startswith("privacy-and-terms"):
        return "Privacy & Terms", "Website Page", ""
    if "privacy" in path:
        return "Privacy", "Website Page", ""
    if "terms" in path:
        return "Terms", "Website Page", ""

    return "Other", "Website Page", ""


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
# Notion API with retries
# -------------------------
def _notion_request(method: str, url: str, payload: Optional[dict] = None) -> dict:
    backoffs = [0.6, 1.2, 2.5, 5.0, 9.0]
    last_err = None

    for wait_s in backoffs:
        try:
            notion_rl.wait()

            if method == "GET":
                r = requests.get(url, headers=NOTION_HEADERS, timeout=TIMEOUT)
            elif method == "POST":
                r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=TIMEOUT)
            elif method == "PATCH":
                r = requests.patch(url, headers=NOTION_HEADERS, json=payload, timeout=TIMEOUT)
            else:
                raise ValueError("Unsupported method")

            if r.status_code in (429, 500, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
                time.sleep(wait_s)
                continue

            if not r.ok:
                print(f"Notion {method} error:", r.status_code, r.text)
            r.raise_for_status()
            return r.json() if r.text else {}

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
            last_err = e
            time.sleep(wait_s)
            continue
        except requests.RequestException as e:
            last_err = e
            time.sleep(wait_s)
            continue

    raise last_err or RuntimeError("Notion request failed after retries")

def notion_get(url: str) -> dict:
    return _notion_request("GET", url)

def notion_post(url: str, payload: dict) -> dict:
    return _notion_request("POST", url, payload)

def notion_patch(url: str, payload: dict) -> dict:
    return _notion_request("PATCH", url, payload)

def notion_get_db_schema(database_id: str) -> dict:
    return notion_get(f"https://api.notion.com/v1/databases/{database_id}")

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
    payload = {"parent": {"database_id": database_id}, "properties": properties}
    return notion_post("https://api.notion.com/v1/pages", payload)

def notion_update_page(page_id: str, properties: dict) -> None:
    payload = {"properties": properties}
    notion_patch(f"https://api.notion.com/v1/pages/{page_id}", payload)


# -------------------------
# Notion schema helpers (resolve prop + select/multiselect)
# -------------------------
def schema_props(schema: dict) -> Dict[str, dict]:
    return (schema.get("properties", {}) or {})

def resolve_prop(schema: dict, desired: str) -> Optional[str]:
    """Match also if Notion prop has trailing spaces, case mismatch."""
    props = schema_props(schema)
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

def prop_type(schema: dict, prop_name: str) -> Optional[str]:
    meta = schema_props(schema).get(prop_name)
    return meta.get("type") if meta else None

def find_title_prop(schema: dict) -> str:
    for name, meta in schema_props(schema).items():
        if meta.get("type") == "title":
            return name
    return "Name"

def select_option_names(schema: dict, prop_name: str) -> List[str]:
    meta = schema_props(schema).get(prop_name) or {}
    t = meta.get("type")
    if t == "select":
        return [o.get("name") for o in (meta.get("select", {}) or {}).get("options", []) if o.get("name")]
    if t == "multi_select":
        return [o.get("name") for o in (meta.get("multi_select", {}) or {}).get("options", []) if o.get("name")]
    return []

def ensure_option(database_id: str, schema: dict, prop_name_desired: str, option_name: str) -> dict:
    """Add a single option if missing (select or multi_select). Returns refreshed schema if changed."""
    actual = resolve_prop(schema, prop_name_desired)
    if not actual:
        return schema
    t = prop_type(schema, actual)
    if t not in ("select", "multi_select"):
        return schema

    existing = set(select_option_names(schema, actual))
    if option_name in existing:
        return schema

    meta = schema_props(schema).get(actual) or {}
    if t == "select":
        opts = (meta.get("select", {}) or {}).get("options", []) or []
        new_opts = list(opts) + [{"name": option_name, "color": "gray"}]
        payload = {"properties": {actual: {"select": {"options": new_opts}}}}
    else:
        opts = (meta.get("multi_select", {}) or {}).get("options", []) or []
        new_opts = list(opts) + [{"name": option_name, "color": "gray"}]
        payload = {"properties": {actual: {"multi_select": {"options": new_opts}}}}

    notion_patch(f"https://api.notion.com/v1/databases/{database_id}", payload)
    return notion_get_db_schema(database_id)

def ensure_options_bulk(database_id: str, schema: dict, prop_name_desired: str, required: List[str]) -> dict:
    for opt in required:
        schema = ensure_option(database_id, schema, prop_name_desired, opt)
    return schema

def set_select_or_multi(schema: dict, prop_name_desired: str, value: str) -> Dict[str, dict]:
    actual = resolve_prop(schema, prop_name_desired)
    if not actual:
        return {}
    t = prop_type(schema, actual)
    if t == "select":
        return {actual: {"select": {"name": value}}}
    if t == "multi_select":
        return {actual: {"multi_select": [{"name": value}]}}
    return {}

def set_url(schema: dict, prop_name_desired: str, value: str) -> Dict[str, dict]:
    actual = resolve_prop(schema, prop_name_desired)
    if not actual or prop_type(schema, actual) != "url":
        return {}
    return {actual: {"url": value}}

def set_number(schema: dict, prop_name_desired: str, value: float) -> Dict[str, dict]:
    actual = resolve_prop(schema, prop_name_desired)
    if not actual or prop_type(schema, actual) != "number":
        return {}
    return {actual: {"number": float(value)}}

def set_date(schema: dict, prop_name_desired: str, value_iso: str) -> Dict[str, dict]:
    actual = resolve_prop(schema, prop_name_desired)
    if not actual or prop_type(schema, actual) != "date":
        return {}
    return {actual: {"date": {"start": value_iso}}}

def set_rich_text(schema: dict, prop_name_desired: str, text: str) -> Dict[str, dict]:
    actual = resolve_prop(schema, prop_name_desired)
    if not actual or prop_type(schema, actual) != "rich_text":
        return {}
    return {actual: {"rich_text": rt(text)}}

def set_relation(schema: dict, prop_name_desired: str, page_id: str) -> Dict[str, dict]:
    actual = resolve_prop(schema, prop_name_desired)
    if not actual or prop_type(schema, actual) != "relation":
        return {}
    return {actual: {"relation": [{"id": page_id}]}}


# -------------------------
# Read props from pages
# -------------------------
def page_prop_url(page: dict, prop_name_actual: str) -> Optional[str]:
    p = (page.get("properties", {}) or {}).get(prop_name_actual, {})
    return p.get("url")

def page_prop_select_or_multi(page: dict, prop_name_actual: str) -> Optional[str]:
    p = (page.get("properties", {}) or {}).get(prop_name_actual, {})
    if "select" in p and p["select"]:
        return p["select"].get("name")
    if "multi_select" in p and p["multi_select"]:
        return p["multi_select"][0].get("name")
    return None

def page_prop_rich_text(page: dict, prop_name_actual: str) -> str:
    p = (page.get("properties", {}) or {}).get(prop_name_actual, {})
    t = p.get("rich_text", [])
    if t and isinstance(t, list):
        return "".join([x.get("plain_text", "") for x in t]).strip()
    return ""


# -------------------------
# Index builders
# -------------------------
def build_db_a_index(db_a_schema: dict) -> Dict[str, str]:
    idx: Dict[str, str] = {}
    url_prop = resolve_prop(db_a_schema, DBA_PRIMARY_URL)
    if not url_prop:
        return idx
    for pg in notion_query_all(DB_A_ID):
        u = page_prop_url(pg, url_prop)
        if u:
            idx[strip_trailing_slash(u)] = pg["id"]
    return idx

def build_db_b_index(db_b_schema: dict) -> Dict[str, dict]:
    idx: Dict[str, dict] = {}
    fk_prop = resolve_prop(db_b_schema, DBB_FINDING_KEY)
    res_prop = resolve_prop(db_b_schema, DBB_RESULT)
    lt_prop = resolve_prop(db_b_schema, DBB_LINK_TYPE)
    da_prop = resolve_prop(db_b_schema, DBB_DOM_AREA)

    for pg in notion_query_all(DB_B_ID):
        fk = page_prop_rich_text(pg, fk_prop) if fk_prop else ""
        if not fk:
            continue
        idx[fk] = {
            "id": pg["id"],
            "result": page_prop_select_or_multi(pg, res_prop) if res_prop else None,
            "has_link_type": bool(page_prop_select_or_multi(pg, lt_prop)) if lt_prop else False,
            "has_dom_area": bool(page_prop_select_or_multi(pg, da_prop)) if da_prop else False,
        }
    return idx


# -------------------------
# Link checking
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

def pw_collect(pw_page) -> List[Dict[str, str]]:
    items = pw_page.evaluate(JS_EXTRACT_LINKS)
    return [{
        "href": it.get("href", ""),
        "anchor_text": it.get("text", ""),
        "snippet": it.get("snippet", ""),
        "dom_area": it.get("area", "Main"),
        "locator_css": it.get("loc", ""),
    } for it in items]

def pw_expand_tabs_accordions(pw_page) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    tabs = pw_page.locator('[role="tab"]')
    try:
        tab_count = tabs.count()
    except Exception:
        tab_count = 0

    def process_group(ui_group: str):
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
                if (b.get_attribute("aria-expanded") or "").lower() == "true":
                    continue
                b.scroll_into_view_if_needed(timeout=2000)
                before = pw_page.url
                b.click(timeout=3000)
                pw_page.wait_for_timeout(200)
                after = pw_page.url
                deep = after if after != before else ""
                if not deep:
                    continue

                links_now = pw_collect(pw_page)
                for ln in links_now:
                    if ln.get("dom_area") in ("Header", "Nav", "Footer"):
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
                pw_page.wait_for_timeout(250)
                process_group(ui_group)
            except Exception:
                continue
    else:
        process_group("")

    return results


# -------------------------
# Upserts
# -------------------------
def upsert_db_a(
    db_a_schema: dict,
    db_a_title_prop: str,
    db_a_index: Dict[str, str],
    page_url: str,
    title: str,
    page_http_ok: bool,
    broken_count: int,
) -> str:
    key = strip_trailing_slash(page_url)
    existing_id = db_a_index.get(key)

    pages_val, ctype_val, company_val = classify_from_url(page_url)

    # assicurati che le option esistano (così Notion le accetta)
    # NOTE: aggiorniamo schema "al volo" solo se serve
    for opt in (pages_val,):
        db_a_schema = ensure_option(DB_A_ID, db_a_schema, DBA_PAGES, opt)
    for opt in (ctype_val,):
        db_a_schema = ensure_option(DB_A_ID, db_a_schema, DBA_CONTENT_TYPE, opt)
    if company_val:
        db_a_schema = ensure_option(DB_A_ID, db_a_schema, DBA_COMPANY, company_val)

    if not page_http_ok:
        status_val = "Broken"
    elif broken_count > 0:
        status_val = "Need Review"
    else:
        status_val = "Active"

    db_a_schema = ensure_option(DB_A_ID, db_a_schema, DBA_STATUS, status_val)

    props: Dict[str, dict] = {}
    props[db_a_title_prop] = {"title": [{"text": {"content": title or page_url}}]}

    props.update(set_url(db_a_schema, DBA_PRIMARY_URL, page_url))
    props.update(set_date(db_a_schema, DBA_LAST_CRAWLED, iso_now()))
    props.update(set_select_or_multi(db_a_schema, DBA_STATUS, status_val))
    props.update(set_select_or_multi(db_a_schema, DBA_PAGES, pages_val))
    props.update(set_select_or_multi(db_a_schema, DBA_CONTENT_TYPE, ctype_val))

    if company_val:
        props.update(set_select_or_multi(db_a_schema, DBA_COMPANY, company_val))

    if existing_id:
        notion_update_page(existing_id, props)
        return existing_id

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


def upsert_db_b(
    db_b_schema: dict,
    db_b_title_prop: str,
    db_b_index: Dict[str, dict],
    source_page_id: str,
    source_page_url: str,
    link_url: str,
    link_type_val: str,   # External / Internal
    result_val: str,      # Active / Broken / Blocked
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
    pages_val_for_b: str,  # (optional) se DB B ha Pages
) -> bool:
    finding_key = f"{source_page_url} | {link_url}"

    existing = db_b_index.get(finding_key)
    prev_result = existing["result"] if existing else None
    newly_broken = (result_val == "Broken" and prev_result != "Broken")

    # decidere se scrivere (risparmia API)
    should_write = False
    if not existing:
        should_write = True
    elif prev_result != result_val:
        should_write = True
    elif BACKFILL_MISSING:
        if not existing.get("has_link_type") or not existing.get("has_dom_area"):
            should_write = True

    if not should_write:
        return newly_broken

    # assicurati option esistano
    db_b_schema = ensure_option(DB_B_ID, db_b_schema, DBB_LINK_TYPE, link_type_val)
    db_b_schema = ensure_option(DB_B_ID, db_b_schema, DBB_RESULT, result_val)
    db_b_schema = ensure_option(DB_B_ID, db_b_schema, DBB_DOM_AREA, dom_area or "Unknown")
    db_b_schema = ensure_option(DB_B_ID, db_b_schema, DBB_RENDER_MODE, "Playwright")
    if resolve_prop(db_b_schema, DBB_PAGES):
        db_b_schema = ensure_option(DB_B_ID, db_b_schema, DBB_PAGES, pages_val_for_b)

    props: Dict[str, dict] = {}
    props[db_b_title_prop] = {"title": [{"text": {"content": make_occ_name(anchor_text, link_url)}}]}

    props.update(set_relation(db_b_schema, DBB_SOURCE_CONTENT, source_page_id))
    props.update(set_url(db_b_schema, DBB_URL, link_url))
    props.update(set_rich_text(db_b_schema, DBB_FINDING_KEY, finding_key))
    props.update(set_rich_text(db_b_schema, DBB_ANCHOR, anchor_text or ""))
    props.update(set_rich_text(db_b_schema, DBB_CONTEXT, snippet or ""))
    props.update(set_rich_text(db_b_schema, DBB_BREADCRUMB, breadcrumb or ""))
    props.update(set_rich_text(db_b_schema, DBB_ERROR, error or ""))

    props.update(set_select_or_multi(db_b_schema, DBB_LINK_TYPE, link_type_val))
    props.update(set_select_or_multi(db_b_schema, DBB_RESULT, result_val))
    props.update(set_select_or_multi(db_b_schema, DBB_DOM_AREA, dom_area or "Unknown"))
    props.update(set_select_or_multi(db_b_schema, DBB_RENDER_MODE, "Playwright"))

    if resolve_prop(db_b_schema, DBB_PAGES):
        props.update(set_select_or_multi(db_b_schema, DBB_PAGES, pages_val_for_b))

    if http_code is not None:
        props.update(set_number(db_b_schema, DBB_HTTP, float(http_code)))

    props.update(set_rich_text(db_b_schema, DBB_UI_GROUP, ui_group or ""))
    props.update(set_rich_text(db_b_schema, DBB_UI_ITEM, ui_item or ""))
    props.update(set_rich_text(db_b_schema, DBB_CLICK_PATH, click_path or ""))
    props.update(set_rich_text(db_b_schema, DBB_LOCATOR_CSS, locator_css or ""))
    if deep_link:
        props.update(set_url(db_b_schema, DBB_DEEP_LINK, deep_link))

    now = iso_now()
    if existing:
        props.update(set_date(db_b_schema, DBB_LAST_SEEN, now))
        notion_update_page(existing["id"], props)
        existing["result"] = result_val
        existing["has_link_type"] = True
        existing["has_dom_area"] = True
    else:
        props.update(set_date(db_b_schema, DBB_FIRST_SEEN, now))
        props.update(set_date(db_b_schema, DBB_LAST_SEEN, now))
        created = notion_create_page(DB_B_ID, props)
        db_b_index[finding_key] = {"id": created["id"], "result": result_val, "has_link_type": True, "has_dom_area": True}

    return newly_broken


# -------------------------
# Main
# -------------------------
def main():
    base = strip_trailing_slash(SITE_BASE_URL)
    domain = urlparse(base).netloc.lower()

    print("Fetching Notion DB schemas (A + B)…", flush=True)
    db_a_schema = notion_get_db_schema(DB_A_ID)
    db_b_schema = notion_get_db_schema(DB_B_ID)

    db_a_title_prop = find_title_prop(db_a_schema)
    db_b_title_prop = find_title_prop(db_b_schema)

    # Pre-seed base options (così Pages/About ecc funzionano subito)
    print("Ensuring base select options…", flush=True)
    db_a_schema = ensure_options_bulk(DB_A_ID, db_a_schema, DBA_STATUS, ["Active", "Broken", "Need Review"])
    db_a_schema = ensure_options_bulk(DB_A_ID, db_a_schema, DBA_PAGES, [
        "Home", "Community", "Article", "Companies", "Opportunities", "How It Works", "Careers",
        "What We Look For", "FAQ", "Privacy", "Terms", "Privacy & Terms", "About", "Other"
    ])
    db_a_schema = ensure_options_bulk(DB_A_ID, db_a_schema, DBA_CONTENT_TYPE, [
        "Website Page", "Directory", "Company", "Listing", "Article"
    ])
    # Companies: è dinamico (cryobio, aerleum, …). Qui non pre-seediamo tutto.

    db_b_schema = ensure_options_bulk(DB_B_ID, db_b_schema, DBB_RESULT, ["Active", "Broken", "Blocked"])
    db_b_schema = ensure_options_bulk(DB_B_ID, db_b_schema, DBB_LINK_TYPE, ["External", "Internal"])
    db_b_schema = ensure_options_bulk(DB_B_ID, db_b_schema, DBB_RENDER_MODE, ["Static", "Playwright"])
    db_b_schema = ensure_options_bulk(DB_B_ID, db_b_schema, DBB_DOM_AREA, ["Main", "Header", "Footer", "Nav", "Accordion", "Unknown"])

    print("Prefetching indices (DB A + DB B)…", flush=True)
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

            pages_val, ctype_val, company_val = classify_from_url(page_url)

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

            # DB A upsert (pass 1)
            page_id = upsert_db_a(
                db_a_schema=db_a_schema,
                db_a_title_prop=db_a_title_prop,
                db_a_index=db_a_index,
                page_url=page_url,
                title=page_title,
                page_http_ok=page_ok,
                broken_count=0,
            )

            broken_in_page = 0
            unique_links_in_page = set()

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

                # de-dupe dentro la stessa pagina (footer ripetuto ecc)
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

                # type + check
                if same_domain(link_url, domain):
                    if not CHECK_INTERNAL:
                        continue
                    link_type_val = "Internal"
                    if link_url in internal_cache:
                        code, err, result_val = internal_cache[link_url]
                    else:
                        c1, e1 = check_url(link_url)
                        code, err, result_val = double_check_broken(link_url, c1, e1)
                        internal_cache[link_url] = (code, err, result_val)
                        time.sleep(CRAWL_SLEEP)
                else:
                    if not CHECK_EXTERNAL:
                        continue
                    link_type_val = "External"
                    d = domain_of(link_url)
                    if d in SKIP_DOMAINS:
                        code, err, result_val = None, "skipped_domain", "Blocked"
                    else:
                        if link_url in external_cache:
                            code, err, result_val = external_cache[link_url]
                        else:
                            c1, e1 = check_url(link_url)
                            code, err, result_val = double_check_broken(link_url, c1, e1)
                            external_cache[link_url] = (code, err, result_val)
                            time.sleep(CRAWL_SLEEP)

                if result_val == "Broken":
                    broken_in_page += 1

                newly_broken = upsert_db_b(
                    db_b_schema=db_b_schema,
                    db_b_title_prop=db_b_title_prop,
                    db_b_index=db_b_index,
                    source_page_id=page_id,
                    source_page_url=page_url,
                    link_url=link_url,
                    link_type_val=link_type_val,
                    result_val=result_val,
                    http_code=code,
                    error=err or "",
                    anchor_text=anchor_text,
                    snippet=snippet,
                    breadcrumb=breadcrumb,
                    dom_area=("Accordion" if deep_link else dom_area),
                    ui_group=ui_group,
                    ui_item=ui_item,
                    click_path=click_path,
                    deep_link=deep_link,
                    locator_css=locator_css,
                    pages_val_for_b=pages_val,  # se DB B ha Pages, eredita
                )

                if newly_broken:
                    newly_broken_alerts.append(f"• {page_title} ({page_url}) -> {link_url}")

            # DB A upsert (pass 2): Need Review se broken links
            upsert_db_a(
                db_a_schema=db_a_schema,
                db_a_title_prop=db_a_title_prop,
                db_a_index=db_a_index,
                page_url=page_url,
                title=page_title,
                page_http_ok=page_ok,
                broken_count=broken_in_page,
            )

            print(f"[{pages_crawled}/{MAX_PAGES}] {page_title} | Pages={pages_val} | Company={company_val or '-'} | broken_in_page={broken_in_page} | queue={len(queue)}", flush=True)

        browser.close()

    if newly_broken_alerts:
        msg = "⚠️ Link Health Hub 360 (Playwright BFS): Newly broken links\n" + "\n".join(newly_broken_alerts[:20])
        if len(newly_broken_alerts) > 20:
            msg += f"\n… and {len(newly_broken_alerts)-20} more."
        slack_notify(msg)

    print(f"Done. Pages crawled={pages_crawled}", flush=True)


if __name__ == "__main__":
    main()
