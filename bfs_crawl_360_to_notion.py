#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bfs_crawl_360_to_notion.py

DB A = "Link Health Hub 360"
- Primary URL (url)
- Status (select) -> Active / Broken / Need Review
- Broken Links Count (number)
- Blocked Links Count (number)
- Last Crawled (date)

DB B = "Link Occurrences"
- URL (url)
- Link Type (select) -> Internal / External
- Result (select) -> Active / Broken / Blocked
- HTTP Code (number)
- Error (rich_text)
- Last Seen (date)
- Source Content (relation -> DB A)
- Source Page URL (rich_text)
- Anchor Text (rich_text)
- Context Snippet (rich_text)
- Breadcrumb Trail (rich_text)
- DOM Area (select)
- Deep Link (url)
- Locator CSS (rich_text)
- Pages (select)

TRIPLE CHECK (senza leggere HTML testo):
- HEAD
- GET headers-only
- Notion oracle via getPublicPageData (solo notion.site / notion.so)
  Active SOLO se oracle dice public. Se inconclusivo => Blocked.

LIMITI:
- LIMIT_MODE=pages -> MAX_PAGES
- LIMIT_MODE=total -> MAX_TOTAL (totale check unici, cache)
"""

import os
import re
import time
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Any, Set
from collections import deque
from urllib.parse import urlparse, urljoin

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


# =============================================================================
# ENV
# =============================================================================

NOTION_TOKEN = os.environ["NOTION_TOKEN"].strip()
DB_A_ID = os.environ["NOTION_DB_A_ID"].strip()
DB_B_ID = os.environ["NOTION_DB_B_ID"].strip()
SITE_BASE_URL = os.environ["SITE_BASE_URL"].strip()

LIMIT_MODE = os.environ.get("LIMIT_MODE", "pages").strip().lower()  # pages|total
MAX_PAGES = int(os.environ.get("MAX_PAGES", "120"))
MAX_TOTAL = int(os.environ.get("MAX_TOTAL", "2000"))

CHECK_EXTERNAL = os.environ.get("CHECK_EXTERNAL", "true").lower() == "true"
CHECK_INTERNAL = os.environ.get("CHECK_INTERNAL", "true").lower() == "true"

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()

NOTION_MIN_INTERVAL = float(os.environ.get("NOTION_MIN_INTERVAL", "1.0"))
CRAWL_SLEEP = float(os.environ.get("CRAWL_SLEEP", "0.35"))

BACKFILL_MISSING = os.environ.get("BACKFILL_MISSING", "true").lower() == "true"
FORCE_TOUCH_EXISTING = os.environ.get("FORCE_TOUCH_EXISTING", "true").lower() == "true"

SKIP_DOMAINS = os.environ.get("SKIP_DOMAINS", "").strip()
EXCLUDE_DOM_AREAS = os.environ.get("EXCLUDE_DOM_AREAS", "").strip()

TIMEOUT = int(os.environ.get("TIMEOUT", "12"))

USER_AGENT = os.environ.get(
    "USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36",
)

SKIP_DOMAINS_SET: Set[str] = set(d.strip().lower() for d in SKIP_DOMAINS.split(",") if d.strip()) if SKIP_DOMAINS else set()
EXCLUDE_DOM_AREAS_SET: Set[str] = set(d.strip() for d in EXCLUDE_DOM_AREAS.split(",") if d.strip()) if EXCLUDE_DOM_AREAS else set()

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


# =============================================================================
# URL helpers + filters
# =============================================================================

ASSET_EXTS = (
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg",
    ".css", ".js", ".ico", ".pdf",
    ".zip", ".rar", ".7z",
    ".mp4", ".mp3", ".mov",
    ".woff", ".woff2", ".ttf", ".otf",
)


def strip_trailing_slash(u: str) -> str:
    return u[:-1] if u.endswith("/") and len(u) > 8 else u


def drop_query(u: str) -> str:
    try:
        p = urlparse(u)
        return p._replace(query="", fragment="").geturl()
    except Exception:
        return u


def domain_of(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""


def same_domain(u: str, domain: str) -> bool:
    try:
        return urlparse(u).netloc.lower().endswith(domain.lower())
    except Exception:
        return False


def has_skipped_extension(u: str) -> bool:
    lu = u.lower()
    return any(lu.endswith(ext) for ext in ASSET_EXTS)


def should_ignore_url(u: str) -> bool:
    if not u:
        return True
    lu = u.strip().lower()
    return lu.startswith(("mailto:", "tel:", "javascript:"))


def is_skipped_domain(d: str) -> bool:
    if not d:
        return False
    dd = d.lower()
    if dd.startswith("www."):
        dd = dd[4:]
    for sd in SKIP_DOMAINS_SET:
        s = sd
        if s.startswith("www."):
            s = s[4:]
        if dd == s or dd.endswith("." + s):
            return True
    return False


def normalize_url(base: str, href: str) -> str:
    if not href:
        return ""
    try:
        return urljoin(base, href.strip())
    except Exception:
        return ""


# =============================================================================
# Notion API helpers (with retry on 429 / 5xx)
# =============================================================================

_last_notion_call = 0.0


def _notion_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _notion_rate_limit_sleep():
    global _last_notion_call
    now = time.time()
    delta = now - _last_notion_call
    if delta < NOTION_MIN_INTERVAL:
        time.sleep(NOTION_MIN_INTERVAL - delta)
    _last_notion_call = time.time()


def _notion_request(method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Generic Notion API call with automatic retry on 429 (rate-limit) and 5xx (server errors).
    Uses exponential backoff with up to 5 attempts.
    """
    url = f"{NOTION_API}{path}"
    headers = _notion_headers()
    max_attempts = 5

    for attempt in range(1, max_attempts + 1):
        _notion_rate_limit_sleep()
        try:
            if method == "POST":
                r = SESSION.post(url, headers=headers, json=payload or {}, timeout=TIMEOUT)
            elif method == "PATCH":
                r = SESSION.patch(url, headers=headers, json=payload or {}, timeout=TIMEOUT)
            elif method == "GET":
                r = SESSION.get(url, headers=headers, timeout=TIMEOUT)
            else:
                raise ValueError(f"Unknown method: {method}")
        except requests.RequestException as e:
            print(f"Notion {method} network error (attempt {attempt}/{max_attempts}): {e}", flush=True)
            if attempt < max_attempts:
                time.sleep(2 ** attempt)
                continue
            raise

        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", 2 ** attempt))
            print(f"Notion 429 rate-limit. Waiting {retry_after}s (attempt {attempt}/{max_attempts})", flush=True)
            time.sleep(retry_after)
            continue

        if r.status_code >= 500:
            print(f"Notion {r.status_code} server error (attempt {attempt}/{max_attempts}): {r.text[:200]}", flush=True)
            if attempt < max_attempts:
                time.sleep(2 ** attempt)
                continue

        if not r.ok:
            print(f"NOTION {method} ERROR {r.status_code}: {r.text[:400]}", flush=True)
            r.raise_for_status()

        return r.json()

    raise RuntimeError(f"Notion {method} {path} failed after {max_attempts} attempts")


def notion_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return _notion_request("POST", path, payload)


def notion_patch(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return _notion_request("PATCH", path, payload)


def notion_get(path: str) -> Dict[str, Any]:
    return _notion_request("GET", path)


def fetch_db_schema(db_id: str) -> Dict[str, Any]:
    return notion_get(f"/databases/{db_id}")


def query_db_all(db_id: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    cursor = None
    while True:
        payload: Dict[str, Any] = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        data = notion_post(f"/databases/{db_id}/query", payload)
        results.extend(data.get("results", []))
        if data.get("has_more"):
            cursor = data.get("next_cursor")
        else:
            break
    return results


def infer_title_prop(schema: Dict[str, Any]) -> str:
    props = schema.get("properties", {})
    for k, v in props.items():
        if v.get("type") == "title":
            return k
    raise RuntimeError("No title property found in Notion DB schema")


def get_rich_text(props: Dict[str, Any], name: str) -> str:
    try:
        arr = props[name]["rich_text"]
        return "".join(x.get("plain_text", "") for x in arr).strip() if arr else ""
    except Exception:
        return ""


def get_url_prop(props: Dict[str, Any], name: str) -> str:
    try:
        return (props[name].get("url") or "").strip()
    except Exception:
        return ""


def get_select(props: Dict[str, Any], name: str) -> str:
    try:
        sel = props[name].get("select")
        return (sel.get("name") or "").strip() if sel else ""
    except Exception:
        return ""


def set_rich_text(v: str) -> Dict[str, Any]:
    v = v or ""
    return {"rich_text": [{"type": "text", "text": {"content": v}}]} if v else {"rich_text": []}


def set_title(v: str) -> Dict[str, Any]:
    v = v or ""
    return {"title": [{"type": "text", "text": {"content": v}}]} if v else {"title": []}


def set_url(v: Optional[str]) -> Dict[str, Any]:
    vv = (v or "").strip()
    return {"url": vv if vv else None}


def set_select(v: str) -> Dict[str, Any]:
    vv = (v or "").strip()
    return {"select": {"name": vv}} if vv else {"select": None}


def set_number(n: Optional[int]) -> Dict[str, Any]:
    return {"number": n if n is not None else None}


def set_date_now() -> Dict[str, Any]:
    # FIX: use UTC so timestamps in Notion are always consistent regardless of runner timezone
    return {"date": {"start": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")}}


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def slack_notify(text: str):
    if not SLACK_WEBHOOK_URL:
        return
    try:
        SESSION.post(SLACK_WEBHOOK_URL, json={"text": text, "mrkdwn": True}, timeout=TIMEOUT)
    except Exception:
        pass


def build_db_a_index() -> Tuple[Dict[str, str], Dict[str, Any], str]:
    schema = fetch_db_schema(DB_A_ID)
    title_prop = infer_title_prop(schema)
    rows = query_db_all(DB_A_ID)
    idx: Dict[str, str] = {}
    for r in rows:
        props = r.get("properties", {})
        u = get_url_prop(props, "Primary URL")
        if u:
            idx[strip_trailing_slash(drop_query(u))] = r["id"]
    return idx, schema, title_prop


def build_db_b_index() -> Tuple[Dict[str, Tuple[str, str]], Dict[str, Any], str]:
    """
    FIX: index now stores (page_id, old_result) instead of just page_id.
    This eliminates one notion_get() per link inside upsert_db_b.
    """
    schema = fetch_db_schema(DB_B_ID)
    title_prop = infer_title_prop(schema)
    rows = query_db_all(DB_B_ID)
    idx: Dict[str, Tuple[str, str]] = {}
    for r in rows:
        props = r.get("properties", {})
        src = ""
        try:
            rel = props.get("Source Content", {}).get("relation") or props.get("Source Page", {}).get("relation") or []
            if rel:
                src = rel[0].get("id", "")
        except Exception:
            src = ""
        u = get_url_prop(props, "URL")
        a = get_rich_text(props, "Anchor Text")
        dom = get_select(props, "DOM Area")
        loc = get_rich_text(props, "Locator CSS")
        result = get_select(props, "Result")  # FIX: read existing result
        key = sha1("|".join([src, strip_trailing_slash(drop_query(u)), a, dom, loc]))
        idx[key] = (r["id"], result)
    return idx, schema, title_prop


# =============================================================================
# Upserts
# =============================================================================

def get_or_create_db_a(
    db_a_title_prop: str,
    db_a_index: Dict[str, str],
    page_url: str,
    title: str,
) -> str:
    """
    FIX: lightweight first call — only creates the row if missing, returns page_id.
    Does NOT patch existing rows (avoids writing 0 counts before the real upsert).
    """
    url_key = strip_trailing_slash(drop_query(page_url))

    if url_key in db_a_index:
        return db_a_index[url_key]

    # Create with placeholder counts; real counts written by upsert_db_a after the loop
    props_payload = {
        db_a_title_prop: set_title(title or url_key),
        "Primary URL": set_url(url_key),
        "Status": set_select("Active"),
        "Broken Links Count": set_number(0),
        "Blocked Links Count": set_number(0),
        "Last Crawled": set_date_now(),
    }
    data = notion_post("/pages", {"parent": {"database_id": DB_A_ID}, "properties": props_payload})
    page_id = data["id"]
    db_a_index[url_key] = page_id
    return page_id


def upsert_db_a(
    db_a_title_prop: str,
    db_a_index: Dict[str, str],
    page_url: str,
    title: str,
    page_alive: bool,
    broken_count: int,
    blocked_count: int,
) -> str:
    url_key = strip_trailing_slash(drop_query(page_url))
    now_prop = set_date_now()

    status_val = "Active"
    if not page_alive:
        status_val = "Broken"
    elif broken_count > 0:
        status_val = "Need Review"

    props_payload = {
        db_a_title_prop: set_title(title or url_key),
        "Primary URL": set_url(url_key),
        "Status": set_select(status_val),
        "Broken Links Count": set_number(broken_count),
        "Blocked Links Count": set_number(blocked_count),
        "Last Crawled": now_prop,
    }

    if url_key in db_a_index:
        page_id = db_a_index[url_key]
        if FORCE_TOUCH_EXISTING or BACKFILL_MISSING:
            notion_patch(f"/pages/{page_id}", {"properties": props_payload})
        return page_id

    data = notion_post("/pages", {"parent": {"database_id": DB_A_ID}, "properties": props_payload})
    page_id = data["id"]
    db_a_index[url_key] = page_id
    return page_id


def upsert_db_b(
    db_b_title_prop: str,
    db_b_index: Dict[str, Tuple[str, str]],
    source_page_id: str,
    source_page_url: str,
    link_url: str,
    link_type_val: str,
    result_val: str,
    http_code: Optional[int],
    error: str,
    anchor_text: str,
    snippet: str,
    breadcrumb: str,
    dom_area: str,
    deep_link: Optional[str],
    locator_css: str,
    pages_val_for_b: str,
) -> bool:
    url_key = strip_trailing_slash(drop_query(link_url))
    now_prop = set_date_now()

    title_val = f"{link_type_val}: {url_key}"
    deep_link = (deep_link or "").strip() or None

    key = sha1("|".join([source_page_id, url_key, anchor_text, dom_area, locator_css]))

    props_payload = {
        db_b_title_prop: set_title(title_val),
        "URL": set_url(url_key),
        "Link Type": set_select(link_type_val),
        "Result": set_select(result_val),
        "HTTP Code": set_number(http_code),
        "Error": set_rich_text(error or ""),
        "Last Seen": now_prop,
        "Source Content": {"relation": [{"id": source_page_id}]},
        "Source Page URL": set_rich_text(strip_trailing_slash(drop_query(source_page_url))),
        "Anchor Text": set_rich_text(anchor_text),
        "Context Snippet": set_rich_text(snippet),
        "Breadcrumb Trail": set_rich_text(breadcrumb),
        "DOM Area": set_select(dom_area),
        "Deep Link": set_url(deep_link),
        "Locator CSS": set_rich_text(locator_css),
        "Pages": set_select(pages_val_for_b),
    }

    newly_broken = False

    if key in db_b_index:
        page_id, old_result = db_b_index[key]  # FIX: read old result from index, no extra notion_get
        if FORCE_TOUCH_EXISTING or BACKFILL_MISSING:
            if old_result != "Broken" and result_val == "Broken":
                newly_broken = True
            notion_patch(f"/pages/{page_id}", {"properties": props_payload})
            db_b_index[key] = (page_id, result_val)  # update cached result
        return newly_broken

    data = notion_post("/pages", {"parent": {"database_id": DB_B_ID}, "properties": props_payload})
    page_id = data["id"]
    db_b_index[key] = (page_id, result_val)
    if result_val == "Broken":
        newly_broken = True
    return newly_broken


# =============================================================================
# Page metadata helpers
# =============================================================================

def classify_page_group(url: str) -> str:
    p = urlparse(url)
    path = (p.path or "/").strip("/").lower()
    if not path:
        return "Home"
    first = path.split("/")[0]
    mapping = {
        "about": "About",
        "pricing": "Pricing",
        "faq": "FAQ",
        "community": "Community",
        "docs": "Docs",
        "blog": "Blog",
        "company": "Company",
        "careers": "Careers",
    }
    return mapping.get(first, first.capitalize())


def breadcrumb_for(url: str, parent: Dict[str, Optional[str]]) -> str:
    trail: List[str] = []
    cur = url
    seen_local = set()
    while cur and cur not in seen_local:
        seen_local.add(cur)
        trail.append(cur)
        cur = parent.get(cur)
    trail.reverse()

    paths = []
    for u in trail:
        try:
            pu = urlparse(u)
            paths.append(pu.path or "/")
        except Exception:
            paths.append("/")
    return " > ".join(paths)


# =============================================================================
# TRIPLE CHECK: link checking without HTML parsing
# =============================================================================

def _is_notion(url: str) -> bool:
    try:
        netloc = urlparse(url).netloc.lower()
        return netloc.endswith("notion.site") or netloc.endswith("notion.so") or netloc in ("www.notion.so", "notion.so")
    except Exception:
        return False


def _extract_notion_block_id(url: str) -> Optional[str]:
    try:
        clean = re.sub(r"[^0-9a-fA-F]", "", url)
        m = re.search(r"([0-9a-fA-F]{32})", clean)
        if not m:
            return None
        raw = m.group(1).lower()
        return f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"
    except Exception:
        return None


def _probe_head(url: str) -> Tuple[Optional[int], Optional[str]]:
    try:
        r = SESSION.head(
            url,
            allow_redirects=True,
            timeout=TIMEOUT,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
        )
        code = r.status_code
        r.close()
        return code, None
    except requests.RequestException as e:
        return None, type(e).__name__


def _probe_get_headers_only(url: str, user_agent: Optional[str] = None) -> Tuple[Optional[int], Optional[str]]:
    try:
        r = SESSION.get(
            url,
            allow_redirects=True,
            timeout=TIMEOUT,
            stream=True,
            headers={
                "User-Agent": user_agent or USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
        )
        code = r.status_code
        r.close()
        return code, None
    except requests.RequestException as e:
        return None, type(e).__name__


def _notion_oracle(block_id: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        resp = SESSION.post(
            "https://www.notion.so/api/v3/getPublicPageData",
            json={"blockId": block_id},
            headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
            timeout=TIMEOUT,
        )

        try:
            data = resp.json() if resp.content else {}
        except Exception:
            data = {}

        if resp.ok:
            role = (data or {}).get("publicAccessRole")
            if role in (None, "", "none"):
                return "private", "notion_publicAccessRole_none"
            return "public", None

        sc = resp.status_code
        payload_str = ""
        try:
            payload_str = str(data).lower()
        except Exception:
            payload_str = ""

        if sc in (401, 403) or "unauthorized" in payload_str or "permission" in payload_str or "not authorized" in payload_str:
            return "private", f"notion_api_{sc}_unauthorized"

        if sc in (404, 410) or "not found" in payload_str or "does not exist" in payload_str:
            return "missing", f"notion_api_{sc}_not_found"

        if sc in (429, 503):
            return None, f"notion_api_{sc}_rate_or_temp"

        return None, f"notion_api_{sc}_unknown"

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


def check_url(url: str) -> Tuple[Optional[int], Optional[str]]:
    is_notion = _is_notion(url)

    h_code, h_err = _probe_head(url)
    if h_code in (404, 410):
        return h_code, h_err
    if h_code in (401, 403, 429, 999):
        return h_code, h_err

    g_code, g_err = _probe_get_headers_only(url)
    if g_code in (404, 410):
        return g_code, g_err
    if g_code in (401, 403, 429, 999):
        return g_code, g_err

    if g_code is None:
        if h_code is not None:
            return h_code, g_err or h_err
        return None, g_err or h_err

    if not is_notion:
        return g_code, g_err

    block_id = _extract_notion_block_id(url)
    if not block_id:
        if 200 <= g_code < 400:
            return 401, "notion_no_block_id_oracle_unavailable"
        return g_code, g_err

    verdict, reason = _notion_oracle(block_id)
    if verdict == "public":
        return g_code, None
    if verdict == "private":
        return 401, reason or "notion_private"
    if verdict == "missing":
        return 404, reason or "notion_missing"

    return 401, reason or "notion_oracle_inconclusive"


def double_check_broken(url: str, c1: Optional[int], e1: Optional[str]) -> Tuple[Optional[int], Optional[str], str]:
    r1 = classify(c1)
    if r1 != "Broken":
        return c1, e1, r1

    time.sleep(0.8)
    browser_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36"
    g2, e2 = _probe_get_headers_only(url, user_agent=browser_ua)
    r2 = classify(g2)
    if r2 != "Broken":
        if _is_notion(url):
            c3, e3 = check_url(url)
            return c3, e3, classify(c3)
        return g2, e2, r2

    c3, e3 = check_url(url)
    r3 = classify(c3)
    if r3 != "Broken":
        return c3, e3, r3

    return c1, e1, r1


def check_page_alive(url: str) -> Tuple[bool, Optional[int], Optional[str]]:
    code, err = check_url(url)
    return (code is not None and 200 <= code < 400), code, err


# =============================================================================
# Playwright extraction
# =============================================================================

def extract_links_playwright(page) -> List[Dict[str, str]]:
    """
    FIX: deduplicate links between first pass (Main) and second pass (Accordion).
    Second pass only appends hrefs that were not already seen in the first pass.
    """
    items: List[Dict[str, str]] = []
    seen_hrefs: Set[str] = set()

    # First pass — links visible without clicking
    anchors = page.locator("a[href]")
    n = anchors.count()
    for i in range(min(n, 800)):
        try:
            a = anchors.nth(i)
            href = a.get_attribute("href") or ""
            text = (a.inner_text() or "").strip()
            items.append({"href": href, "anchor_text": text, "dom_area": "Main"})
            seen_hrefs.add(href)
        except Exception:
            continue

    # Click accordion / toggles to reveal hidden links
    try:
        toggles = page.locator("[aria-expanded='false'], button[aria-controls]")
        tcount = toggles.count()
        for i in range(min(tcount, 30)):
            try:
                toggles.nth(i).click(timeout=800)
                time.sleep(0.05)
            except Exception:
                pass
    except Exception:
        pass

    # Second pass — only add hrefs that were NOT in the first pass
    anchors2 = page.locator("a[href]")
    n2 = anchors2.count()
    for i in range(min(n2, 1000)):
        try:
            a = anchors2.nth(i)
            href = a.get_attribute("href") or ""
            if href in seen_hrefs:
                continue  # FIX: skip already-seen links, avoids duplicates
            text = (a.inner_text() or "").strip()
            items.append({"href": href, "anchor_text": text, "dom_area": "Accordion"})
            seen_hrefs.add(href)
        except Exception:
            continue

    return items


# =============================================================================
# Main
# =============================================================================

def main():
    base = strip_trailing_slash(drop_query(SITE_BASE_URL))
    domain = domain_of(base)
    if domain.startswith("www."):
        domain = domain[4:]

    db_a_index, _, db_a_title_prop = build_db_a_index()
    db_b_index, _, db_b_title_prop = build_db_b_index()

    print(f"DB A indexed: {len(db_a_index)} rows; DB B indexed: {len(db_b_index)} rows", flush=True)
    print(f"Starting Playwright BFS from {base} (LIMIT_MODE={LIMIT_MODE}, MAX_PAGES={MAX_PAGES}, MAX_TOTAL={MAX_TOTAL})", flush=True)

    queue = deque([base])
    parent: Dict[str, Optional[str]] = {base: None}
    seen: Set[str] = set()

    pages_crawled = 0
    total_checks = 0
    stop_due_to_total = False

    external_cache: Dict[str, Tuple[Optional[int], Optional[str], str]] = {}
    internal_cache: Dict[str, Tuple[Optional[int], Optional[str], str]] = {}

    newly_broken_alerts: List[Tuple[str, str, str, str]] = []  # (page_group, page_title, page_url, link_url)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1280, "height": 800})
        page = context.new_page()

        while queue and (LIMIT_MODE != "pages" or pages_crawled < MAX_PAGES) and (LIMIT_MODE != "total" or total_checks < MAX_TOTAL):
            page_url = strip_trailing_slash(drop_query(queue.popleft()))

            if page_url in seen:
                continue
            if not same_domain(page_url, domain):
                continue
            if should_ignore_url(page_url) or has_skipped_extension(page_url):
                continue

            seen.add(page_url)
            pages_crawled += 1
            breadcrumb = breadcrumb_for(page_url, parent)

            page_title = page_url
            alive = False
            try:
                page.goto(page_url, wait_until="domcontentloaded", timeout=20000)
                page_title = (page.title() or page_url).strip() or page_url
                alive, _, _ = check_page_alive(page_url)
            except PlaywrightTimeout:
                alive = False
            except Exception:
                alive = False

            links = extract_links_playwright(page)

            for it in links:
                absu = normalize_url(page_url, it.get("href", ""))
                if not absu:
                    continue
                absu = strip_trailing_slash(drop_query(absu))
                if should_ignore_url(absu) or has_skipped_extension(absu):
                    continue
                if same_domain(absu, domain):
                    if absu not in parent:
                        parent[absu] = page_url
                    if absu not in seen:
                        queue.append(absu)

            # FIX: use get_or_create to avoid writing placeholder zeros when row already exists
            page_id = get_or_create_db_a(
                db_a_title_prop=db_a_title_prop,
                db_a_index=db_a_index,
                page_url=page_url,
                title=page_title,
            )

            broken_in_page = 0
            blocked_in_page = 0
            unique_links_in_page: Set[str] = set()

            for it in links:
                link_url = normalize_url(page_url, it.get("href", ""))
                if not link_url:
                    continue
                link_url = strip_trailing_slash(drop_query(link_url))

                if should_ignore_url(link_url) or has_skipped_extension(link_url):
                    continue

                dom_area = (it.get("dom_area", "Main") or "Main").strip()
                if dom_area in EXCLUDE_DOM_AREAS_SET:
                    continue

                if link_url in unique_links_in_page:
                    continue
                unique_links_in_page.add(link_url)

                anchor_text = it.get("anchor_text", "") or ""
                snippet = ""
                locator_css = ""
                deep_link = None

                pages_val_for_b = classify_page_group(page_url)

                if same_domain(link_url, domain):
                    if not CHECK_INTERNAL:
                        continue
                    link_type_val = "Internal"
                    if link_url in internal_cache:
                        code, err, result_val = internal_cache[link_url]
                    else:
                        if LIMIT_MODE == "total" and total_checks >= MAX_TOTAL:
                            stop_due_to_total = True
                            break
                        total_checks += 1
                        c1, e1 = check_url(link_url)
                        code, err, result_val = double_check_broken(link_url, c1, e1)
                        internal_cache[link_url] = (code, err, result_val)
                        time.sleep(CRAWL_SLEEP)
                else:
                    if not CHECK_EXTERNAL:
                        continue
                    link_type_val = "External"
                    d = domain_of(link_url)
                    if is_skipped_domain(d):
                        code, err, result_val = None, "skipped_domain", "Blocked"
                    else:
                        if link_url in external_cache:
                            code, err, result_val = external_cache[link_url]
                        else:
                            if LIMIT_MODE == "total" and total_checks >= MAX_TOTAL:
                                stop_due_to_total = True
                                break
                            total_checks += 1
                            c1, e1 = check_url(link_url)
                            code, err, result_val = double_check_broken(link_url, c1, e1)
                            external_cache[link_url] = (code, err, result_val)
                            time.sleep(CRAWL_SLEEP)

                if result_val == "Broken":
                    broken_in_page += 1
                if result_val == "Blocked":
                    blocked_in_page += 1

                newly_broken = upsert_db_b(
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
                    dom_area=dom_area,
                    deep_link=deep_link,
                    locator_css=locator_css,
                    pages_val_for_b=pages_val_for_b,
                )

                if newly_broken:
                    pg = classify_page_group(page_url)
                    newly_broken_alerts.append((pg, page_title, page_url, link_url))

            # FIX: single final upsert_db_a with real counts (no more double-write)
            upsert_db_a(
                db_a_title_prop=db_a_title_prop,
                db_a_index=db_a_index,
                page_url=page_url,
                title=page_title,
                page_alive=alive,
                broken_count=broken_in_page,
                blocked_count=blocked_in_page,
            )

            print(
                f"[pages={pages_crawled}/" + (str(MAX_PAGES) if LIMIT_MODE == "pages" else "∞")
                + f" total={total_checks}/" + (str(MAX_TOTAL) if LIMIT_MODE == "total" else "∞")
                + f"] {page_title} | alive={alive} | broken={broken_in_page} | blocked={blocked_in_page} | queue={len(queue)}",
                flush=True,
            )

            if stop_due_to_total:
                break

        browser.close()

    if stop_due_to_total:
        print(f"Stopping: reached MAX_TOTAL={MAX_TOTAL} unique link checks (pages_crawled={pages_crawled})", flush=True)

    # FIX: Slack message with proper mrkdwn formatting (consistent with check_links_notion.py)
    if newly_broken_alerts:
        n = len(newly_broken_alerts)
        noun = "link" if n == 1 else "links"
        lines = [f"⚠️ Link Health Hub 360: {n} newly broken {noun}:"]
        for pg, title, src_url, lnk_url in newly_broken_alerts[:20]:
            pg_bold = f"*{pg}*"
            src_link = f"<{src_url}|{title}>"
            broken_link = f"<{lnk_url}|Link>"
            lines.append(f"• {pg_bold} {src_link} → {broken_link}")
        if n > 20:
            lines.append(f"…and {n - 20} more.")
        slack_notify("\n".join(lines))

    print(f"Done. Pages crawled={pages_crawled} | total_checks={total_checks} | LIMIT_MODE={LIMIT_MODE}", flush=True)


if __name__ == "__main__":
    main()
