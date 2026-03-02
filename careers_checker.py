#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
careers_checker.py

Checks job listings on https://careers.marble.studio/jobs (or any careers URL).
Uses Playwright to render the page (handles JS-based career platforms like Ashby,
Lever, Greenhouse, Workable, etc.) then checks each job link.

Alerts via Slack if:
  - Any job URL returns a broken response (404 / 5xx / network error)
  - The number of open positions changed from last run (if Notion tracking enabled)

Optionally writes job status to a Notion database for tracking over time.

ENV vars:
  Required:
    CAREERS_URL            - e.g. https://careers.marble.studio/jobs
    SLACK_WEBHOOK_URL      - Slack Incoming Webhook for alerts

  Optional:
    NOTION_TOKEN           - for writing job status to Notion
    NOTION_CAREERS_DB_ID   - Notion DB to track jobs (if empty, Notion write is skipped)
    CHECK_TIMEOUT          - seconds per HTTP check (default 15)
    JOBS_PAGE_LOAD_WAIT    - ms to wait for JS to render jobs (default 4000)
    SLACK_ALWAYS_NOTIFY    - "true" = always send summary, even if all OK (default false)
"""

import os
import re
import time
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import urlparse, urljoin

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# =============================================================================
# ENV
# =============================================================================

CAREERS_URL = os.environ.get("CAREERS_URL", "https://careers.marble.studio/jobs").strip()
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
NOTION_CAREERS_DB_ID = os.environ.get("NOTION_CAREERS_DB_ID", "").strip()
CHECK_TIMEOUT = int(os.environ.get("CHECK_TIMEOUT", "15"))
JOBS_PAGE_LOAD_WAIT = int(os.environ.get("JOBS_PAGE_LOAD_WAIT", "4000"))  # ms
SLACK_ALWAYS_NOTIFY = os.environ.get("SLACK_ALWAYS_NOTIFY", "false").lower() == "true"

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121 Safari/537.36"
)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

# =============================================================================
# Helpers
# =============================================================================


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def domain_of(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""


def normalize_url(base: str, href: str) -> str:
    if not href:
        return ""
    try:
        return urljoin(base, href.strip())
    except Exception:
        return ""


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def slack_notify(text: str):
    if not SLACK_WEBHOOK_URL:
        print(f"[SLACK SKIP] no webhook configured. Message:\n{text}", flush=True)
        return
    try:
        r = SESSION.post(SLACK_WEBHOOK_URL, json={"text": text, "mrkdwn": True}, timeout=10)
        print(f"[SLACK] {r.status_code}", flush=True)
    except Exception as e:
        print(f"[SLACK ERROR] {e}", flush=True)


# =============================================================================
# Playwright: extract jobs from careers page
# =============================================================================

# Common selectors used by various career platforms (Ashby, Lever, Greenhouse,
# Workable, custom pages, etc.) in order of specificity
JOB_LINK_SELECTORS = [
    # Ashby
    "a[href*='/jobs/']",
    "a[href*='/job/']",
    "a[href*='/opening/']",
    # Lever
    "a[href*='/l/']",
    "a.posting-title",
    # Greenhouse
    "a.job-post",
    ".opening a",
    # Workable
    "li.job a",
    "article.job a",
    # Generic job card links
    ".job-listing a",
    ".job-card a",
    ".job-item a",
    ".position a",
    "[data-job] a",
    "[data-testid*='job'] a",
    # Very generic fallback
    "main a[href]",
]


def extract_job_links_playwright(page, careers_url: str) -> List[Dict[str, str]]:
    """
    Render the careers page with Playwright and extract job links.
    Returns list of {"title": str, "url": str, "department": str}.
    """
    base_domain = domain_of(careers_url)
    found: Dict[str, Dict] = {}  # url -> {title, department}

    for selector in JOB_LINK_SELECTORS:
        try:
            els = page.locator(selector)
            count = els.count()
            if count == 0:
                continue
            for i in range(min(count, 300)):
                try:
                    el = els.nth(i)
                    href = el.get_attribute("href") or ""
                    if not href or href.startswith(("mailto:", "tel:", "#", "javascript:")):
                        continue
                    abs_url = normalize_url(careers_url, href)
                    if not abs_url:
                        continue
                    # Only follow links that look like job postings (same domain or known platform)
                    el_domain = domain_of(abs_url)
                    if el_domain not in (base_domain, "") and not _is_known_job_platform(abs_url):
                        continue
                    # Skip if URL is just the careers listing page itself
                    if abs_url.rstrip("/") == careers_url.rstrip("/"):
                        continue
                    if abs_url not in found:
                        title = _extract_job_title(el)
                        department = _extract_job_department(el)
                        found[abs_url] = {"title": title, "url": abs_url, "department": department}
                except Exception:
                    continue
            # If we found a good number with this selector, don't keep expanding
            if len(found) >= 3:
                break
        except Exception:
            continue

    return list(found.values())


def _is_known_job_platform(url: str) -> bool:
    """Return True if URL points to a known career hosting platform."""
    known = (
        "ashbyhq.com", "lever.co", "greenhouse.io", "workable.com",
        "recruitee.com", "smartrecruiters.com", "jobvite.com",
        "myworkdayjobs.com", "icims.com", "taleo.net",
    )
    d = domain_of(url)
    return any(d.endswith(k) for k in known)


def _extract_job_title(el) -> str:
    """Extract job title from an <a> element or its children."""
    try:
        # Try specific title elements first
        for sel in ["h2", "h3", "h4", ".title", ".job-title", "[class*='title']", "strong"]:
            try:
                child = el.locator(sel).first
                t = child.inner_text().strip()
                if t and len(t) > 2:
                    return t[:200]
            except Exception:
                pass
        # Fallback to link text
        t = el.inner_text().strip()
        return t[:200] if t else "Unknown Position"
    except Exception:
        return "Unknown Position"


def _extract_job_department(el) -> str:
    """Try to extract department from job card."""
    try:
        for sel in [".department", ".team", ".category", "[class*='department']", "[class*='team']", "span"]:
            try:
                child = el.locator(sel).first
                t = child.inner_text().strip()
                if t and len(t) > 1 and len(t) < 60:
                    return t
            except Exception:
                pass
        # Try parent container
        parent = el.locator("xpath=..").first
        for sel in [".department", ".team", "span"]:
            try:
                child = parent.locator(sel).first
                t = child.inner_text().strip()
                if t and len(t) < 60:
                    return t
            except Exception:
                pass
    except Exception:
        pass
    return ""


# =============================================================================
# HTTP check for individual job URLs
# =============================================================================


def check_job_url(url: str) -> Tuple[Optional[int], Optional[str], str]:
    """
    Returns (http_code, error_msg, result) where result is "Active" | "Broken" | "Blocked".
    Uses HEAD first, falls back to GET.
    """
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    # HEAD
    try:
        r = SESSION.head(url, headers=headers, allow_redirects=True, timeout=CHECK_TIMEOUT)
        code = r.status_code
        r.close()
        if 200 <= code < 400:
            return code, None, "Active"
        if code in (404, 410):
            return code, None, "Broken"
        if code in (401, 403, 429, 999):
            return code, None, "Blocked"
    except requests.RequestException as e:
        pass  # fall through to GET

    # GET (some servers don't support HEAD)
    try:
        r = SESSION.get(url, headers=headers, allow_redirects=True, timeout=CHECK_TIMEOUT, stream=True)
        code = r.status_code
        r.close()
        if 200 <= code < 400:
            return code, None, "Active"
        if code in (404, 410):
            return code, None, "Broken"
        if code in (401, 403, 429, 999):
            return code, None, "Blocked"
        if code >= 500:
            return code, f"server_error_{code}", "Broken"
        return code, f"unexpected_{code}", "Broken"
    except requests.ConnectionError as e:
        return None, f"ConnectionError: {str(e)[:100]}", "Broken"
    except requests.Timeout:
        return None, "Timeout", "Broken"
    except requests.RequestException as e:
        return None, type(e).__name__, "Broken"


# =============================================================================
# Playwright check for Blocked jobs (anti-bot)
# =============================================================================


def check_job_playwright(pw_page, url: str) -> Tuple[Optional[int], Optional[str], str]:
    """Use real browser for jobs blocked by anti-bot."""
    try:
        response = pw_page.goto(url, wait_until="domcontentloaded", timeout=20000)
        if response:
            code = response.status
            if 200 <= code < 400:
                return code, None, "Active"
            if code in (404, 410):
                return code, None, "Broken"
            return code, f"http_{code}", "Blocked"
        return None, "no_response", "Broken"
    except PlaywrightTimeout:
        return None, "playwright_timeout", "Broken"
    except Exception as e:
        return None, type(e).__name__, "Broken"


# =============================================================================
# Notion helpers (optional — only used if NOTION_CAREERS_DB_ID is set)
# =============================================================================

_last_notion_call = 0.0


def _notion_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _notion_request(method: str, path: str, payload=None) -> Dict:
    global _last_notion_call
    now = time.time()
    delta = now - _last_notion_call
    if delta < 0.5:
        time.sleep(0.5 - delta)
    _last_notion_call = time.time()

    url = f"{NOTION_API}{path}"
    headers = _notion_headers()
    for attempt in range(1, 5):
        try:
            if method == "POST":
                r = SESSION.post(url, headers=headers, json=payload or {}, timeout=15)
            elif method == "PATCH":
                r = SESSION.patch(url, headers=headers, json=payload or {}, timeout=15)
            elif method == "GET":
                r = SESSION.get(url, headers=headers, timeout=15)
            else:
                raise ValueError(f"Unknown method: {method}")
        except requests.RequestException as e:
            if attempt < 4:
                time.sleep(2 ** attempt)
                continue
            raise
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 2 ** attempt)))
            continue
        if r.status_code >= 500 and attempt < 4:
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"Notion {method} {path} failed")


def _set_rich_text(v: str) -> Dict:
    v = (v or "")[:2000]
    return {"rich_text": [{"type": "text", "text": {"content": v}}]} if v else {"rich_text": []}


def _set_title(v: str) -> Dict:
    v = (v or "")[:2000]
    return {"title": [{"type": "text", "text": {"content": v}}]} if v else {"title": []}


def _set_select(v: str) -> Dict:
    v = (v or "").strip()
    return {"select": {"name": v}} if v else {"select": None}


def _set_url(v: Optional[str]) -> Dict:
    vv = (v or "").strip()
    return {"url": vv if vv else None}


def _set_date(s: str) -> Dict:
    return {"date": {"start": s}}


def _set_number(n: Optional[int]) -> Dict:
    return {"number": n if n is not None else None}


def _query_db(db_id: str) -> List[Dict]:
    results = []
    cursor = None
    while True:
        payload: Dict = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        data = _notion_request("POST", f"/databases/{db_id}/query", payload)
        results.extend(data.get("results", []))
        if data.get("has_more"):
            cursor = data.get("next_cursor")
        else:
            break
    return results


def _get_rich_text(props: Dict, name: str) -> str:
    try:
        arr = props[name]["rich_text"]
        return "".join(x.get("plain_text", "") for x in arr).strip() if arr else ""
    except Exception:
        return ""


def _get_select(props: Dict, name: str) -> str:
    try:
        sel = props[name].get("select")
        return (sel.get("name") or "").strip() if sel else ""
    except Exception:
        return ""


def _get_url_prop(props: Dict, name: str) -> str:
    try:
        return (props[name].get("url") or "").strip()
    except Exception:
        return ""


def upsert_notion_job(
    db_id: str,
    job_url: str,
    job_title: str,
    department: str,
    result: str,
    http_code: Optional[int],
    error: str,
) -> bool:
    """
    Upsert a job entry in Notion. Returns True if this is a newly broken job.
    The Notion DB should have: Name (title), URL (url), Status (select),
    Department (rich_text), HTTP Code (number), Error (rich_text), Last Checked (date).
    """
    try:
        schema = _notion_request("GET", f"/databases/{db_id}")
    except Exception as e:
        print(f"[NOTION] Cannot read DB schema: {e}", flush=True)
        return False

    # Find title property
    title_prop = "Name"
    for k, v in schema.get("properties", {}).items():
        if v.get("type") == "title":
            title_prop = k
            break

    key = sha1(job_url)
    rows = _query_db(db_id)
    existing_page = None
    old_result = ""
    for row in rows:
        props = row.get("properties", {})
        u = _get_url_prop(props, "URL")
        if u and sha1(u) == key:
            existing_page = row
            old_result = _get_select(props, "Status")
            break

    props_payload = {
        title_prop: _set_title(job_title or job_url),
        "URL": _set_url(job_url),
        "Status": _set_select(result),
        "Department": _set_rich_text(department),
        "HTTP Code": _set_number(http_code),
        "Error": _set_rich_text(error),
        "Last Checked": _set_date(iso_now()),
    }

    newly_broken = False
    if existing_page:
        if old_result != "Broken" and result == "Broken":
            newly_broken = True
        _notion_request("PATCH", f"/pages/{existing_page['id']}", {"properties": props_payload})
    else:
        _notion_request("POST", "/pages", {
            "parent": {"database_id": db_id},
            "properties": props_payload,
        })
        if result == "Broken":
            newly_broken = True

    return newly_broken


# =============================================================================
# Main
# =============================================================================


def main():
    print(f"[careers_checker] URL: {CAREERS_URL}", flush=True)
    print(f"[careers_checker] Notion tracking: {'enabled' if NOTION_CAREERS_DB_ID and NOTION_TOKEN else 'disabled'}", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=USER_AGENT,
        )
        page = context.new_page()
        check_page = context.new_page()

        # ----------------------------------------------------------------
        # Step 1: load careers page and extract job listings
        # ----------------------------------------------------------------
        print(f"[careers_checker] Loading {CAREERS_URL}...", flush=True)
        try:
            page.goto(CAREERS_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(JOBS_PAGE_LOAD_WAIT)  # let JS render jobs
            page_title = page.title() or CAREERS_URL
        except PlaywrightTimeout:
            print("[careers_checker] Timeout loading careers page", flush=True)
            slack_notify(f"⚠️ *Careers Checker*: timeout loading <{CAREERS_URL}|careers page>")
            browser.close()
            return
        except Exception as e:
            print(f"[careers_checker] Error loading careers page: {e}", flush=True)
            slack_notify(f"⚠️ *Careers Checker*: error loading <{CAREERS_URL}|careers page> — `{e}`")
            browser.close()
            return

        jobs = extract_job_links_playwright(page, CAREERS_URL)
        print(f"[careers_checker] Found {len(jobs)} job listings", flush=True)
        for j in jobs:
            print(f"  • {j['title']} [{j.get('department','—')}] → {j['url']}", flush=True)

        if not jobs:
            msg = (
                f"⚠️ *Careers Checker*: no job listings found on <{CAREERS_URL}|careers page>.\n"
                "This may mean the page is empty, the selectors don't match the platform, "
                "or the page didn't render in time."
            )
            print("[careers_checker] No jobs found — sending alert", flush=True)
            slack_notify(msg)
            browser.close()
            return

        # ----------------------------------------------------------------
        # Step 2: check each job URL
        # ----------------------------------------------------------------
        results: List[Dict] = []
        broken_jobs: List[Dict] = []
        newly_broken_jobs: List[Dict] = []

        for job in jobs:
            url = job["url"]
            print(f"[careers_checker] Checking: {url}", flush=True)

            code, err, result = check_job_url(url)

            # For Blocked → try Playwright (anti-bot sites)
            if result == "Blocked":
                print(f"  → Blocked via HTTP, trying Playwright...", flush=True)
                code, err, result = check_job_playwright(check_page, url)

            job_data = {
                "title": job["title"],
                "url": url,
                "department": job.get("department", ""),
                "result": result,
                "http_code": code,
                "error": err or "",
            }
            results.append(job_data)
            print(f"  → {result} ({code})", flush=True)

            if result == "Broken":
                broken_jobs.append(job_data)

            # Write to Notion if configured
            if NOTION_CAREERS_DB_ID and NOTION_TOKEN:
                try:
                    is_new_broken = upsert_notion_job(
                        db_id=NOTION_CAREERS_DB_ID,
                        job_url=url,
                        job_title=job["title"],
                        department=job.get("department", ""),
                        result=result,
                        http_code=code,
                        error=err or "",
                    )
                    if is_new_broken:
                        newly_broken_jobs.append(job_data)
                except Exception as e:
                    print(f"  [NOTION ERROR] {e}", flush=True)
            else:
                # Without Notion we can't track history, treat all broken as "newly broken"
                if result == "Broken":
                    newly_broken_jobs.append(job_data)

        browser.close()

        # ----------------------------------------------------------------
        # Step 3: Slack notification
        # ----------------------------------------------------------------
        total = len(results)
        active = sum(1 for r in results if r["result"] == "Active")
        broken = sum(1 for r in results if r["result"] == "Broken")
        blocked = sum(1 for r in results if r["result"] == "Blocked")

        print(
            f"\n[careers_checker] Summary: {total} jobs | {active} active | {broken} broken | {blocked} blocked",
            flush=True,
        )

        if newly_broken_jobs:
            lines = [f"🚨 *Careers Checker*: {len(newly_broken_jobs)} job posting(s) are now broken:"]
            for j in newly_broken_jobs:
                dept_str = f" [{j['department']}]" if j.get("department") else ""
                lines.append(f"  • <{j['url']}|{j['title']}>{dept_str} → {j['result']} ({j['http_code'] or 'no response'})")
            lines.append(f"\n_Open positions page: <{CAREERS_URL}|careers.marble.studio/jobs>_")
            slack_notify("\n".join(lines))

        elif SLACK_ALWAYS_NOTIFY:
            lines = [f"✅ *Careers Checker*: {active}/{total} job postings are active"]
            if blocked:
                lines.append(f"  ⚠️ {blocked} job(s) could not be verified (blocked by anti-bot)")
            lines.append(f"_<{CAREERS_URL}|careers.marble.studio/jobs>_")
            slack_notify("\n".join(lines))

        elif broken == 0:
            print(f"[careers_checker] All {total} jobs OK — no Slack alert (set SLACK_ALWAYS_NOTIFY=true to always notify)", flush=True)

        print("[careers_checker] Done.", flush=True)


if __name__ == "__main__":
    main()
