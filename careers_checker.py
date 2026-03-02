#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
careers_checker.py

Checks job listings on https://careers.marble.studio/jobs (Getro platform).
Uses Playwright to render the page, then for each job:
  1. Clicks the job link
  2. Detects the Getro interstitial popup ("You're about to go to X's website")
  3. Clicks "No thanks, take me to the application form"
  4. Verifies the final application page loads (200 OK)

Alerts via Slack if any job's application form is broken/unreachable.
Optionally tracks job status history in a Notion database.

ENV vars:
  Required:
    CAREERS_URL            - e.g. https://careers.marble.studio/jobs
    SLACK_WEBHOOK_URL      - Slack Incoming Webhook for alerts

  Optional:
    NOTION_TOKEN           - for writing job status to Notion
    NOTION_CAREERS_DB_ID   - Notion DB to track jobs (if empty, Notion write is skipped)
    CHECK_TIMEOUT          - seconds per check (default 20)
    JOBS_PAGE_LOAD_WAIT    - ms to wait for JS to render jobs list (default 4000)
    POPUP_WAIT_MS          - ms to wait for Getro popup to appear (default 3000)
    SLACK_ALWAYS_NOTIFY    - "true" = always send summary, even if all OK (default false)
"""

import os
import signal
import time
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout, Page

# =============================================================================
# ENV
# =============================================================================

CAREERS_URL = os.environ.get("CAREERS_URL", "https://careers.marble.studio/jobs").strip()
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
NOTION_CAREERS_DB_ID = os.environ.get("NOTION_CAREERS_DB_ID", "").strip()
CHECK_TIMEOUT = int(os.environ.get("CHECK_TIMEOUT", "20"))
JOBS_PAGE_LOAD_WAIT = int(os.environ.get("JOBS_PAGE_LOAD_WAIT", "4000"))   # ms
POPUP_WAIT_MS = int(os.environ.get("POPUP_WAIT_MS", "3000"))               # ms
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
        print(f"[SLACK SKIP] no webhook. Message:\n{text}", flush=True)
        return
    try:
        r = SESSION.post(SLACK_WEBHOOK_URL, json={"text": text, "mrkdwn": True}, timeout=10)
        print(f"[SLACK] {r.status_code}", flush=True)
    except Exception as e:
        print(f"[SLACK ERROR] {e}", flush=True)


# =============================================================================
# Playwright: extract job listings from the careers page
# =============================================================================

# Selectors ordered from most specific (Getro) to generic fallback
JOB_LINK_SELECTORS = [
    # Getro-specific
    "a[href*='/jobs/']",
    "a[href*='/job/']",
    # Common job card links
    ".job-listing a",
    ".job-card a",
    "article.job a",
    "li.job a",
    ".opening a",
    # Lever
    "a.posting-title",
    # Greenhouse
    "a.job-post",
    # Generic fallback
    "main a[href]",
]


def extract_job_listings(page: Page, careers_url: str) -> List[Dict[str, str]]:
    """
    Extract job listings from the rendered careers page.
    Returns list of {"title": str, "url": str, "department": str}.
    """
    base_domain = domain_of(careers_url)
    found: Dict[str, Dict] = {}  # url -> metadata

    for selector in JOB_LINK_SELECTORS:
        try:
            els = page.locator(selector)
            count = els.count()
            if count == 0:
                continue
            for i in range(min(count, 300)):
                try:
                    el = els.nth(i)
                    href = el.get_attribute("href", timeout=1000) or ""
                    if not href or href.startswith(("mailto:", "tel:", "#", "javascript:")):
                        continue
                    abs_url = normalize_url(careers_url, href)
                    if not abs_url:
                        continue
                    el_domain = domain_of(abs_url)
                    # Accept same domain or known career platforms
                    if el_domain not in (base_domain, "") and not _is_known_job_platform(abs_url):
                        continue
                    if abs_url.rstrip("/") == careers_url.rstrip("/"):
                        continue
                    if abs_url not in found:
                        title = _extract_title(el)
                        dept = _extract_department(el)
                        found[abs_url] = {"title": title, "url": abs_url, "department": dept}
                except Exception:
                    continue
            if len(found) >= 3:
                break
        except Exception:
            continue

    return list(found.values())


def _is_known_job_platform(url: str) -> bool:
    known = (
        "ashbyhq.com", "lever.co", "greenhouse.io", "workable.com",
        "recruitee.com", "smartrecruiters.com", "myworkdayjobs.com",
        "getro.com", "getro.app",
    )
    d = domain_of(url)
    return any(d.endswith(k) for k in known)


def _extract_title(el) -> str:
    for sel in ["h2", "h3", "h4", ".title", ".job-title", "[class*='title']", "strong"]:
        try:
            t = el.locator(sel).first.inner_text(timeout=1000).strip()
            if t and len(t) > 2:
                return t[:200]
        except Exception:
            pass
    try:
        t = el.inner_text(timeout=1000).strip()
        return t[:200] if t else "Unknown Position"
    except Exception:
        return "Unknown Position"


def _extract_department(el) -> str:
    for sel in [".department", ".team", ".category", "[class*='department']", "span"]:
        try:
            t = el.locator(sel).first.inner_text(timeout=1000).strip()
            if t and 1 < len(t) < 60:
                return t
        except Exception:
            pass
    return ""


# =============================================================================
# Playwright: check a single job URL, handling the Getro popup
# =============================================================================

# Text variants for the "skip popup" link on Getro and similar platforms
SKIP_POPUP_TEXTS = [
    "no thanks, take me to the application form",
    "no thanks",
    "take me to the application form",
    "skip",
    "go to application",
    "continue to application",
    "apply directly",
    "go directly",
]


def check_job_with_playwright(pw_page: Page, job_url: str) -> Tuple[Optional[int], Optional[str], str, str]:
    """
    Navigate to a job URL, handle the Getro interstitial popup if present,
    then verify the final application page loads.

    Returns (http_code, error_msg, result, final_url) where result is:
      "Active"  - application form loaded OK
      "Broken"  - application form returned 4xx/5xx or couldn't load
      "Blocked" - anti-bot or auth wall
    """
    final_url = job_url
    last_response = None

    try:
        # Navigate to the job URL and capture the HTTP response
        response = pw_page.goto(job_url, wait_until="domcontentloaded", timeout=CHECK_TIMEOUT * 1000)
        if response:
            last_response = response
            if response.status in (404, 410):
                return response.status, None, "Broken", pw_page.url

        # Wait a moment for any JS popup to appear
        pw_page.wait_for_timeout(POPUP_WAIT_MS)

        # ----------------------------------------------------------------
        # Detect and dismiss Getro popup
        # Try to find and click "No thanks, take me to the application form"
        # ----------------------------------------------------------------
        popup_dismissed = _dismiss_getro_popup(pw_page)

        if popup_dismissed:
            # Wait for navigation to the real application page
            try:
                pw_page.wait_for_load_state("domcontentloaded", timeout=15000)
                pw_page.wait_for_timeout(2000)
            except Exception:
                pass
            final_url = pw_page.url
            print(f"    Popup dismissed → navigated to: {final_url}", flush=True)
        else:
            final_url = pw_page.url

        # Check the current page status
        code, err, result = _assess_current_page(pw_page, last_response)
        return code, err, result, final_url

    except PlaywrightTimeout:
        return None, "playwright_timeout", "Broken", final_url
    except Exception as e:
        return None, type(e).__name__, "Broken", final_url


def _dismiss_getro_popup(pw_page: Page) -> bool:
    """
    Try to find and click the 'No thanks' / skip link in the Getro popup.
    Returns True if the popup was found and dismissed.
    """
    # First check if there's a popup/modal visible
    popup_indicators = [
        # Getro-specific
        "text=You're about to go to",
        "text=Before you go",
        # Generic modal detection
        "[role='dialog']",
        ".modal",
        "[class*='popup']",
        "[class*='interstitial']",
    ]

    popup_visible = False
    for indicator in popup_indicators:
        try:
            el = pw_page.locator(indicator).first
            if el.is_visible(timeout=500):
                popup_visible = True
                break
        except Exception:
            continue

    if not popup_visible:
        # No popup detected
        return False

    print("    Getro popup detected, looking for skip link...", flush=True)

    # Try clicking "No thanks, take me to the application form" (exact text match first)
    for skip_text in SKIP_POPUP_TEXTS:
        for loc_type in ["text", "partial text"]:
            try:
                if loc_type == "text":
                    el = pw_page.get_by_text(skip_text, exact=False).first
                else:
                    el = pw_page.locator(f"a:has-text('{skip_text}')").first

                if el.is_visible(timeout=500):
                    el.click(timeout=3000)
                    print(f"    Clicked: '{skip_text}'", flush=True)
                    return True
            except Exception:
                continue

    # Fallback: look for any external link icon (↗) next to "application form"
    try:
        # Getro uses an SVG external link icon next to the "take me to the application form" text
        links = pw_page.locator("a").all()
        for link in links[:30]:
            try:
                txt = (link.inner_text(timeout=1000) or "").lower().strip()
                if "application form" in txt or "no thanks" in txt or "directly" in txt:
                    if link.is_visible(timeout=300):
                        link.click(timeout=3000)
                        print(f"    Clicked fallback link: '{txt[:60]}'", flush=True)
                        return True
            except Exception:
                continue
    except Exception:
        pass

    # Last resort: press Escape to close modal
    try:
        pw_page.keyboard.press("Escape")
        pw_page.wait_for_timeout(500)
        print("    Pressed Escape to close popup", flush=True)
        return True
    except Exception:
        pass

    return False


def _assess_current_page(pw_page: Page, last_response) -> Tuple[Optional[int], Optional[str], str]:
    """
    Assess whether the current page loaded successfully.
    """
    current_url = pw_page.url

    # If we navigated to a known 404/error page
    if any(kw in current_url.lower() for kw in ["404", "not-found", "error", "expired"]):
        return 404, "url_contains_error_keyword", "Broken"

    # Check page title for error signals
    try:
        title = (pw_page.title() or "").lower()
        if any(kw in title for kw in ["404", "not found", "page not found", "error", "expired", "closed"]):
            return 404, f"page_title_contains: {title[:80]}", "Broken"
    except Exception:
        pass

    # Check if we're on a meaningful page (has some content)
    try:
        body_text = pw_page.locator("body").inner_text(timeout=3000)
        if len(body_text.strip()) < 50:
            return None, "page_almost_empty", "Broken"
    except Exception:
        pass

    # Use last captured HTTP response code
    if last_response:
        code = last_response.status
        if 200 <= code < 400:
            return code, None, "Active"
        if code in (404, 410):
            return code, None, "Broken"
        if code in (401, 403):
            return code, None, "Blocked"
        if code >= 500:
            return code, f"server_error_{code}", "Broken"

    # Default: page loaded, assume Active
    return 200, None, "Active"


# =============================================================================
# Notion helpers (optional — only used if NOTION_CAREERS_DB_ID is set)
# =============================================================================

_last_notion_call = 0.0


def _notion_headers() -> Dict:
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _notion_request(method: str, path: str, payload=None) -> Dict:
    global _last_notion_call
    delta = time.time() - _last_notion_call
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
        except requests.RequestException:
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
    raise RuntimeError(f"Notion {method} {path} failed after 4 attempts")


def _rt(v: str) -> Dict:
    v = (v or "")[:2000]
    return {"rich_text": [{"type": "text", "text": {"content": v}}]} if v else {"rich_text": []}


def _title_prop(v: str) -> Dict:
    v = (v or "")[:2000]
    return {"title": [{"type": "text", "text": {"content": v}}]} if v else {"title": []}


def _select(v: str) -> Dict:
    v = (v or "").strip()
    return {"select": {"name": v}} if v else {"select": None}


def _url_prop(v: Optional[str]) -> Dict:
    vv = (v or "").strip()
    return {"url": vv if vv else None}


def _date_prop(s: str) -> Dict:
    return {"date": {"start": s}}


def _number(n: Optional[int]) -> Dict:
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


def upsert_notion_job(
    db_id: str,
    job_url: str,
    job_title: str,
    department: str,
    result: str,
    http_code: Optional[int],
    error: str,
    final_url: str,
) -> bool:
    """
    Upsert job status in Notion.
    DB expected columns: Name (title), URL (url), Status (select),
    Department (rich_text), HTTP Code (number), Error (rich_text),
    Final URL (url), Last Checked (date).
    Returns True if this is a newly broken job.
    """
    try:
        schema = _notion_request("GET", f"/databases/{db_id}")
    except Exception as e:
        print(f"[NOTION] Cannot read DB schema: {e}", flush=True)
        return False

    title_prop_name = "Name"
    for k, v in schema.get("properties", {}).items():
        if v.get("type") == "title":
            title_prop_name = k
            break

    key = sha1(job_url)
    rows = _query_db(db_id)
    existing = None
    old_result = ""
    for row in rows:
        props = row.get("properties", {})
        u = ""
        try:
            u = (props.get("URL", {}).get("url") or "").strip()
        except Exception:
            pass
        if u and sha1(u) == key:
            existing = row
            try:
                sel = props.get("Status", {}).get("select")
                old_result = (sel.get("name") or "").strip() if sel else ""
            except Exception:
                pass
            break

    props_payload = {
        title_prop_name: _title_prop(job_title or job_url),
        "URL": _url_prop(job_url),
        "Status": _select(result),
        "Department": _rt(department),
        "HTTP Code": _number(http_code),
        "Error": _rt(error),
        "Final URL": _url_prop(final_url if final_url != job_url else ""),
        "Last Checked": _date_prop(iso_now()),
    }

    newly_broken = False
    if existing:
        if old_result != "Broken" and result == "Broken":
            newly_broken = True
        _notion_request("PATCH", f"/pages/{existing['id']}", {"properties": props_payload})
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
    print(f"[careers_checker] Popup wait: {POPUP_WAIT_MS}ms | Page load wait: {JOBS_PAGE_LOAD_WAIT}ms", flush=True)
    print(f"[careers_checker] Notion tracking: {'enabled' if NOTION_CAREERS_DB_ID and NOTION_TOKEN else 'disabled'}", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=USER_AGENT,
        )
        page = context.new_page()

        # ----------------------------------------------------------------
        # Step 1: load careers listing page
        # ----------------------------------------------------------------
        print(f"[careers_checker] Loading jobs listing page...", flush=True)
        try:
            page.goto(CAREERS_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(JOBS_PAGE_LOAD_WAIT)
        except PlaywrightTimeout:
            slack_notify(f"⚠️ *Careers Checker*: timeout loading <{CAREERS_URL}|careers page>")
            browser.close()
            return
        except Exception as e:
            slack_notify(f"⚠️ *Careers Checker*: error loading <{CAREERS_URL}|careers page> — `{e}`")
            browser.close()
            return

        jobs = extract_job_listings(page, CAREERS_URL)
        print(f"[careers_checker] Found {len(jobs)} job listings:", flush=True)
        for j in jobs:
            print(f"  • [{j.get('department','—'):20s}] {j['title']} → {j['url']}", flush=True)

        if not jobs:
            msg = (
                f"⚠️ *Careers Checker*: no job listings found on <{CAREERS_URL}|careers page>.\n"
                "The page may be empty, or the selectors don't match the platform yet."
            )
            slack_notify(msg)
            browser.close()
            return

        # ----------------------------------------------------------------
        # Step 2: check each job (navigate → dismiss Getro popup → verify)
        # ----------------------------------------------------------------
        results: List[Dict] = []
        newly_broken_jobs: List[Dict] = []

        for job in jobs:
            url = job["url"]
            print(f"\n[careers_checker] Checking: {job['title']}", flush=True)
            print(f"  URL: {url}", flush=True)

            # Each job gets its own page context to avoid state leaking between checks
            job_page = context.new_page()
            try:
                code, err, result, final_url = check_job_with_playwright(job_page, url)
            finally:
                try:
                    job_page.close()
                except Exception:
                    pass

            job_data = {
                "title": job["title"],
                "url": url,
                "final_url": final_url,
                "department": job.get("department", ""),
                "result": result,
                "http_code": code,
                "error": err or "",
            }
            results.append(job_data)

            status_icon = "✅" if result == "Active" else ("⚠️" if result == "Blocked" else "❌")
            print(f"  {status_icon} Result: {result} (HTTP {code}) → {final_url}", flush=True)

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
                        final_url=final_url,
                    )
                    if is_new_broken:
                        newly_broken_jobs.append(job_data)
                except Exception as e:
                    print(f"  [NOTION ERROR] {e}", flush=True)
                    if result == "Broken":
                        newly_broken_jobs.append(job_data)
            else:
                # Without Notion history, flag all broken as newly broken
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
            f"\n[careers_checker] ── Summary ──────────────────────────────",
            flush=True,
        )
        print(f"  Total: {total} | Active: {active} | Broken: {broken} | Blocked: {blocked}", flush=True)
        for r in results:
            icon = "✅" if r["result"] == "Active" else ("⚠️" if r["result"] == "Blocked" else "❌")
            print(f"  {icon} {r['title']} → {r['result']}", flush=True)

        if newly_broken_jobs:
            lines = [f"🚨 *Careers Checker*: {len(newly_broken_jobs)} job posting(s) have a broken application form:"]
            for j in newly_broken_jobs:
                dept = f" [{j['department']}]" if j.get("department") else ""
                lines.append(
                    f"  • <{j['url']}|{j['title']}>{dept}\n"
                    f"    ↳ Result: *{j['result']}* (HTTP {j['http_code'] or 'no response'})"
                    + (f"\n    ↳ Final URL: `{j['final_url']}`" if j['final_url'] != j['url'] else "")
                )
            lines.append(f"\n_<{CAREERS_URL}|View open positions at Marble>_")
            slack_notify("\n".join(lines))

        elif SLACK_ALWAYS_NOTIFY:
            lines = [f"✅ *Careers Checker*: {active}/{total} job postings have a working application form"]
            if blocked:
                lines.append(f"  ⚠️ {blocked} job(s) could not be fully verified (anti-bot)")
            lines.append(f"_<{CAREERS_URL}|careers.marble.studio/jobs>_")
            slack_notify("\n".join(lines))

        else:
            print("[careers_checker] All OK — no Slack alert (SLACK_ALWAYS_NOTIFY=true to always notify)", flush=True)

        print("[careers_checker] Done.", flush=True)


if __name__ == "__main__":
    # Hard kill after 10 minutes to prevent CI jobs hanging indefinitely
    def _timeout_handler(signum, frame):
        print("[careers_checker] FATAL: global timeout (10m) exceeded, aborting.", flush=True)
        raise SystemExit(1)

    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(600)  # 10 minutes

    main()
