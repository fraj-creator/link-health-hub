import argparse
import time
from datetime import datetime, timezone

import pandas as pd
import requests

USER_AGENT = "Mozilla/5.0 (CommunityLinkChecker)"
TIMEOUT = 12

def check_url(url: str):
    if not isinstance(url, str) or not url.strip():
        return None, "empty_url"

    url = url.strip()

    try:
        r = requests.head(
            url,
            allow_redirects=True,
            timeout=TIMEOUT,
            headers={"User-Agent": USER_AGENT},
        )
        code = r.status_code

        if code in (405, 403) or code >= 500:
            r = requests.get(
                url,
                allow_redirects=True,
                timeout=TIMEOUT,
                headers={"User-Agent": USER_AGENT},
                stream=True,
            )
            code = r.status_code

        return code, None
    except requests.RequestException as e:
        return None, type(e).__name__

def status_from_code(code):
    if code is None:
        return "Broken"
    if 200 <= code < 400:
        return "Active"
    if code in (404, 410):
        return "Broken"
    if code == 403:
        return "Active"
    return "Broken"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True, help="Input CSV path")
    parser.add_argument("-o", "--output", required=True, help="Output CSV path")
    parser.add_argument("--url-col", default="Primary URL")
    parser.add_argument("--status-col", default="Status")
    parser.add_argument("--skip-status", default="Replaced")
    parser.add_argument("--sleep", type=float, default=0.2)
    args = parser.parse_args()

    df = pd.read_csv(args.input)

    if args.url_col not in df.columns:
        raise SystemExit(f"Missing column '{args.url_col}'. Found: {list(df.columns)}")

    if "HTTP Code" not in df.columns:
        df["HTTP Code"] = pd.NA
    if "Checked At" not in df.columns:
        df["Checked At"] = pd.NA
    if "Check Error" not in df.columns:
        df["Check Error"] = pd.NA
    if args.status_col not in df.columns:
        df[args.status_col] = pd.NA

    checked_at = datetime.now(timezone.utc).isoformat()
    checked = 0
    updated = 0

    for idx, row in df.iterrows():
        url = row.get(args.url_col)
        current_status = row.get(args.status_col)

        if isinstance(current_status, str) and current_status.strip() == args.skip_status:
            continue

        code, err = check_url(url)
        new_status = status_from_code(code)

        df.at[idx, "HTTP Code"] = code if code is not None else pd.NA
        df.at[idx, "Checked At"] = checked_at
        df.at[idx, "Check Error"] = err if err else pd.NA

        checked += 1

        if new_status != current_status:
            df.at[idx, args.status_col] = new_status
            updated += 1

        time.sleep(args.sleep)

    df.to_csv(args.output, index=False)
    print(f"Done. Checked={checked}, Status updated={updated}")
    print(f"Saved to: {args.output}")

if __name__ == "__main__":
    main()
