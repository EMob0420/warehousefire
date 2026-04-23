#!/usr/bin/env python3
"""
merge_nfirs.py
Merges expanded_nfirs.json records into warehouse_explorer.html NFIRS_DATA array,
then syncs to public/index.html.

Usage: python3 merge_nfirs.py
"""

import os, json, re, sys

HTML_FILE    = '/Volumes/DATA/Fire data/warehouse_explorer.html'
PUBLIC_HTML  = '/Volumes/DATA/Fire data/public/index.html'
EXPANDED_JSON = '/Volumes/DATA/Fire data/expanded_nfirs.json'


def parse_key(k):
    parts = k.split('_')
    if len(parts) < 5:
        return None
    fdid   = parts[1].lstrip('0') or '0'
    date   = parts[2]
    if len(date) == 7:
        date = '0' + date
    inc_no = parts[3].lstrip('0') or '0'
    exp_no = parts[4].lstrip('0') or '0'
    return (fdid, date, inc_no, exp_no)


def load_html_nfirs(content):
    marker = 'const NFIRS_DATA = '
    start = content.find(marker)
    if start == -1:
        raise ValueError("NFIRS_DATA not found in HTML")
    arr_start = start + len(marker)
    depth, i = 0, arr_start
    while i < len(content):
        if content[i] == '[': depth += 1
        elif content[i] == ']':
            depth -= 1
            if depth == 0:
                arr_end = i + 1
                break
        i += 1
    return arr_start, arr_end, json.loads(content[arr_start:arr_end])


def main():
    if not os.path.exists(EXPANDED_JSON):
        print(f"ERROR: {EXPANDED_JSON} not found. Run expand_nfirs.py first.")
        sys.exit(1)

    with open(EXPANDED_JSON) as f:
        new_records = json.load(f)
    print(f"New records to merge: {len(new_records):,}")

    with open(HTML_FILE, encoding='utf-8') as f:
        content = f.read()

    arr_start, arr_end, existing = load_html_nfirs(content)
    print(f"Existing records: {len(existing):,}")

    # Build existing key set for de-dup check
    existing_keys = set()
    for r in existing:
        pk = parse_key(r['k'])
        if pk:
            existing_keys.add(pk)

    # Filter out any duplicates in new_records
    added = []
    for r in new_records:
        pk = parse_key(r['k'])
        if pk and pk not in existing_keys:
            added.append(r)
            existing_keys.add(pk)
        else:
            print(f"  skip dup: {r['k']}")

    combined = existing + added
    print(f"Combined total: {len(combined):,} ({len(added):,} added)")

    # Serialize compactly
    new_json = json.dumps(combined, separators=(',', ':'))

    # Replace the NFIRS_DATA array in the HTML
    new_content = content[:arr_start] + new_json + content[arr_end:]

    with open(HTML_FILE, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print(f"Updated {HTML_FILE}")

    # Sync to public/index.html
    if os.path.exists(PUBLIC_HTML):
        with open(PUBLIC_HTML, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Synced to {PUBLIC_HTML}")
    else:
        print(f"WARNING: {PUBLIC_HTML} not found — skipping sync")

    # Summary by county/year
    by_county, by_year = {}, {}
    for r in added:
        by_county[r.get('co', '?')] = by_county.get(r.get('co', '?'), 0) + 1
        by_year[r.get('yr', 0)]     = by_year.get(r.get('yr', 0), 0) + 1
    print("\nAdded by county:", dict(sorted(by_county.items())))
    print("Added by year:",   dict(sorted(by_year.items())))


if __name__ == '__main__':
    main()
