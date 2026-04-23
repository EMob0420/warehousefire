#!/usr/bin/env python3
"""
expand_nfirs.py
Extract INC_TYPE=111 (building fire) records for SoCal storage/warehouse facilities
from FEMA NFIRS zips, de-duplicate against existing 654 embedded records,
output JSON for embedding in warehouse_explorer.html.

Filter: PROP_USE starts with '8' (storage/warehouse category) unless --all flag is set.
"""

import os, sys, json, csv, zipfile, io
from collections import defaultdict

DOWNLOADS = '/Volumes/DATA/Fire data/nfirs_downloads'
HTML_FILE  = '/Volumes/DATA/Fire data/warehouse_explorer.html'
OUTPUT_JSON = '/Volumes/DATA/Fire data/expanded_nfirs.json'

ALL_111 = '--all' in sys.argv  # include all prop-use codes, not just storage

FIPS_COUNTY = {
    '37': 'Los Angeles',  '037': 'Los Angeles',
    '59': 'Orange',       '059': 'Orange',
    '65': 'Riverside',    '065': 'Riverside',
    '71': 'San Bernardino','071': 'San Bernardino',
}

FDID_PREFIX_COUNTY = {
    '19': 'Los Angeles',
    '30': 'Orange',
    '33': 'Riverside',
    '36': 'San Bernardino',
}

CAUSE_LOOKUP = {
    '0': 'Other', '1': 'Intentional', '2': 'Unintentional',
    '3': 'Equipment Failure', '4': 'Act of Nature',
    '5': 'Under Investigation', 'U': 'Undetermined',
}

ZIP_FILES = {
    2005: 'usfa_nfirs_2005.zip',
    2006: 'usfa_nfirs_2006.zip',
    2007: 'usfa_nfirs_2007.zip',
    2008: 'usfa_nfirs_2008.zip',
    2009: 'usfa_nfirs_2009.zip',
    2010: 'usfa_nfirs_2010.zip',
    2012: 'usfa_nfirs_2012.zip',
    2014: 'usfa_nfirs_2014.zip',
    2015: 'usfa_nfirs_2015.zip',
    2016: 'usfa_nfirs_2016.zip',
    2017: 'usfa_nfirs_2017.zip',
    2024: 'nfirs_fire_hazmat_pdr_2024.zip',
}


# ── Key helpers ────────────────────────────────────────────────────────────────

def parse_key(k):
    """Normalize embedded key string → lookup tuple (same logic as enrich_puc.py)."""
    parts = k.split('_')
    if len(parts) < 5:
        return None
    fdid   = parts[1].lstrip('0') or '0'
    date   = parts[2]
    if len(date) == 7:
        date = '0' + date            # pad MDDYYYY → MMDDYYYY
    inc_no = parts[3].lstrip('0') or '0'
    exp_no = parts[4].lstrip('0') or '0'
    return (fdid, date, inc_no, exp_no)


def nfirs_lookup_key(fdid, inc_date, inc_no, exp_no):
    """Build normalized lookup tuple from NFIRS row fields."""
    f = fdid.strip().strip('"').lstrip('0') or '0'
    d = inc_date.strip().strip('"')
    if len(d) == 7:
        d = '0' + d
    i = inc_no.strip().strip('"').lstrip('0') or '0'
    e = exp_no.strip().strip('"').lstrip('0') or '0'
    return (f, d, i, e)


def nfirs_raw_key(fdid, inc_date, inc_no, exp_no):
    """Build raw key string from NFIRS fields (preserves leading zeros)."""
    return f"CA_{fdid.strip()}_{inc_date.strip()}_{inc_no.strip()}_{exp_no.strip()}"


def make_dt(inc_date):
    """Convert NFIRS date (MDDYYYY or MMDDYYYY) → YYYY-MM-DD."""
    d = inc_date.strip().strip('"')
    if len(d) == 7:
        return f"{d[3:7]}-{int(d[0]):02d}-{d[1:3]}"
    if len(d) == 8:
        return f"{d[4:8]}-{d[0:2]}-{d[2:4]}"
    return ''


def safe_int(v):
    try:
        return int(str(v).strip().strip('"') or '0')
    except Exception:
        return 0


# ── Zip helpers ────────────────────────────────────────────────────────────────

def _is_basicincident(name):
    bn = os.path.basename(name).lower()
    return bn.startswith('basicincident') and not bn.endswith('.dbf')

def _is_fireincident(name):
    bn = os.path.basename(name).lower()
    return bn.startswith('fireincident') and not bn.endswith('.dbf')

def _is_fdheader(name):
    bn = os.path.basename(name).lower()
    return 'fdheader' in bn and not bn.endswith('.dbf')

def _is_incidentaddress(name):
    bn = os.path.basename(name).lower()
    return 'incidentaddress' in bn and not bn.endswith('.dbf')

def _find_inner_zip(names):
    for n in names:
        if n.upper().endswith('.ZIP') and 'FIRES' in n.upper():
            return n
    for n in names:
        bn = os.path.basename(n).upper()
        if bn.endswith('.ZIP') and ('NFIRS' in bn or 'USFA' in bn):
            if not any(x in bn for x in ('HAZMAT', 'CAUSE', 'FIRECAUSE')):
                return n
    return None

class _NulFilter:
    """Wraps a zipfile binary stream: strips NUL bytes, yields decoded lines.
    Used so csv.DictReader never holds the full uncompressed file in RAM."""
    CHUNK = 1 << 17  # 128 KB

    def __init__(self, f, enc='latin-1'):
        self._f   = f
        self._enc = enc
        self._buf = b''
        self._eof = False

    def readline(self):
        while not self._eof and b'\n' not in self._buf:
            chunk = self._f.read(self.CHUNK)
            if not chunk:
                self._eof = True
                break
            self._buf += chunk.replace(b'\x00', b'')
        if b'\n' in self._buf:
            i = self._buf.index(b'\n')
            line, self._buf = self._buf[:i + 1], self._buf[i + 1:]
        else:
            line, self._buf = self._buf, b''
        return line.decode(self._enc, errors='replace')

    def __iter__(self):
        while True:
            line = self.readline()
            if not line:
                break
            yield line


def stream_csv(zf, name):
    """Stream-parse a CSV member of a zipfile; never loads the full file into RAM.
    Returns (DictReader, open_file_handle) — caller must close the file handle."""
    import itertools
    f       = zf.open(name)
    wrapper = _NulFilter(f)
    header  = wrapper.readline()            # peek at first line for delimiter
    delim   = '^' if '^' in header else ','
    reader  = csv.DictReader(
        itertools.chain([header], wrapper), # put header back as first row
        delimiter=delim, quotechar='"'
    )
    return reader, f


# ── Per-year extraction ────────────────────────────────────────────────────────

def process_year(year, zip_path, existing_keys):
    print(f"\n[{year}] {os.path.basename(zip_path)}", flush=True)
    new_records = []

    with zipfile.ZipFile(zip_path) as outer:
        names = outer.namelist()

        inner_zf = None
        if not any(_is_basicincident(n) for n in names):
            inner_name = _find_inner_zip(names)
            if inner_name:
                print(f"  nested: {inner_name}", flush=True)
                inner_bytes = outer.read(inner_name)
                inner_zf = zipfile.ZipFile(io.BytesIO(inner_bytes))

        zf = inner_zf if inner_zf else outer
        znames = zf.namelist()

        bi_name = next((n for n in znames if _is_basicincident(n)), None)
        fi_name = next((n for n in znames if _is_fireincident(n)), None)
        fd_name = next((n for n in znames if _is_fdheader(n)), None)
        ia_name = next((n for n in znames if _is_incidentaddress(n)), None)

        if not bi_name:
            print(f"  no basicincident found — skipping")
            if inner_zf:
                inner_zf.close()
            return []

        # FDID → county map from fdheader
        socal_fdids = {}
        if fd_name:
            reader, fh = stream_csv(zf, fd_name)
            for row in reader:
                if row.get('STATE', '').strip() != 'CA':
                    continue
                fdid = row.get('FDID', '').strip().strip('"')
                fips = row.get('FD_FIP_CTY', '').strip().strip('"')
                county = FIPS_COUNTY.get(fips)
                if not county:
                    for prefix, cname in FDID_PREFIX_COUNTY.items():
                        if fdid.startswith(prefix):
                            county = cname
                            break
                if county:
                    socal_fdids[fdid] = county
            fh.close()
        print(f"  {len(socal_fdids)} SoCal FDIDs", flush=True)

        # fireincident: sqft / floors / cause / heat  (SoCal only)
        fire_data = {}
        if fi_name:
            reader, fh = stream_csv(zf, fi_name)
            for row in reader:
                if row.get('STATE', '').strip().strip('"') != 'CA':
                    continue
                fdid = row.get('FDID', '').strip().strip('"')
                if fdid not in socal_fdids:
                    continue
                lk = nfirs_lookup_key(
                    fdid,
                    row.get('INC_DATE', ''), row.get('INC_NO', ''), row.get('EXP_NO', '')
                )
                cause_raw = row.get('CAUSE_IGN', '').strip().strip('"')
                fire_data[lk] = {
                    'sqft':   safe_int(row.get('TOT_SQ_FT', 0)),
                    'floors': safe_int(row.get('BLDG_ABOVE', 0)),
                    'cause':  CAUSE_LOOKUP.get(cause_raw, cause_raw),
                    'heat':   row.get('HEAT_SOURC', '').strip().strip('"'),
                }
            fh.close()
        print(f"  {len(fire_data)} fireincident SoCal rows", flush=True)

        # incidentaddress: city / zip / street  (SoCal only)
        addr_data = {}
        if ia_name:
            reader, fh = stream_csv(zf, ia_name)
            for row in reader:
                if row.get('STATE', '').strip().strip('"') != 'CA':
                    continue
                fdid = row.get('FDID', '').strip().strip('"')
                if fdid not in socal_fdids:
                    continue
                lk = nfirs_lookup_key(
                    fdid,
                    row.get('INC_DATE', ''), row.get('INC_NO', ''), row.get('EXP_NO', '')
                )
                sname = row.get('STREETNAME', '').strip().strip('"')
                stype = row.get('STREETTYPE', '').strip().strip('"')
                addr_data[lk] = {
                    'city': row.get('CITY', '').strip().strip('"').upper(),
                    'zip':  row.get('ZIP5', '').strip().strip('"'),
                    'addr': f"{sname} {stype}".strip(),
                }
            fh.close()
        print(f"  {len(addr_data)} incidentaddress SoCal rows", flush=True)

        # Stream basicincident
        reader, fh = stream_csv(zf, bi_name)
        total = ca = socal_111 = new = dup = 0

        for row in reader:
            total += 1
            if row.get('STATE', '').strip().strip('"') != 'CA':
                continue
            ca += 1

            fdid = row.get('FDID', '').strip().strip('"')
            if fdid not in socal_fdids:
                continue

            inc_type = row.get('INC_TYPE', '').strip().strip('"')
            if inc_type != '111':
                continue

            prop_use = row.get('PROP_USE', '').strip().strip('"')
            if not ALL_111 and not prop_use.startswith('8'):
                continue    # skip non-storage when in default mode

            socal_111 += 1

            inc_date = row.get('INC_DATE', '').strip().strip('"')
            inc_no   = row.get('INC_NO',   '').strip().strip('"')
            exp_no   = row.get('EXP_NO',   '').strip().strip('"')

            lk = nfirs_lookup_key(fdid, inc_date, inc_no, exp_no)
            if lk in existing_keys:
                dup += 1
                continue
            existing_keys.add(lk)
            new += 1

            fi = fire_data.get(lk, {})
            ai = addr_data.get(lk, {})

            dt = make_dt(inc_date)
            try:
                yr = int(dt[:4])
            except Exception:
                yr = year

            rec = {
                'k':      nfirs_raw_key(fdid, inc_date, inc_no, exp_no),
                'yr':     yr,
                'dt':     dt,
                'tc':     inc_type,
                'ty':     'Building fire',
                'co':     socal_fdids[fdid],
                'ci':     ai.get('city', ''),
                'zip':    ai.get('zip', ''),
                'addr':   ai.get('addr', ''),
                'pl':     safe_int(row.get('PROP_LOSS', 0)),
                'cl':     safe_int(row.get('CONT_LOSS', 0)),
                'pv':     safe_int(row.get('PROP_VAL', 0)),
                'ffd':    safe_int(row.get('FF_DEATH', 0)),
                'cvd':    safe_int(row.get('OTH_DEATH', 0)),
                'ffi':    safe_int(row.get('FF_INJ', 0)),
                'cvi':    safe_int(row.get('OTH_INJ', 0)),
                'sqft':   fi.get('sqft', 0),
                'floors': fi.get('floors', 0),
                'cause':  fi.get('cause', ''),
                'heat':   fi.get('heat', ''),
                'puc':    prop_use,
            }
            new_records.append(rec)

        fh.close()
        filter_label = 'all-111' if ALL_111 else '8xx-111'
        print(f"  total={total:,} CA={ca:,} SoCal-{filter_label}={socal_111:,} "
              f"new={new:,} dup={dup:,}", flush=True)

        if inner_zf:
            inner_zf.close()

    return new_records


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print(f"Mode: {'all INC_TYPE=111' if ALL_111 else 'storage/warehouse PROP_USE 8xx only'}")
    print("Loading existing embedded keys ...")

    with open(HTML_FILE, encoding='utf-8') as f:
        content = f.read()
    start = content.find('const NFIRS_DATA = ')
    if start == -1:
        print("ERROR: NFIRS_DATA not found in HTML"); sys.exit(1)
    start += len('const NFIRS_DATA = ')
    depth, i = 0, start
    while i < len(content):
        if content[i] == '[': depth += 1
        elif content[i] == ']':
            depth -= 1
            if depth == 0:
                end = i + 1; break
        i += 1
    existing_records = json.loads(content[start:end])
    existing_keys = set()
    for r in existing_records:
        pk = parse_key(r['k'])
        if pk:
            existing_keys.add(pk)
    print(f"  {len(existing_keys)} existing keys loaded")

    all_new = []
    for year in sorted(ZIP_FILES.keys()):
        fname = ZIP_FILES[year]
        zip_path = os.path.join(DOWNLOADS, fname)
        if not os.path.exists(zip_path):
            print(f"[{year}] {fname} not found — skipping")
            continue
        try:
            recs = process_year(year, zip_path, existing_keys)
            all_new.extend(recs)
            print(f"  cumulative new: {len(all_new):,}", flush=True)
        except Exception as e:
            import traceback
            print(f"[{year}] ERROR: {e}")
            traceback.print_exc()

    print(f"\n{'='*60}")
    print(f"Total new records: {len(all_new):,}")

    # County / year breakdown
    by_county = {}
    by_year   = {}
    for r in all_new:
        by_county[r['co']] = by_county.get(r['co'], 0) + 1
        by_year[r['yr']]   = by_year.get(r['yr'], 0) + 1
    print("By county:", dict(sorted(by_county.items())))
    print("By year:",   dict(sorted(by_year.items())))

    with open(OUTPUT_JSON, 'w') as f:
        json.dump(all_new, f, separators=(',', ':'))
    print(f"\nWrote {OUTPUT_JSON}")
    print(f"Run merge_nfirs.py (or manually append) to embed these records in the HTML.")

if __name__ == '__main__':
    main()
