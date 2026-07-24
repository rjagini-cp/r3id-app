from flask import Flask, jsonify, request, send_from_directory

from flask_cors import CORS

from google.cloud import bigquery

from google.oauth2 import service_account

import os

import re

import json

import math

import datetime

import traceback

import pandas as pd

import anthropic

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger("datagenie")

creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")

if creds_json:
    _bq_credentials = service_account.Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
elif not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    raise RuntimeError(
        "BigQuery credentials not configured. Set GOOGLE_APPLICATION_CREDENTIALS_JSON "
        "(preferred, JSON content) or GOOGLE_APPLICATION_CREDENTIALS (path to file)."
    )
else:
    _bq_credentials = None  # use GOOGLE_APPLICATION_CREDENTIALS file path

app = Flask(__name__)

CORS(app)

@app.route("/")
@app.route("/r3id.html")
def serve_frontend():
    """Serve the DataGenie frontend."""
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), "r3id.html")

client = bigquery.Client(credentials=_bq_credentials) if _bq_credentials else bigquery.Client()

_anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY")

_anthropic_client = anthropic.Anthropic(api_key=_anthropic_api_key) if _anthropic_api_key else None

if not _anthropic_client:
    logger.warning("ANTHROPIC_API_KEY not set — /analytics/nlq will be unavailable.")

def handle_error(endpoint_name: str, e: Exception):
    """Log exception server-side; return a safe generic error to the client."""
    logger.exception(f"Error in {endpoint_name}: {e}")
    return jsonify({"error": "An internal error occurred. Check server logs."}), 500

@app.errorhandler(Exception)
def handle_unhandled_exception(e):
    """Catch any unhandled exception and return JSON instead of HTML 500."""
    logger.exception(f"Unhandled exception: {e}")
    return jsonify({"error": "An internal error occurred."}), 500

DATABASE_INDEX_SHEET_ID = "1plJGqLHT-9u90ojvAKnrtA0edTmTr45suMV2OnQfx_c"

DATABASE_INDEX_ACCOUNTS_GID = "1162564586"

_user_groups_cache = {"data": None, "fetched_at": None}

_USER_GROUPS_CACHE_TTL_SECONDS = 600  # 10 minutes

def get_user_groups(force_refresh=False):
    """Fetches the Database_Index 'Accounts' tab's System / Team / System UserID
    columns and returns {"bigquery": {team: [userIds]}, "camstar": {team: [userIds]}}.
    Cached in memory for _USER_GROUPS_CACHE_TTL_SECONDS to avoid hitting the
    sheet on every request. No credentials needed — reads the sheet's public
    CSV export (same access level as viewing it in a browser)."""
    now = datetime.datetime.utcnow()
    cached = _user_groups_cache["data"]
    fetched_at = _user_groups_cache["fetched_at"]
    if (not force_refresh and cached is not None and fetched_at is not None
            and (now - fetched_at).total_seconds() < _USER_GROUPS_CACHE_TTL_SECONDS):
        return cached

    csv_url = (f"https://docs.google.com/spreadsheets/d/{DATABASE_INDEX_SHEET_ID}"
               f"/export?format=csv&gid={DATABASE_INDEX_ACCOUNTS_GID}")
    try:
        df = pd.read_csv(csv_url)
    except Exception as e:
        logger.error(f"Failed to fetch Database_Index sheet: {e}")
        return cached if cached is not None else {"bigquery": {}, "camstar": {}}

    df.columns = [str(c).strip() for c in df.columns]
    result = {"bigquery": {}, "camstar": {}}
    for _, row in df.iterrows():
        system = str(row.get("System", "")).strip()
        team = str(row.get("Team", "")).strip()
        user_id = str(row.get("System UserID", "")).strip()
        if system not in ("BigQuery", "Camstar") or not team or not user_id or user_id.lower() == "nan":
            continue
        bucket = "bigquery" if system == "BigQuery" else "camstar"
        result[bucket].setdefault(team, []).append(user_id)

    _user_groups_cache["data"] = result
    _user_groups_cache["fetched_at"] = now
    return result

@app.route("/analytics/user-groups", methods=["GET"])
def user_groups():
    try:
        force_refresh = request.args.get("refresh", "").lower() == "true"
        return jsonify(get_user_groups(force_refresh=force_refresh))
    except Exception as e:
        return handle_error(request.endpoint, e)

DATABASE_INDEX_VOLUME_FORECAST_GID = "502647842"

_forecast_targets_cache = {"data": None, "fetched_at": None}

_FORECAST_TARGETS_CACHE_TTL_SECONDS = 600  # 10 minutes

def _shift_two_months_ahead(monthly_12):
    """CAD runs 2 months ahead: new[Jan..Oct] = old[Mar..Dec]; new[Nov]=new[Dec]=old[Dec]
    (no Jan/Feb-of-next-year data exists yet, so December's number is reused).
    monthly_12 is a 0-indexed list, Jan=index0..Dec=index11."""
    new = [None] * 12
    for i in range(10):
        new[i] = monthly_12[i + 2]
    new[10] = monthly_12[11]
    new[11] = monthly_12[11]
    return new

def get_forecast_targets(force_refresh=False):
    """Fetches the live 'Volume Forecast' tab and returns the full FORECAST_2026-style
    dict: {bucket_name: {1: val, ..., 12: val}}, 2-month-shifted per CAD lead time.
    - Rollup rows (66-85, column H) are read directly by their real system name.
    - The 4 case-type splits (Kinos Axiom/Aeros, rTSA Custom/Cleared) are computed
      from the raw Vena Code rows + marketing rows, same combination logic as
      before — this logic stays in code since the raw numbers keep getting revised,
      but no numbers themselves are hardcoded anymore.
    - A few items with no live source at all fall back to FORECAST_2026_FALLBACKS.
    Cached in memory for _FORECAST_TARGETS_CACHE_TTL_SECONDS.
    """
    now = datetime.datetime.utcnow()
    cached = _forecast_targets_cache["data"]
    fetched_at = _forecast_targets_cache["fetched_at"]
    if (not force_refresh and cached is not None and fetched_at is not None
            and (now - fetched_at).total_seconds() < _FORECAST_TARGETS_CACHE_TTL_SECONDS):
        return cached

    csv_url = (f"https://docs.google.com/spreadsheets/d/{DATABASE_INDEX_SHEET_ID}"
               f"/export?format=csv&gid={DATABASE_INDEX_VOLUME_FORECAST_GID}")
    try:
        df = pd.read_csv(csv_url, header=None)
    except Exception as e:
        logger.error(f"Failed to fetch Volume Forecast sheet: {e}")
        return cached if cached is not None else dict(FORECAST_2026_FALLBACKS)

    # Positional columns (0-indexed): G=6 (Vena Code), H=7 (label/case-type name),
    # O..Z=14..25 (Jan..Dec monthly values). header=None means sheet row N == df.iloc[N-1].
    def months_for_row(row_idx):
        vals = df.iloc[row_idx, 14:26].tolist()
        return [float(v) if pd.notna(v) and str(v).strip() != '' else 0.0 for v in vals]

    # Raw + marketing rows: sheet rows 2-63 -> df index 1-62. Keyed by Vena Code (G),
    # falling back to the label column (H) for marketing rows where G is blank.
    raw_by_key = {}
    for i in range(1, 63):
        if i >= len(df):
            break
        g_val = str(df.iloc[i, 6]).strip() if pd.notna(df.iloc[i, 6]) else ''
        h_val = str(df.iloc[i, 7]).strip() if pd.notna(df.iloc[i, 7]) else ''
        key = g_val if g_val else h_val
        if not key or key.lower() == 'nan':
            continue
        raw_by_key[key] = months_for_row(i)

    # Rollup rows: sheet rows 66-85 -> df index 65-84. Keyed directly by column H,
    # which now holds the real system name (Camstar Product Line / BigQuery
    # case_category_name) after the sheet edit.
    rollup_by_name = {}
    for i in range(65, 85):
        if i >= len(df):
            break
        h_val = str(df.iloc[i, 7]).strip() if pd.notna(df.iloc[i, 7]) else ''
        if not h_val or h_val.lower() == 'nan':
            continue
        rollup_by_name[h_val] = months_for_row(i)

    result_raw_monthly = dict(rollup_by_name)

    # The 4 case-type splits — combination logic stays in code, numbers are live.
    def add_lists(*lists):
        return [sum(vals) for vals in zip(*lists)]

    def half(lst):
        return [v / 2 for v in lst]

    try:
        kinos_match = raw_by_key.get('Kinos Match', [0]*12)
        kinos_psr_mktg = raw_by_key.get('Kinos PSR', [0]*12)
        result_raw_monthly['Kinos Axiom TAR + PSR'] = add_lists(kinos_match, kinos_psr_mktg)

        kinos_aeros_match = raw_by_key.get('Kinos Aeros Match', [0]*12)
        aeros_psr_mktg = raw_by_key.get('Aeros PSR', [0]*12)
        result_raw_monthly['Kinos Aeros Modular Stem'] = add_lists(kinos_aeros_match, aeros_psr_mktg)

        custom_veritas_rtsa = raw_by_key.get('Custom Veritas rTSA', [0]*12)
        patient_specific_rtsa = raw_by_key.get('Patient Specific rTSA', [0]*12)
        shoulder_mktg = raw_by_key.get('Shoulder', [0]*12)
        result_raw_monthly['Custom rTSA - Custom Glenoid + Standard Stem'] = add_lists(custom_veritas_rtsa, half(shoulder_mktg))
        result_raw_monthly['Cleared rTSA - Veritas PS Glenoid + Standard Stem'] = add_lists(patient_specific_rtsa, half(shoulder_mktg))
    except Exception as e:
        logger.error(f"Failed to compute forecast case-type splits: {e}")

    # Apply the 2-month-ahead shift to everything derived from the live sheet.
    result = {}
    for name, monthly in result_raw_monthly.items():
        shifted = _shift_two_months_ahead(monthly)
        result[name] = {m + 1: round(shifted[m]) for m in range(12)}

    # Merge in the fallbacks for anything with no live source at all.
    for name, monthly_dict in FORECAST_2026_FALLBACKS.items():
        if name not in result:
            result[name] = monthly_dict

    _forecast_targets_cache["data"] = result
    _forecast_targets_cache["fetched_at"] = now
    return result

@app.route("/analytics/forecast-targets", methods=["GET"])
def forecast_targets_debug():
    """Debug/inspection endpoint — shows the live-computed forecast targets."""
    try:
        force_refresh = request.args.get("refresh", "").lower() == "true"
        return jsonify(get_forecast_targets(force_refresh=force_refresh))
    except Exception as e:
        return handle_error(request.endpoint, e)

CAMSTAR_PRODUCT_GROUPS = {
    'Cleared Knee': ['Identity CR','Identity CR Dragon','Identity PS','Imprint','Stryker (Triathlon)',
                     'iTotal (G2) CR','iTotal (G2) PS','iUni','PKR','iDuo'],
    'Cleared Hip':  ['Hip'],
}

FORECAST_2026_FALLBACKS = {
    "Custom Shoulder (all)":               {1:23,2:10,3:16,4:19,5:17,6:19,7:12,8:12,9:14,10:15,11:13,12:15},
    "Total Talus and Other Arthroplasty":  {1:14,2:19,3:18,4:23,5:20,6:21,7:20,8:20,9:20,10:21,11:19,12:21},
    "Maxilla":              {1:1,2:2,3:3,4:2,5:2,6:3,7:3,8:3,9:2,10:3,11:3,12:3},
    "Mandible":             {1:1,2:1,3:2,4:2,5:2,6:2,7:2,8:2,9:2,10:2,11:2,12:2},
    "Total Knee Arthroplasty": {1:2,2:3,3:2,4:4,5:4,6:5,7:5,8:5,9:5,10:7,11:5,12:7},
}

def get_forecast(product_lines, case_types=None, date_from=None, date_to=None):
    """Return forecast target for a given set of product lines/case types and date range.
    Returns dict with total, by_month, and daily_rate."""

    def month_forecast(bucket, month_num):
        return get_forecast_targets().get(bucket, {}).get(month_num, 0)

    # Determine date range
    if date_from and date_to:
        try:
            d_from = datetime.date.fromisoformat(str(date_from)[:10])
            d_to = datetime.date.fromisoformat(str(date_to)[:10])
        except Exception:
            return {"total": 0, "by_month": {}}
    else:
        return {"total": 0, "by_month": {}}

    # Sum forecast for relevant months
    total = 0
    by_month = {}
    current = d_from.replace(day=1)
    while current <= d_to:
        month_num = current.month
        # Determine fraction of month in range
        month_start = current
        if current.month == 12:
            month_end = current.replace(day=31)
        else:
            month_end = (current.replace(month=current.month+1, day=1) -
                        datetime.timedelta(days=1))
        actual_start = max(d_from, month_start)
        actual_end = min(d_to, month_end)
        days_in_range = (actual_end - actual_start).days + 1
        days_in_month = (month_end - month_start).days + 1
        fraction = days_in_range / days_in_month

        # Sum across all relevant buckets
        month_total = 0
        for pl in (product_lines or []):
            month_total += month_forecast(pl, month_num) * fraction

        if month_total > 0:
            by_month[current.strftime('%Y-%m')] = round(month_total)
        total += month_total

        # Next month
        if current.month == 12:
            current = current.replace(year=current.year+1, month=1)
        else:
            current = current.replace(month=current.month+1)

    return {"total": round(total), "by_month": by_month}

PROJECT = "restor3d-data-warehouse"

DATASET = "production3_r3id_public"

def tbl(name):
    return f"`{PROJECT}.{DATASET}.{name}`"

PRODUCT_GROUPS = {
    'Upper Extremity': [
        'Anatomic Shoulder Arthroplasty','Hemiarthroplasty Shoulder',
        'Reverse Shoulder Arthroplasty - Glenoid Baseplate',
        'Reverse Shoulder Arthroplasty - Glenosphere Only',
        'Custom rTSA - Custom Glenoid + Veritas OTS Components',
        'Reverse Total Shoulder Arthroplasty',
        'Reverse Shoulder Arthroplasty - Proximal Humerus',
        'Custom rTSA - Other',
        'Acromion','Clavicle','Shoulder',
        'Hemiarthroplasty Elbow','Total Elbow Arthroplasty','Elbow Fusion',
        'Total Wrist Arthroplasty','Hand/Wrist Fusion',
        'Hemiarthroplasty Hand/Wrist','Carpal Replacement',
        'Corrective Osteotomy Hand/Wrist','Corrective Osteotomy - Arm',
        'Segmental Defect - Arm','Bone Models - Arm','Prosthetic - Arm',
    ],
    'Lower Extremity': [
        'Total Ankle Replacement','PSR','Ankle Fusion',
        'Corrective Osteotomy Ankle','Corrective Osteotomy Foot',
        'Corrective Osteotomy - Leg','Hindfoot Fusion','Midfoot','MPJ/MTP',
        'Total Talus and Other Arthroplasty','Hemitalus',
        'Antibiotic Spacer','Hemiarthroplasty Ankle','Unknown - Foot/Ankle',
        'Segmental Defect - Leg','Prosthetic - Leg',
        'Bone Models - Ankle','Bone Models - Foot',
        'Corrective Osteotomy','Prosthetic','Segmental Defect',
    ],
    'Knee': ['Total Knee Arthroplasty','TKA','Hemiarthroplasty Knee'],
    'Hip': ['Hip Arthroplasty','Hip Hemipelvis','Hip Hemiarthroplasty'],
    'Craniofacial': ['Mandible','Maxilla','General Reconstruction'],
    'Spine': ['Lumbar'],
}

CLEARED_PRODUCTS = [
    'Reverse Shoulder Arthroplasty - Glenoid Baseplate',
    'Reverse Shoulder Arthroplasty - Glenosphere Only',
    'Reverse Total Shoulder Arthroplasty',
    'Total Ankle Replacement',
]

TAR_PRODUCTS = ['Total Ankle Replacement', 'PSR']

RTSA_PRODUCTS = [
    'Reverse Total Shoulder Arthroplasty',
    'Reverse Shoulder Arthroplasty - Glenosphere Only',
    'Reverse Shoulder Arthroplasty - Glenoid Baseplate',
]

SCAN_TIMES = {
    'Corrective Osteotomy Hand/Wrist':7,'Hemiarthroplasty Hand/Wrist':7,'Carpal Replacement':7,
    'Total Wrist Arthroplasty':7,'Hand/Wrist Fusion':7,'Total Elbow Arthroplasty':7,
    'Hemiarthroplasty Elbow':7,'Elbow Fusion':7,'Clavicle':7,'Hemiarthroplasty Shoulder':7,
    'Acromion':7,'Anatomic Shoulder Arthroplasty':7,'Reverse Total Shoulder Arthroplasty':7,
    'Reverse Shoulder Arthroplasty - Glenosphere Only':7,'Reverse Shoulder Arthroplasty - Glenoid Baseplate':7,
    'Reverse Shoulder Arthroplasty - Proximal Humerus':7,'Shoulder':7,
    'Hemiarthroplasty Knee':7,'Total Knee Arthroplasty':7,'TKA':7,
    'Mandible':7,'Maxilla':7,'General Reconstruction':7,
    'Corrective Osteotomy Foot':15,'Bone Models - Foot':15,'Midfoot':15,'MPJ/MTP':15,'Hindfoot Fusion':15,
    'Corrective Osteotomy Ankle':15,'Bone Models - Ankle':15,'Ankle Fusion':15,'Antibiotic Spacer':15,
    'Hemiarthroplasty Ankle':15,'Hemitalus':15,'Unknown - Foot/Ankle':15,
    'Total Ankle Replacement':15,'Total Talus and Other Arthroplasty':15,'PSR':15,
    'Bone Models - Arm':7,'Prosthetic - Arm':7,'Segmental Defect - Arm':7,'Corrective Osteotomy - Arm':7,
    'Prosthetic - Leg':7,'Corrective Osteotomy - Leg':7,'Segmental Defect - Leg':7,
    'Corrective Osteotomy':7,'Prosthetic':7,'Segmental Defect':7,
    'Hip Hemipelvis':7,'Hip Arthroplasty':7,'Hip Hemiarthroplasty':7,
}

SEG_TIMES = {
    'Corrective Osteotomy Hand/Wrist':(240,20),'Hemiarthroplasty Hand/Wrist':(240,20),'Carpal Replacement':(240,20),
    'Total Wrist Arthroplasty':(240,20),'Hand/Wrist Fusion':(240,20),'Total Elbow Arthroplasty':(240,20),
    'Hemiarthroplasty Elbow':(240,20),'Elbow Fusion':(300,20),'Clavicle':(120,20),
    'Hemiarthroplasty Shoulder':(150,30),'Acromion':(150,30),'Anatomic Shoulder Arthroplasty':(150,30),
    'Reverse Total Shoulder Arthroplasty':(150,30),'Reverse Shoulder Arthroplasty - Glenosphere Only':(150,30),
    'Reverse Shoulder Arthroplasty - Glenoid Baseplate':(150,30),'Reverse Shoulder Arthroplasty - Proximal Humerus':(150,30),
    'Shoulder':(150,30),'Hemiarthroplasty Knee':(120,20),'Total Knee Arthroplasty':(120,20),'TKA':(120,20),
    'Mandible':(200,20),'Maxilla':(200,20),'General Reconstruction':(200,20),
    'Corrective Osteotomy Foot':(200,20),'Bone Models - Foot':(200,20),'Midfoot':(200,20),'MPJ/MTP':(200,20),
    'Hindfoot Fusion':(200,20),'Corrective Osteotomy Ankle':(200,20),'Bone Models - Ankle':(200,20),
    'Ankle Fusion':(200,20),'Antibiotic Spacer':(200,20),'Hemiarthroplasty Ankle':(200,20),
    'Hemitalus':(200,20),'Unknown - Foot/Ankle':(200,20),'Total Ankle Replacement':(150,30),
    'Total Talus and Other Arthroplasty':(150,20),'PSR':(150,30),
    'Bone Models - Arm':(200,20),'Prosthetic - Arm':(200,20),'Segmental Defect - Arm':(200,20),
    'Corrective Osteotomy - Arm':(200,20),'Prosthetic - Leg':(120,20),'Corrective Osteotomy - Leg':(120,20),
    'Segmental Defect - Leg':(120,20),'Corrective Osteotomy':(120,20),'Prosthetic':(120,20),
    'Segmental Defect':(120,20),'Hip Hemipelvis':(120,20),'Hip Arthroplasty':(120,20),'Hip Hemiarthroplasty':(120,20),
}

STEP_TIMES = {
    'Planning':     {'TAR':(150,60), 'rTSA':(330,60)},
    'Design':       {'TAR':(150,60), 'rTSA':(240,60)},
    'Jigs Design':  {'rTSA':(240,60)},  # Alias — same as Design for rTSA
    'Peer Review':  {'TAR':(180,60)},   # Worker=180 min, Reviewer=60 min (from PSR pbit)
    'Proposed Surgical Plan': {'TAR':(90,30), 'rTSA':(120,30)},
    'Shipping':     {'TAR':(180,60)},  # Bone Model — TAR only
}

MINUTES_PER_DAY = 480

def get_product_group(product):
    """Map a case_category_name to a product group for design time lookup.
    (Only used by the fallback path now — see get_design_time().)"""
    if product in TAR_PRODUCTS:
        return 'TAR'
    if product in RTSA_PRODUCTS:
        return 'rTSA'
    return None

DATABASE_INDEX_DESIGN_TIMES_GID = "1507030841"

_design_times_cache = {"data": None, "fetched_at": None}

_DESIGN_TIMES_CACHE_TTL_SECONDS = 600  # 10 minutes

def get_design_times_live(force_refresh=False):
    """Fetches the live 'Design Times' tab and returns
    {(product, step_name): (design_min, review_min_or_None)}.
    Only System == 'BigQuery' rows are used here — Camstar design times (once
    added to this same tab) are a separate lookup path, not wired up yet.
    Cached in memory for _DESIGN_TIMES_CACHE_TTL_SECONDS."""
    now = datetime.datetime.utcnow()
    cached = _design_times_cache["data"]
    fetched_at = _design_times_cache["fetched_at"]
    if (not force_refresh and cached is not None and fetched_at is not None
            and (now - fetched_at).total_seconds() < _DESIGN_TIMES_CACHE_TTL_SECONDS):
        return cached

    csv_url = (f"https://docs.google.com/spreadsheets/d/{DATABASE_INDEX_SHEET_ID}"
               f"/export?format=csv&gid={DATABASE_INDEX_DESIGN_TIMES_GID}")
    try:
        df = pd.read_csv(csv_url)
    except Exception as e:
        logger.error(f"Failed to fetch Design Times sheet: {e}")
        return cached  # None triggers the hardcoded fallback in get_design_time()

    df.columns = [str(c).strip() for c in df.columns]
    result = {}
    for _, row in df.iterrows():
        system = str(row.get("System", "")).strip()
        if system != "BigQuery":
            continue
        product = str(row.get("Product (case_category_name)", "")).strip()
        step = str(row.get("Process/Step", "")).strip()
        if not product or not step or product.lower() == 'nan' or step.lower() == 'nan':
            continue
        design_val = row.get("Design", None)
        review_val = row.get("Review", None)
        design_min = float(design_val) if pd.notna(design_val) else None
        review_min = float(review_val) if pd.notna(review_val) else None
        result[(product, step)] = (design_min, review_min)

    _design_times_cache["data"] = result
    _design_times_cache["fetched_at"] = now
    return result

@app.route("/analytics/design-times", methods=["GET"])
def design_times_debug():
    """Debug/inspection endpoint — shows the live-loaded design times."""
    try:
        force_refresh = request.args.get("refresh", "").lower() == "true"
        live = get_design_times_live(force_refresh=force_refresh)
        if live is None:
            return jsonify({"error": "Live sheet unavailable, using hardcoded fallback", "live": False})
        return jsonify({"live": True, "count": len(live),
                         "data": {f"{p} | {s}": {"design": d, "review": r} for (p, s), (d, r) in live.items()}})
    except Exception as e:
        return handle_error(request.endpoint, e)

def get_design_time(step_name, user_type, product, multiplier=1):
    """Return standard design time in minutes for a step+role+product combo, or None.
    `multiplier` (1, 2, or 4) is applied ONLY to Scan Assessment and Segmentation —
    both WORKER and REVIEWER minutes — per the bilateral/revision case-complexity rule.
    Planning/Design/PSP/Peer Review/Shipping are unaffected.

    Reads live from the Design Times sheet first; falls back to the hardcoded
    SCAN_TIMES/SEG_TIMES/STEP_TIMES dicts only if the live sheet is unreachable.
    """
    live = get_design_times_live()
    if live is not None:
        design_min, review_min = live.get((product, step_name), (None, None))
        base = design_min if user_type == 'WORKER' else review_min
        if base is None:
            return None
        if step_name in ('Scan Assessment', 'Segmentation'):
            return base * multiplier
        return base

    # ── Fallback path (live sheet unreachable) ──
    if step_name == 'Scan Assessment':
        base = SCAN_TIMES.get(product) if user_type == 'WORKER' else None
        return base * multiplier if base is not None else None
    if step_name == 'Segmentation':
        times = SEG_TIMES.get(product)
        if times:
            base = times[0] if user_type == 'WORKER' else times[1]
            return base * multiplier
        return None
    if step_name in STEP_TIMES:
        pg = get_product_group(product)
        if pg and pg in STEP_TIMES[step_name]:
            times = STEP_TIMES[step_name][pg]
            return times[0] if user_type == 'WORKER' else times[1]
    return None

def get_case_time_multiplier(laterality, preoperative_state, proposed_indication, design_notes):
    """Bilateral case -> 2x, revision case -> 2x, both -> 4x, neither -> 1x.
    Revision = preoperativeState == 'REVISION_OTHER_SYSTEM' OR the word "revision"
    appears in proposedIndication or designNotes (case-insensitive).
    Applies to all BigQuery case types (not Camstar — this data doesn't exist there).
    """
    is_bilateral = (laterality or '').strip().upper() == 'BILATERAL'
    is_revision = (
        (preoperative_state or '') == 'REVISION_OTHER_SYSTEM'
        or 'revision' in (proposed_indication or '').lower()
        or 'revision' in (design_notes or '').lower()
    )
    m = 1
    if is_bilateral:
        m *= 2
    if is_revision:
        m *= 2
    return m

METRIC_MAP = {
    'total_lt':   ("TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0)", "Total LT", "s.ship_wrk_comp_date"),
    'seg_lt':     ("TIMESTAMP_DIFF(sd.seg_review_date, s.first_scan_upload_date, DAY)", "Segmentation LT", "sd.seg_review_date"),
    'surgeon_lt': ("TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY)", "Surgeon Approval LT", "sd.surgeon_approval_date"),
    'digital_lt': ("GREATEST(0, LEAST(TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0) - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)), COALESCE(TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0), TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0))))", "Digital Production LT", "sd.first_psp_review_date"),
    'volume':     ("COUNT(DISTINCT f.id)", "Volume", "sd.first_psp_review_date"),
}

def on_hold_cte():
    return f"""
    on_hold_time AS (
        SELECT refId as caseId, SUM(hold_days) as total_hold_days
        FROM (
            SELECT refId,
                CASE
                    WHEN type = 'r3idCaseRemovedFromOnHold'
                        THEN TIMESTAMP_DIFF(CAST(timestamp AS DATETIME), CAST(LAG(timestamp) OVER (PARTITION BY refId ORDER BY timestamp) AS DATETIME), DAY)
                    WHEN type = 'r3idCasePutOnHold'
                        AND LEAD(type) OVER (PARTITION BY refId ORDER BY timestamp) IS NULL
                        THEN TIMESTAMP_DIFF(CAST(CURRENT_TIMESTAMP() AS DATETIME), CAST(timestamp AS DATETIME), DAY)
                    ELSE 0
                END as hold_days
            FROM {tbl('vw_event_log_rank')}
            WHERE type IN ('r3idCasePutOnHold','r3idCaseRemovedFromOnHold')
        )
        GROUP BY refId
    )"""

def on_hold_user_cte():
    return f"""
    on_hold_user AS (
        SELECT refId as caseId, userId as put_on_hold_by, timestamp as put_on_hold_at
        FROM (
            SELECT refId, userId, timestamp,
                ROW_NUMBER() OVER (PARTITION BY refId ORDER BY timestamp DESC) as rn
            FROM {tbl('vw_event_log_rank')}
            WHERE type = 'r3idCasePutOnHold'
        )
        WHERE rn = 1
    )"""

def signoff_cte():
    return f"""
    signoff_dates AS (
        SELECT
            w.caseId,
            MIN(CASE WHEN wm.name = 'Segmentation' AND w.workModuleUserType = 'REVIEWER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as seg_review_date,
            MIN(CASE WHEN wm.name = 'Design Call Prep' AND w.workModuleUserType = 'REVIEWER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as design_call_prep_date,
            MAX(CASE WHEN wm.name = 'Proposed Surgical Plan' AND w.workModuleUserType = 'APPROVER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as surgeon_approval_date,
            MAX(CASE WHEN wm.name = 'Proposed Surgical Plan' AND w.workModuleUserType = 'REVIEWER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as first_psp_review_date,
            MIN(CASE WHEN wm.name = 'Peer Review' AND w.workModuleUserType = 'REVIEWER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as peer_review_date,
            -- True if the most recent PSP APPROVER signoff is a REJECT (surgeon rejected, case sent back for rework).
            -- When true the case should NOT sit in surgeon_approval — it belongs back in psp_design.
            MAX(CASE WHEN wm.name = 'Proposed Surgical Plan' AND w.workModuleUserType = 'APPROVER' AND w.workModuleSignatureType = 'REJECT' THEN w.createdAt END) > MAX(CASE WHEN wm.name = 'Proposed Surgical Plan' AND w.workModuleUserType = 'APPROVER' AND w.workModuleSignatureType = 'ACCEPT' THEN w.createdAt END) as surgeon_last_action_reject
        FROM {tbl('WorkModuleSignoff')} w
        JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
        JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
        WHERE w.deleted = false
        GROUP BY w.caseId
    )"""

def case_select_fields(include_hold_user=False):
    hold_cols = ", hu.put_on_hold_by, hu.put_on_hold_at" if include_hold_user else ""
    return f"""
        f.id, CONCAT(f.count, '_', f.alias) as alias, f.alias as alias_short, f.count as case_number, f.case_category_name, f.phase,
        f.phy_nameFirst, f.phy_nameLast, f.fac_name, f.fac_state,
        f.laterality, f.onHold, f.createdAt,
        s.first_scan_upload_date, s.ship_wrk_comp_date,
        sd.seg_review_date, sd.first_psp_review_date, sd.surgeon_approval_date,
        COALESCE(oh.total_hold_days, 0) as onhold_days{hold_cols},
        CASE WHEN s.ship_wrk_comp_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
            ELSE NULL END as total_lt,
        CASE WHEN sd.seg_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.seg_review_date, s.first_scan_upload_date, DAY)
            ELSE NULL END as seg_lt,
        CASE WHEN sd.surgeon_approval_date IS NOT NULL AND sd.first_psp_review_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY)
            ELSE NULL END as surgeon_lt,
        CASE WHEN sd.peer_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN GREATEST(0, LEAST(
                TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY)
                    - COALESCE(oh.total_hold_days, 0)
                    - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)),
                COALESCE(
                    TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0),
                    TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
                )
            ))
            ELSE NULL END as digital_lt,
        f.onHoldComment as oh_note,
        cn.last_case_note, cn.last_case_note_author, cn.last_case_note_date,
        cn.last_internal_case_note, cn.last_internal_case_note_author, cn.last_internal_case_note_date,
        cn.last_design_case_note, cn.last_design_case_note_author, cn.last_design_case_note_date"""

def case_joins(include_hold_user=False):
    hold_join = f"\n        LEFT JOIN on_hold_user hu ON f.id = hu.caseId" if include_hold_user else ""
    return f"""
        FROM {tbl('vw_fact_case')} f
        LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON f.id = s.caseId
        LEFT JOIN signoff_dates sd ON f.id = sd.caseId
        LEFT JOIN on_hold_time oh ON f.id = oh.caseId
        LEFT JOIN {tbl('vw_lkup_last_case_notes')} cn ON f.id = cn.caseid{hold_join}"""

def fetch_cases_by_ids_query(case_ids, include_hold_user=False):
    """Return a query that fetches full case details for the given IDs.
    Uses raw Case table LEFT JOIN vw_fact_case so COMPLETED phase cases are included."""
    hold_join = f"\n        LEFT JOIN on_hold_user hu ON f.id = hu.caseId" if include_hold_user else ""
    hold_cols = ", hu.put_on_hold_by, hu.put_on_hold_at" if include_hold_user else ""
    ids_str = "','".join(str(i) for i in case_ids)
    return f"""
    WITH {signoff_cte()}, {on_hold_cte()}{',' + on_hold_user_cte() if include_hold_user else ''}
    SELECT
        base.id,
        COALESCE(CONCAT(f.count, '_', f.alias), CAST(base.id AS STRING)) as alias,
        COALESCE(f.alias, CAST(base.id AS STRING)) as alias_short,
        f.count as case_number,
        COALESCE(f.case_category_name, cc.name) as case_category_name,
        COALESCE(f.phase, base.phase) as phase,
        f.phy_nameFirst, f.phy_nameLast, f.fac_name, f.fac_state,
        COALESCE(f.laterality, base.laterality) as laterality,
        COALESCE(f.onHold, base.onHold) as onHold,
        COALESCE(f.createdAt, base.createdAt) as createdAt,
        s.first_scan_upload_date, s.ship_wrk_comp_date,
        sd.seg_review_date, sd.first_psp_review_date, sd.surgeon_approval_date,
        COALESCE(oh.total_hold_days, 0) as onhold_days{hold_cols},
        CASE WHEN s.ship_wrk_comp_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
            ELSE NULL END as total_lt,
        CASE WHEN sd.seg_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.seg_review_date, s.first_scan_upload_date, DAY)
            ELSE NULL END as seg_lt,
        CASE WHEN sd.surgeon_approval_date IS NOT NULL AND sd.first_psp_review_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY)
            ELSE NULL END as surgeon_lt,
        CASE WHEN sd.peer_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN GREATEST(0, LEAST(
                TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY)
                    - COALESCE(oh.total_hold_days, 0)
                    - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)),
                COALESCE(
                    TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0),
                    TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
                )
            ))
            ELSE NULL END as digital_lt,
        f.onHoldComment as oh_note,
        cn.last_case_note, cn.last_case_note_author, cn.last_case_note_date,
        cn.last_internal_case_note, cn.last_internal_case_note_author, cn.last_internal_case_note_date,
        cn.last_design_case_note, cn.last_design_case_note_author, cn.last_design_case_note_date
    FROM {tbl('Case')} base
    JOIN {tbl('CaseCategory')} cc ON base.caseCategoryId = cc.id
    LEFT JOIN {tbl('vw_fact_case')} f ON base.id = f.id
    LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON base.id = s.caseId
    LEFT JOIN signoff_dates sd ON base.id = sd.caseId
    LEFT JOIN on_hold_time oh ON base.id = oh.caseId
    LEFT JOIN {tbl('vw_lkup_last_case_notes')} cn ON base.id = cn.caseid{hold_join}
    WHERE base.id IN ('{ids_str}')
    ORDER BY base.createdAt DESC
    LIMIT 500
    """

def volume_case_joins():
    """For volume queries: bypass vw_fact_case (which excludes COMPLETED cases)
    and query raw Case table so completed cases are counted too."""
    return f"""
        FROM {tbl('Case')} f
        LEFT JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
        LEFT JOIN {tbl('CaseType')} ct ON f.caseTypeId = ct.id
        LEFT JOIN signoff_dates sd ON f.id = sd.caseId
        LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON f.id = s.caseId"""

def volume_case_joins_full():
    """Same population as volume_case_joins() (raw Case table, no Case_In_Take restriction,
    includes COMPLETED cases) but also joins vw_fact_case for denormalized display fields
    (surgeon, facility, etc.) plus on_hold_time and case-notes — so this can back a full
    case-list response (case_select_fields-equivalent), not just chart aggregation."""
    return f"""
        FROM {tbl('Case')} f
        LEFT JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
        LEFT JOIN {tbl('CaseType')} ct ON f.caseTypeId = ct.id
        LEFT JOIN signoff_dates sd ON f.id = sd.caseId
        LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON f.id = s.caseId
        LEFT JOIN {tbl('vw_fact_case')} vf ON f.id = vf.id
        LEFT JOIN on_hold_time oh ON f.id = oh.caseId
        LEFT JOIN {tbl('vw_lkup_last_case_notes')} cn ON f.id = cn.caseid"""

def volume_case_select_fields():
    """case_select_fields()-equivalent for the raw-Case-table (volume) population —
    denormalized display fields pulled from vw_fact_case (vf) with a fallback to the
    raw Case/CaseCategory columns (f/cc) for cases vw_fact_case excludes (COMPLETED)."""
    return f"""
        f.id, CONCAT(f.count, '_', COALESCE(vf.alias, CAST(f.id AS STRING))) as alias,
        COALESCE(vf.alias, CAST(f.id AS STRING)) as alias_short, f.count as case_number,
        COALESCE(vf.case_category_name, cc.name) as case_category_name,
        COALESCE(vf.phase, f.phase) as phase,
        vf.phy_nameFirst, vf.phy_nameLast, vf.fac_name, vf.fac_state,
        COALESCE(vf.laterality, f.laterality) as laterality,
        COALESCE(vf.onHold, f.onHold) as onHold,
        COALESCE(vf.createdAt, f.createdAt) as createdAt,
        s.first_scan_upload_date, s.ship_wrk_comp_date,
        sd.seg_review_date, sd.first_psp_review_date, sd.surgeon_approval_date,
        COALESCE(oh.total_hold_days, 0) as onhold_days,
        CASE WHEN s.ship_wrk_comp_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
            ELSE NULL END as total_lt,
        CASE WHEN sd.seg_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.seg_review_date, s.first_scan_upload_date, DAY)
            ELSE NULL END as seg_lt,
        CASE WHEN sd.surgeon_approval_date IS NOT NULL AND sd.first_psp_review_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY)
            ELSE NULL END as surgeon_lt,
        CASE WHEN sd.peer_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN GREATEST(0, LEAST(
                TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY)
                    - COALESCE(oh.total_hold_days, 0)
                    - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)),
                COALESCE(
                    TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0),
                    TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
                )
            ))
            ELSE NULL END as digital_lt,
        COALESCE(vf.onHoldComment, f.onHoldComment) as oh_note,
        cn.last_case_note, cn.last_case_note_author, cn.last_case_note_date,
        cn.last_internal_case_note, cn.last_internal_case_note_author, cn.last_internal_case_note_date,
        cn.last_design_case_note, cn.last_design_case_note_author, cn.last_design_case_note_date"""

def volume_where_clause(args):
    """Build WHERE clause for volume using raw Case table columns.
    No phase filter — includes COMPLETED cases for accurate volume counting.
    Includes cancelled cases — PSP completion is the milestone; post-PSP cancellation should not affect volume."""
    conditions = ["f.deleted = false"]
    # Product filter — use cc.name (CaseCategory.name) instead of f.case_category_name
    product = args.get('product', '').strip()
    product_group = args.get('product_group', '').strip()
    if product:
        products = [p.strip() for p in product.split(',')]
        joined = "','".join(products)
        conditions.append(f"cc.name IN ('{joined}')" if len(products) > 1 else f"cc.name = '{products[0]}'")
    elif product_group:
        all_prods = []
        for g in [g.strip() for g in product_group.split(',')]:
            all_prods.extend(PRODUCT_GROUPS.get(g, []))
        if all_prods:
            joined = "','".join(all_prods)
            conditions.append(f"cc.name IN ('{joined}')")
    # No product selected — no default filter, returns all products
    # Surgeon filter
    surgeon = args.get('surgeon', '').strip()
    if surgeon:
        conditions.append(f"LOWER(f.alias) LIKE LOWER('%{sanitize(surgeon)}%')")
    # Case type filter
    case_type = args.get('case_type', '').strip()
    if case_type:
        types = [t.strip() for t in case_type.split(',')]
        joined = "','".join(types)
        conditions.append(f"ct.name IN ('{joined}')" if len(types) > 1 else f"ct.name = '{types[0]}'")
    # Step + User filter via WorkModuleSignoff subquery (mirrors build_where_clause logic
    # so the User dropdown and the Designed/Reviewed toggle apply to Volume too).
    step_val = args.get('step', '').strip()
    user_val = args.get('step_user', '').strip()
    if step_val or user_val:
        sub = ["w_flt.deleted = false", "w_flt.workModuleSignatureType = 'ACCEPT'"]
        if step_val:
            steps = [s.strip() for s in step_val.split(',')]
            if len(steps) == 1:
                sub.append(f"wm_flt.name = '{steps[0]}'")
            else:
                joined = "','".join(steps)
                sub.append(f"wm_flt.name IN ('{joined}')")
        if user_val:
            users = [u.strip() for u in user_val.split(',')]
            if len(users) == 1:
                sub.append(f"w_flt.signature = '{users[0]}'")
            else:
                joined = "','".join(users)
                sub.append(f"w_flt.signature IN ('{joined}')")
            user_role = args.get('user_role', '').strip().lower()
            if user_role == 'worker':
                sub.append("w_flt.workModuleUserType = 'WORKER'")
            elif user_role == 'reviewer':
                sub.append("w_flt.workModuleUserType IN ('REVIEWER', 'APPROVER')")
        sub_where = " AND ".join(sub)
        conditions.append(f"""f.id IN (
            SELECT DISTINCT w_flt.caseId
            FROM `{PROJECT}.{DATASET}.WorkModuleSignoff` w_flt
            LEFT JOIN `{PROJECT}.{DATASET}.WorkModuleInstance` wi_flt ON w_flt.workModuleInstanceId = wi_flt.id
            LEFT JOIN `{PROJECT}.{DATASET}.WorkModule` wm_flt ON wi_flt.workModuleId = wm_flt.id
            WHERE {sub_where}
        )""")
    return " AND ".join(conditions)

def format_cases(df):
    return df.astype(str).replace('nan','').replace('NaT','').replace('None','').replace('<NA>','').to_dict(orient='records')

def shorten_product(s):
    return (str(s)
        .replace('Reverse Total Shoulder Arthroplasty','RTSA')
        .replace('Total Ankle Replacement','TAR')
        .replace('Total Knee Arthroplasty','TKA')
        .replace('Total Talus and Other Arthroplasty','Total Talus')
        .replace('Hip Arthroplasty','Hip'))

def build_where_clause(args, alias='f'):
    conditions = [f"{alias}.deleted = false", f"{alias}.Case_In_Take IN ('Case Intake','Restor3d MedEd','Cody Case Intake')"]
    if args.get('product'):
        products = [p.strip() for p in args.get('product').split(',')]
        joined = "','".join(products)
        if len(products) == 1:
            conditions.append(f"{alias}.case_category_name = '{products[0]}'")
        else:
            conditions.append(f"{alias}.case_category_name IN ('{joined}')")
    if args.get('product_group'):
        all_products = []
        for g in [g.strip() for g in args.get('product_group').split(',')]:
            all_products.extend(PRODUCT_GROUPS.get(g, []))
        if all_products:
            joined = "','".join(all_products)
            conditions.append(f"{alias}.case_category_name IN ('{joined}')")
    if args.get('joint'):
        joints = [j.strip() for j in args.get('joint').split(',')]
        joined = "','".join(joints)
        conditions.append(f"{alias}.anatomy_name IN ('{joined}')")
    if args.get('regulatory'):
        regs = [r.strip() for r in args.get('regulatory').split(',')]
        joined = "','".join(CLEARED_PRODUCTS)
        if 'Cleared' in regs and 'Custom' not in regs:
            conditions.append(f"{alias}.case_category_name IN ('{joined}')")
        elif 'Custom' in regs and 'Cleared' not in regs:
            conditions.append(f"{alias}.case_category_name NOT IN ('{joined}')")
    if args.get('surgeon'):
        conditions.append(f"LOWER({alias}.phy_nameLast) LIKE LOWER('%{sanitize(args.get('surgeon'))}%')")
    if args.get('laterality'):
        lats = [l.strip() for l in args.get('laterality').split(',')]
        joined = "','".join(lats)
        conditions.append(f"{alias}.laterality IN ('{joined}')")
    if args.get('facility'):
        conditions.append(f"LOWER({alias}.fac_name) LIKE LOWER('%{sanitize(args.get('facility'))}%')")
    if args.get('state'):
        conditions.append(f"UPPER({alias}.fac_state) = UPPER('{sanitize(args.get('state'))}')")
    for flag, field in {'surgery_scheduled':'isSurgeryScheduled','oncology':'isOncologyCase','trauma':'isTraumaCase','pediatric':'isPediatricCase','preop':'preoperativePlanningOnly','research':'researchCase'}.items():
        if args.get(flag) == 'true':
            conditions.append(f"{alias}.{field} = true")
    # Case type filter
    case_type = args.get('case_type', '').strip()
    if case_type:
        types = [t.strip() for t in case_type.split(',')]
        if len(types) == 1:
            conditions.append(f"{alias}.case_type_name = '{types[0]}'")
        else:
            joined = "','".join(types)
            conditions.append(f"{alias}.case_type_name IN ('{joined}')")
    # Shoulder/ankle case-type selection now goes through the generic case_type filter above —
    # no per-product grouping dicts needed.
    # Step + User filter via WorkModuleSignoff subquery
    step_val = args.get('step', '').strip()
    user_val = args.get('step_user', '').strip()
    if step_val or user_val:
        sub = ["w_flt.deleted = false", "w_flt.workModuleSignatureType = 'ACCEPT'"]
        if step_val:
            steps = [s.strip() for s in step_val.split(',')]
            if len(steps) == 1:
                sub.append(f"wm_flt.name = '{steps[0]}'")
            else:
                joined = "','".join(steps)
                sub.append(f"wm_flt.name IN ('{joined}')")
        if user_val:
            users = [u.strip() for u in user_val.split(',')]
            if len(users) == 1:
                sub.append(f"w_flt.signature = '{users[0]}'")
            else:
                joined = "','".join(users)
                sub.append(f"w_flt.signature IN ('{joined}')")
        sub_where = " AND ".join(sub)
        conditions.append(f"""{alias}.id IN (
            SELECT DISTINCT w_flt.caseId
            FROM `{PROJECT}.{DATASET}.WorkModuleSignoff` w_flt
            LEFT JOIN `{PROJECT}.{DATASET}.WorkModuleInstance` wi_flt ON w_flt.workModuleInstanceId = wi_flt.id
            LEFT JOIN `{PROJECT}.{DATASET}.WorkModule` wm_flt ON wi_flt.workModuleId = wm_flt.id
            WHERE {sub_where}
        )""")
    return " AND ".join(conditions)

def sanitize(value: str) -> str:
    """Strip characters that could be used for SQL injection in LIKE/equality clauses."""
    return re.sub(r"['\";\\]", "", value).strip()

_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

def validate_date(value: str) -> str:
    """Return value if it matches YYYY-MM-DD, otherwise return empty string."""
    v = (value or '').strip()
    return v if _DATE_RE.match(v) else ''

def date_range_filter(date_field, date_from, date_to):
    date_from = validate_date(date_from)
    date_to   = validate_date(date_to)
    conds = [f"{date_field} IS NOT NULL"]
    if date_from:
        conds.append(f"CAST({date_field} AS DATETIME) >= CAST('{date_from}' AS DATETIME)")
    if date_to:
        conds.append(f"CAST({date_field} AS DATETIME) <= CAST('{date_to} 23:59:59' AS DATETIME)")
    return " AND ".join(conds)

def outlier_exclusion_sql(avg_digital_lt=None):
    parts = ["(s.ship_wrk_comp_date IS NULL OR TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0) <= 50)"]
    if avg_digital_lt and avg_digital_lt > 0:
        threshold = round(avg_digital_lt * 2, 1)
        parts.append(f"""(sd.peer_review_date IS NULL OR
            TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0)
            - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)) <= {threshold})""")
    return " AND ".join(parts)

@app.route("/config", methods=["GET"])
def config():
    try:
        # Get live product counts from BigQuery (all time)
        # Pull products directly from live BigQuery — no static list, no duplicates
        df = client.query(f"""
            SELECT case_category_name, COUNT(*) as case_count
            FROM {tbl('vw_fact_case')}
            WHERE deleted = false AND case_category_name IS NOT NULL
            GROUP BY case_category_name ORDER BY case_count DESC
        """).to_dataframe()
        live_products = df['case_category_name'].tolist()  # already sorted by count

        # Build product groups — only include products that actually exist in BigQuery
        live_set = set(live_products)
        groups = {}
        for g, prods in PRODUCT_GROUPS.items():
            matched = [p for p in prods if p in live_set]
            if matched:
                groups[g] = matched

        return jsonify({
            "products": live_products,
            "product_groups": groups,
            "cleared": [p for p in live_products if p in CLEARED_PRODUCTS],
            "custom": [p for p in live_products if p not in CLEARED_PRODUCTS],
            "live_products": live_products,
        })
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/forecast", methods=["GET"])
def forecast():
    """Return forecast targets for selected products/date range."""
    try:
        args = request.args
        date_from = args.get('date_from', '').strip()
        date_to   = args.get('date_to', '').strip()
        product   = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()
        case_type = args.get('case_type', '').strip()
        source    = args.get('source', 'both')  # 'bigquery', 'camstar', or 'both'

        # Determine which forecast buckets to sum.
        # Bucket keys are flat — either a case_category_name (product) or a
        # case_type_name (specific case type) — matched directly, no grouping dicts.
        forecast_targets = get_forecast_targets()
        buckets = []
        if product:
            prods = [p.strip() for p in product.split(',')]
            for p in prods:
                if p in forecast_targets:
                    buckets.append(p)
        elif product_group:
            groups = [g.strip() for g in product_group.split(',')]
            for g in groups:
                for p in PRODUCT_GROUPS.get(g, []):
                    if p in forecast_targets and p not in buckets:
                        buckets.append(p)
                # Also add CamStar products for this group
                for p in CAMSTAR_PRODUCT_GROUPS.get(g, []):
                    if p in forecast_targets and p not in buckets:
                        buckets.append(p)
        elif case_type:
            # Case type wasn't matched at all before — any case_type-only filter
            # (with no product/product_group set) fell through to the "sum every
            # bucket" branch below, which is what produced the inflated RTSA line.
            types = [t.strip() for t in case_type.split(',')]
            for t in types:
                if t in forecast_targets:
                    buckets.append(t)
        else:
            buckets = list(forecast_targets.keys())

        result = get_forecast(buckets, date_from=date_from, date_to=date_to)
        result['buckets'] = buckets
        return jsonify(result)
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/case-types", methods=["GET"])
def case_types():
    """Return distinct case_type_name values for the case type filter dropdown."""
    try:
        args = request.args
        product = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()

        conditions = ["f.deleted = false", "f.case_type_name IS NOT NULL"]
        if product:
            prods = [p.strip() for p in product.split(',')]
            joined = "','".join(prods)
            conditions.append(f"f.case_category_name IN ('{joined}')")
        elif product_group:
            all_prods = []
            for g in [g.strip() for g in product_group.split(',')]:
                all_prods.extend(PRODUCT_GROUPS.get(g, []))
            if all_prods:
                joined = "','".join(all_prods)
                conditions.append(f"f.case_category_name IN ('{joined}')")

        where_sql = " AND ".join(conditions)
        query = f"""
            SELECT f.case_type_name, COUNT(*) as case_count
            FROM {tbl('vw_fact_case')} f
            WHERE {where_sql}
            GROUP BY f.case_type_name
            ORDER BY case_count DESC
        """
        rows = client.query(query).to_dataframe().astype(str).to_dict(orient="records")
        return jsonify({"case_types": rows})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/all-products")
def all_products():
    """Returns all distinct case_category_name values ever, with case counts."""
    try:
        return jsonify(client.query(f"""
            SELECT case_category_name, COUNT(*) as case_count
            FROM {tbl('vw_fact_case')}
            WHERE deleted = false AND case_category_name IS NOT NULL
            GROUP BY case_category_name ORDER BY case_count DESC
        """).to_dataframe().astype(str).to_dict(orient="records"))
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/work-module-step-names", methods=["GET"])
def work_module_step_names():
    """Returns distinct WorkModule.name values (the actual step names, not workflow templates)"""
    try:
        return jsonify(client.query(f"""
            SELECT wm.name as step_name, COUNT(DISTINCT w.caseId) as case_count
            FROM {tbl('WorkModuleSignoff')} w
            LEFT JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
            LEFT JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
            WHERE w.deleted = false AND wm.name IS NOT NULL
            GROUP BY wm.name ORDER BY case_count DESC
        """).to_dataframe().astype(str).to_dict(orient="records"))
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/work-module-names", methods=["GET"])
def work_module_names():
    try:
        return jsonify(client.query(f"SELECT DISTINCT wi.name, wi.casePhase, wi.milestoneEvent, COUNT(*) as signoff_count FROM {tbl('WorkModuleSignoff')} w LEFT JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id WHERE w.deleted = false AND wi.name IS NOT NULL GROUP BY wi.name, wi.casePhase, wi.milestoneEvent ORDER BY wi.casePhase, wi.name").to_dataframe().astype(str).to_dict(orient="records"))
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/lead-time-views", methods=["GET"])
def lead_time_views():
    try:
        result = {}
        for view in ["vw_lkp_lead_time_diff","vw_lkup_lead_time_values","STG_CTRL_R3ID_LEAD_TIME_VAR"]:
            df = client.query(f"SELECT * FROM {tbl(view)} LIMIT 2").to_dataframe().astype(str)
            result[view] = {"columns": list(df.columns), "sample": df.to_dict(orient="records")}
        return jsonify(result)
    except Exception as e:
        return handle_error(request.endpoint, e)

def kpi_or_cases(args, date_field, metric_sql, fetch_cases=False, include_hold_user=False, exclude_outliers=False, avg_digital_lt=None):
    where = build_where_clause(args)
    df_filter = date_range_filter(date_field, args.get('date_from',''), args.get('date_to',''))
    if exclude_outliers:
        where = f"{where} AND {outlier_exclusion_sql(avg_digital_lt)}"
    if fetch_cases:
        hold_cte_str = f", {on_hold_user_cte()}" if include_hold_user else ""
        query = f"""
        WITH {signoff_cte()}, {on_hold_cte()}{hold_cte_str}
        SELECT {case_select_fields(include_hold_user)}
        {case_joins(include_hold_user)}
        WHERE {where} AND {df_filter}
        ORDER BY f.createdAt DESC LIMIT 500
        """
        df = client.query(query).to_dataframe()
        return jsonify({"cases": format_cases(df), "count": len(df)})
    else:
        query = f"""
        WITH {signoff_cte()}, {on_hold_cte()}
        SELECT COUNT(DISTINCT f.id) as case_count, ROUND(AVG({metric_sql}), 1) as avg_lt
        {case_joins()}
        WHERE {where} AND {df_filter}
        """
        df = client.query(query).to_dataframe().fillna(0)
        row = df.iloc[0]
        return jsonify({"case_count": int(row.get('case_count', 0)), "avg_lt": round(float(row.get('avg_lt', 0)), 1)})

@app.route("/analytics/kpi/total", methods=["GET"])
def kpi_total():
    try:
        args = request.args
        return kpi_or_cases(args, 's.ship_wrk_comp_date', METRIC_MAP['total_lt'][0],
            args.get('fetch_cases')=='true', exclude_outliers=args.get('exclude_outliers')=='true',
            avg_digital_lt=float(args.get('avg_digital_lt',0)))
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/kpi/seg", methods=["GET"])
def kpi_seg():
    try:
        args = request.args
        return kpi_or_cases(args, 'sd.seg_review_date', METRIC_MAP['seg_lt'][0],
            args.get('fetch_cases')=='true', exclude_outliers=args.get('exclude_outliers')=='true',
            avg_digital_lt=float(args.get('avg_digital_lt',0)))
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/kpi/surgeon", methods=["GET"])
def kpi_surgeon():
    try:
        args = request.args
        return kpi_or_cases(args, 'sd.surgeon_approval_date', METRIC_MAP['surgeon_lt'][0],
            args.get('fetch_cases')=='true', exclude_outliers=args.get('exclude_outliers')=='true',
            avg_digital_lt=float(args.get('avg_digital_lt',0)))
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/kpi/digital", methods=["GET"])
def kpi_digital():
    try:
        args = request.args
        return kpi_or_cases(args, 'sd.first_psp_review_date', METRIC_MAP['digital_lt'][0],
            args.get('fetch_cases')=='true', exclude_outliers=args.get('exclude_outliers')=='true',
            avg_digital_lt=float(args.get('avg_digital_lt',0)))
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/kpi/volume", methods=["GET"])
def kpi_volume():
    try:
        args = request.args
        fetch = args.get('fetch_cases') == 'true'
        where = volume_where_clause(args)
        # Filter by PSP date only — matches process counts PSP definition exactly
        df_filter = date_range_filter('CAST(sd.first_psp_review_date AS DATETIME)', args.get('date_from',''), args.get('date_to',''))
        if fetch:
            where_vw = build_where_clause(args)
            # Get matching case IDs first using same logic as count
            id_query = f"""
            WITH {signoff_cte()}
            SELECT DISTINCT f.id as caseId
            {volume_case_joins()}
            WHERE {where}
              AND sd.first_psp_review_date IS NOT NULL
              AND {df_filter}
            LIMIT 500
            """
            id_df = client.query(id_query).to_dataframe()
            case_ids = id_df['caseId'].tolist()
            if not case_ids:
                return jsonify({"cases": [], "count": 0})
            case_query = fetch_cases_by_ids_query(case_ids)
            df = client.query(case_query).to_dataframe()
            return jsonify({"cases": format_cases(df), "count": len(df)})
        else:
            query = f"""
            WITH {signoff_cte()}
            SELECT COUNT(DISTINCT f.id) as case_count
            {volume_case_joins()}
            WHERE {where}
              AND sd.first_psp_review_date IS NOT NULL
              AND {df_filter}
            """
            df = client.query(query).to_dataframe().fillna(0)
            return jsonify({"case_count": int(df.iloc[0].get('case_count', 0))})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/kpi/onhold", methods=["GET"])
def kpi_onhold():
    try:
        args = request.args
        fetch = args.get('fetch_cases') == 'true'
        where = build_where_clause(args) + " AND f.onHold = true"
        if fetch:
            query = f"WITH {signoff_cte()}, {on_hold_cte()}, {on_hold_user_cte()} SELECT {case_select_fields(True)} {case_joins(True)} WHERE {where} ORDER BY oh.total_hold_days DESC LIMIT 500"
            df = client.query(query).to_dataframe()
            return jsonify({"cases": format_cases(df), "count": len(df)})
        else:
            query = f"WITH {signoff_cte()}, {on_hold_cte()} SELECT COUNT(DISTINCT f.id) as case_count {case_joins()} WHERE {where}"
            df = client.query(query).to_dataframe().fillna(0)
            return jsonify({"case_count": int(df.iloc[0].get('case_count', 0))})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/wip", methods=["GET"])
def analytics_wip():
    try:
        args = request.args
        where = build_where_clause(args)
        fetch_step = args.get('fetch_step')
        # Cases where Design Call Prep REVIEWER signoff exists AND case sits in PLANNING_REVIEW work queue
        # are awaiting surgeon — they should NOT appear in CAD Design WIP, but should appear in Surgeon Approval.
        dcp_done_cte = f"""
        design_call_prep_done AS (
            SELECT DISTINCT w.caseId
            FROM {tbl('WorkModuleSignoff')} w
            LEFT JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
            LEFT JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
            WHERE w.deleted = false
              AND wm.name = 'Design Call Prep'
              AND w.workModuleUserType = 'REVIEWER'
              AND w.workModuleSignatureType = 'ACCEPT'
        )"""
        # In the CAD Design (psp_design) bucket: exclude cases where Design Call Prep is done AND case is in PLANNING_REVIEW.
        # Those cases have left CAD's hands and are waiting on surgeon — they belong in surgeon_approval instead.
        awaiting_surgeon_predicate = "(f.id IN (SELECT caseId FROM design_call_prep_done) AND f.work_queue = 'PLANNING_REVIEW')"
        # Implicit-PSP rule: a case is considered "past PSP" if any of these exist:
        #   - first_psp_review_date (formal PSP REVIEWER ACCEPT)
        #   - peer_review_date (completed peer review without formal PSP signoff)
        #   - ship_wrk_comp_date (shipped without formal PSP signoff)
        past_psp = "(sd.first_psp_review_date IS NOT NULL OR sd.peer_review_date IS NOT NULL OR s.ship_wrk_comp_date IS NOT NULL)"
        # Marketing exclusion — applies to Peer Review and Manufacturing WIP.
        # A case is a marketing case if 'marketing' appears anywhere in the surgeon name (case-insensitive).
        not_marketing = "(LOWER(COALESCE(f.phy_nameFirst,'') || ' ' || COALESCE(f.phy_nameLast,'')) NOT LIKE '%marketing%')"
        STEP_FILTERS = {
            'segmentation':
                f"s.first_scan_upload_date IS NOT NULL AND sd.seg_review_date IS NULL AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY')",
            'psp_design':
                f"sd.seg_review_date IS NOT NULL AND NOT {past_psp} AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY') AND NOT {awaiting_surgeon_predicate}"
                f" OR (sd.first_psp_review_date IS NOT NULL AND sd.surgeon_approval_date IS NULL AND sd.surgeon_last_action_reject = true AND sd.peer_review_date IS NULL AND s.ship_wrk_comp_date IS NULL AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY'))",
            'surgeon_approval':
                f"(sd.first_psp_review_date IS NOT NULL AND sd.surgeon_approval_date IS NULL AND COALESCE(sd.surgeon_last_action_reject, false) = false AND sd.peer_review_date IS NULL AND s.ship_wrk_comp_date IS NULL AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY')) OR (sd.seg_review_date IS NOT NULL AND NOT {past_psp} AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY') AND {awaiting_surgeon_predicate})",
            'peer_review':
                # Exclude: awaiting surgeon approval (already in surgeon bucket)
                # Exclude: in MANUFACTURING phase (already in manufacturing bucket)
                f"{past_psp} AND sd.peer_review_date IS NULL AND s.ship_wrk_comp_date IS NULL"
                f" AND f.onHold = false AND f.canceled = false"
                f" AND f.phase NOT IN ('SHIPPING','SURGERY','MANUFACTURING')"
                f" AND NOT (sd.first_psp_review_date IS NOT NULL AND sd.surgeon_approval_date IS NULL AND COALESCE(sd.surgeon_last_action_reject, false) = false)"
                f" AND {not_marketing}",
            'manufacturing':
                f"f.phase = 'MANUFACTURING' AND f.onHold = false AND f.canceled = false AND s.ship_wrk_comp_date IS NULL AND {not_marketing}",
        }
        if fetch_step:
            step_filter = STEP_FILTERS.get(fetch_step, "1=1")
            query = f"""
            WITH {signoff_cte()}, {on_hold_cte()}, {dcp_done_cte}
            SELECT {case_select_fields()}
            {case_joins()}
            WHERE {where} AND ({step_filter})
            ORDER BY f.createdAt DESC LIMIT 500
            """
            df = client.query(query).to_dataframe()
            return jsonify({"cases": format_cases(df), "count": len(df), "step": fetch_step})
        query = f"""
        WITH {signoff_cte()}, {on_hold_cte()}, {dcp_done_cte}
        SELECT
            COUNTIF(s.first_scan_upload_date IS NOT NULL AND sd.seg_review_date IS NULL
                AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY')) as segmentation,
            COUNTIF(
                (sd.seg_review_date IS NOT NULL
                AND NOT (sd.first_psp_review_date IS NOT NULL OR sd.peer_review_date IS NOT NULL OR s.ship_wrk_comp_date IS NOT NULL)
                AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY')
                AND NOT {awaiting_surgeon_predicate})
                OR
                (sd.first_psp_review_date IS NOT NULL AND sd.surgeon_approval_date IS NULL
                AND COALESCE(sd.surgeon_last_action_reject, false) = true
                AND sd.peer_review_date IS NULL AND s.ship_wrk_comp_date IS NULL
                AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY'))
            ) as psp_design,
            COUNTIF(
                (sd.first_psp_review_date IS NOT NULL AND sd.surgeon_approval_date IS NULL
                    AND COALESCE(sd.surgeon_last_action_reject, false) = false
                    AND sd.peer_review_date IS NULL AND s.ship_wrk_comp_date IS NULL
                    AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY'))
                OR
                (sd.seg_review_date IS NOT NULL
                    AND NOT (sd.first_psp_review_date IS NOT NULL OR sd.peer_review_date IS NOT NULL OR s.ship_wrk_comp_date IS NOT NULL)
                    AND f.onHold = false AND f.canceled = false AND f.phase NOT IN ('SHIPPING','SURGERY')
                    AND {awaiting_surgeon_predicate})
            ) as surgeon_approval,
            COUNTIF((sd.first_psp_review_date IS NOT NULL OR sd.peer_review_date IS NOT NULL OR s.ship_wrk_comp_date IS NOT NULL)
                AND sd.peer_review_date IS NULL AND s.ship_wrk_comp_date IS NULL
                AND f.onHold = false AND f.canceled = false
                AND f.phase NOT IN ('SHIPPING','SURGERY','MANUFACTURING')
                AND NOT (sd.first_psp_review_date IS NOT NULL AND sd.surgeon_approval_date IS NULL AND COALESCE(sd.surgeon_last_action_reject, false) = false)
                AND (LOWER(COALESCE(f.phy_nameFirst,'') || ' ' || COALESCE(f.phy_nameLast,'')) NOT LIKE '%marketing%')) as peer_review,
            COUNTIF(f.phase = 'MANUFACTURING' AND f.onHold = false AND f.canceled = false AND s.ship_wrk_comp_date IS NULL
                AND (LOWER(COALESCE(f.phy_nameFirst,'') || ' ' || COALESCE(f.phy_nameLast,'')) NOT LIKE '%marketing%')) as manufacturing
        {case_joins()}
        WHERE {where}
        """
        df = client.query(query).to_dataframe().fillna(0)
        row = df.iloc[0]
        return jsonify({
            "segmentation": int(row.get('segmentation', 0)),
            "psp_design": int(row.get('psp_design', 0)),
            "surgeon_approval": int(row.get('surgeon_approval', 0)),
            "peer_review": int(row.get('peer_review', 0)),
            "manufacturing": int(row.get('manufacturing', 0)),
        })
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/cases", methods=["GET"])
def analytics_cases():
    try:
        args = request.args
        is_volume_pop = args.get('case_population', '').strip().lower() == 'volume'
        where = volume_where_clause(args) if is_volume_pop else build_where_clause(args)
        select_fields = volume_case_select_fields() if is_volume_pop else case_select_fields()
        joins = volume_case_joins_full() if is_volume_pop else case_joins()
        if is_volume_pop:
            # Matches the volume chart's own requirement that PSP completion date is set
            where += " AND sd.first_psp_review_date IS NOT NULL"
        if args.get('milestone_field') and (args.get('date_from') or args.get('date_to')):
            mf = args.get('milestone_field')
            df_filter = date_range_filter(mf, args.get('date_from',''), args.get('date_to',''))
            query = f"WITH {signoff_cte()}, {on_hold_cte()} SELECT {select_fields} {joins} WHERE {where} AND {df_filter} ORDER BY f.createdAt DESC LIMIT 500"
        else:
            if args.get('date_from'):
                where += f" AND f.createdAt >= '{validate_date(args.get('date_from'))}'"
            if args.get('date_to'):
                where += f" AND f.createdAt <= '{validate_date(args.get('date_to'))} 23:59:59'"
            query = f"WITH {signoff_cte()}, {on_hold_cte()} SELECT {select_fields} {joins} WHERE {where} ORDER BY f.createdAt DESC LIMIT 500"
        df = client.query(query).to_dataframe()
        return jsonify({"cases": format_cases(df), "count": len(df)})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/trends", methods=["GET"])
def analytics_trends():
    try:
        args = request.args
        metric = args.get('metric', 'volume')
        granularity = args.get('granularity', 'monthly')
        exclude_outliers = args.get('exclude_outliers') == 'true'
        avg_digital_lt = float(args.get('avg_digital_lt', 0))
        trunc = 'WEEK' if granularity == 'weekly' else 'MONTH'
        # breakdown_by: 'product' (default) or 'case_type'.
        # When 'case_type', group by ct.name / f.case_type_name instead of product.
        breakdown_by = args.get('breakdown_by', 'product').strip().lower()
        use_case_type = breakdown_by == 'case_type'

        if metric == 'volume':
            # Use raw Case table to include COMPLETED cases
            where = volume_where_clause(args)
            # date_field can be overridden by caller:
            #   first_scan_upload_date → Submitted (scan upload)
            #   first_psp_review_date  → Completed (PSP review) [default, uses effective PSP = LEAST(psp, peer review, ship)]
            date_field_param = args.get('date_field', '').strip()
            if date_field_param == 'first_scan_upload_date':
                date_field = 's.first_scan_upload_date'
            else:
                # PSP reviewer accept date only — matches stat card and process counts definition
                date_field = "CAST(sd.first_psp_review_date AS DATETIME)"
            df_filter = date_range_filter(date_field, args.get('date_from',''), args.get('date_to',''))
            # Also require PSP date is not null for completed bar
            if date_field_param != 'first_scan_upload_date':
                df_filter += " AND sd.first_psp_review_date IS NOT NULL"
            # Group by case type or product
            group_field = "ct.name" if use_case_type else "cc.name"
            query = f"""
            WITH {signoff_cte()}
            SELECT DATE_TRUNC(DATE(CAST({date_field} AS DATETIME)), {trunc}) as period,
                {group_field} as product, COUNT(DISTINCT f.id) as value
            {volume_case_joins()}
            WHERE {where} AND {df_filter}
            GROUP BY period, product ORDER BY period ASC
            """
        else:
            where = build_where_clause(args)
            if exclude_outliers:
                where = f"{where} AND {outlier_exclusion_sql(avg_digital_lt)}"
            if metric in METRIC_MAP:
                date_field = METRIC_MAP[metric][2]
                agg_expr = f"ROUND(AVG({METRIC_MAP[metric][0]}), 1)"
            else:
                date_field = 'sd.first_psp_review_date'
                agg_expr = 'COUNT(DISTINCT f.id)'
            df_filter = date_range_filter(date_field, args.get('date_from',''), args.get('date_to',''))
            # Group by case type or product
            group_field = "f.case_type_name" if use_case_type else "f.case_category_name"
            query = f"""
            WITH {signoff_cte()}, {on_hold_cte()}
            SELECT DATE_TRUNC(DATE(CAST({date_field} AS DATETIME)), {trunc}) as period,
                {group_field} as product, {agg_expr} as value
            {case_joins()}
            WHERE {where} AND {df_filter}
            GROUP BY period, product ORDER BY period ASC
            """

        df = client.query(query).to_dataframe().astype(str)
        periods = sorted(df['period'].unique().tolist())
        products = sorted(df['product'].unique().tolist())
        colors = ['#0284c7','#16a34a','#7c3aed','#d97706','#0891b2','#dc2626','#059669','#9333ea']
        datasets = []
        for i, product in enumerate(products):
            pdf = df[df['product'] == product]
            period_map = dict(zip(pdf['period'].tolist(), pdf['value'].tolist()))
            data = []
            for p in periods:
                try: data.append(float(period_map.get(p, 0) or 0))
                except: data.append(0)
            datasets.append({'label': shorten_product(product), 'full_label': product, 'data': data, 'color': colors[i % len(colors)]})
        return jsonify({'periods': periods, 'datasets': datasets, 'metric': metric, 'granularity': granularity})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/users-by-step", methods=["GET"])
def users_by_step():
    """Return distinct step names and/or userIds for populating filter dropdowns."""
    try:
        args = request.args
        step = args.get('step', '').strip()
        mode = args.get('mode', '').strip()  # 'users_only' to force user list
        where_parts = ["w.deleted = false", "w.workModuleSignatureType = 'ACCEPT'"]
        # Apply product filter via case join
        product = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()
        case_join = ""
        if product or product_group:
            case_join = f"JOIN {tbl('vw_fact_case')} f ON w.caseId = f.id"
            if product:
                prods = [p.strip() for p in product.split(',')]
                joined = "','".join(prods)
                where_parts.append(f"f.case_category_name IN (\'{joined}\')")
            if product_group:
                all_prods = []
                for g in [g.strip() for g in product_group.split(',')]:
                    all_prods.extend(PRODUCT_GROUPS.get(g, []))
                if all_prods:
                    joined = "','".join(all_prods)
                    where_parts.append(f"f.case_category_name IN (\'{joined}\')")

        if step:
            steps = [s.strip() for s in step.split(',')]
            if len(steps) == 1:
                where_parts.append(f"wm.name = '{steps[0]}'")
            else:
                joined = "','".join(steps)
                where_parts.append(f"wm.name IN ('{joined}')")

        if step or mode == 'users_only':
            # Return users (optionally filtered by step and/or product)
            where_sql = " AND ".join(where_parts)
            query = f"""
                SELECT w.signature as userId, COUNT(DISTINCT w.caseId) as case_count
                FROM {tbl('WorkModuleSignoff')} w
                LEFT JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
                LEFT JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
                {case_join}
                WHERE {where_sql} AND w.signature IS NOT NULL AND w.signature != ''
                GROUP BY w.signature ORDER BY case_count DESC
            """
            rows = client.query(query).to_dataframe().astype(str).to_dict(orient="records")
            return jsonify({"mode": "users", "step": step, "users": rows})
        else:
            # Return step names
            where_sql = " AND ".join(where_parts)
            query = f"""
                SELECT wm.name as step_name, COUNT(DISTINCT w.caseId) as case_count
                FROM {tbl('WorkModuleSignoff')} w
                LEFT JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
                LEFT JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
                {case_join}
                WHERE {where_sql} AND wm.name IS NOT NULL
                GROUP BY wm.name ORDER BY case_count DESC
            """
            rows = client.query(query).to_dataframe().astype(str).to_dict(orient="records")
            return jsonify({"mode": "steps", "steps": rows})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/utilization", methods=["GET"])
def utilization():
    """Calculate utilization per user: earned_minutes / (active_days * 480).
    IST timezone: EST + 5:30 hours (330 minutes).
    Active days = distinct IST dates with at least 1 signoff.
    """
    try:
        args = request.args
        granularity = args.get('granularity', 'weekly')  # daily, weekly, monthly
        date_from = args.get('date_from', '')
        date_to = args.get('date_to', '')
        step_filter = args.get('step', '').strip()
        user_filter = args.get('step_user', '').strip()
        product_filter = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()

        # Build product filter for case join
        # Use raw Case table so COMPLETED cases are included (vw_fact_case excludes them)
        # MedEd and all intake types included — no Case_In_Take filter
        case_conditions = ["f.deleted = false", "f.canceled = false"]
        if product_filter:
            prods = [p.strip() for p in product_filter.split(',')]
            joined = "','".join(prods)
            case_conditions.append(f"cc.name IN ('{joined}')" if len(prods) > 1 else f"cc.name = '{prods[0]}'")
        elif product_group:
            all_prods = []
            for g in [g.strip() for g in product_group.split(',')]:
                all_prods.extend(PRODUCT_GROUPS.get(g, []))
            if all_prods:
                joined = "','".join(all_prods)
                case_conditions.append(f"cc.name IN ('{joined}')")
        else:
            case_conditions.append("cc.name IN ('Reverse Total Shoulder Arthroplasty','Total Ankle Replacement')")
        case_where = " AND ".join(case_conditions)

        # Signoff conditions — ACCEPT only for workers, but include all for active day counting
        sig_conditions = ["w.deleted = false", "w.workModuleSignatureType = 'ACCEPT'"]
        if step_filter:
            steps = [s.strip() for s in step_filter.split(',')]
            if len(steps) == 1:
                sig_conditions.append(f"wm.name = '{steps[0]}'")
            else:
                joined = "','".join(steps)
                sig_conditions.append(f"wm.name IN ('{joined}')")
        if user_filter:
            users = [u.strip() for u in user_filter.split(',')]
            if len(users) == 1:
                sig_conditions.append(f"w.signature = '{users[0]}'")
            else:
                joined = "','".join(users)
                sig_conditions.append(f"w.signature IN ('{joined}')")

        # Date filter on IST date
        ist_date = "DATE(TIMESTAMP_ADD(CAST(w.createdAt AS TIMESTAMP), INTERVAL 330 MINUTE))"
        if date_from:
            sig_conditions.append(f"{ist_date} >= '{date_from}'")
        if date_to:
            sig_conditions.append(f"{ist_date} <= '{date_to}'")

        sig_where = " AND ".join(sig_conditions)

        # Period grouping
        if granularity == 'daily':
            period_expr = ist_date
        elif granularity == 'monthly':
            period_expr = f"FORMAT_DATE('%Y-%m', {ist_date})"
        else:  # weekly
            period_expr = f"FORMAT_DATE('%G-W%V', {ist_date})"

        # Main query: per user per period, get signoff details
        query = f"""
            SELECT
                w.signature as userId,
                {period_expr} as period,
                wm.name as step_name,
                w.workModuleUserType as user_type,
                cc.name as product,
                f.laterality as laterality,
                f.preoperativeState as preoperativeState,
                f.proposedIndication as proposedIndication,
                f.designNotes as designNotes,
                COUNT(DISTINCT w.caseId) as signoff_count,
                COUNT(DISTINCT {ist_date}) as active_days
            FROM {tbl('WorkModuleSignoff')} w
            LEFT JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
            LEFT JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
            JOIN {tbl('Case')} f ON w.caseId = f.id
            JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
            WHERE {sig_where} AND {case_where} AND w.signature IS NOT NULL AND w.signature != ''
            GROUP BY w.signature, period, step_name, user_type, product, w.caseId,
                     laterality, preoperativeState, proposedIndication, designNotes
            ORDER BY w.signature, period
        """
        df = client.query(query).to_dataframe()

        if df.empty:
            return jsonify({"users": [], "team_avg": 0, "granularity": granularity, "periods": []})

        # Bilateral/revision multiplier — computed per case, applied only within
        # get_design_time's Scan Assessment/Segmentation branches.
        df['time_multiplier'] = df.apply(
            lambda r: get_case_time_multiplier(
                r['laterality'], r['preoperativeState'], r['proposedIndication'], r['designNotes']
            ), axis=1
        )

        # Calculate earned minutes per row using design time lookup
        df['earned_min'] = df.apply(
            lambda r: (get_design_time(r['step_name'], r['user_type'], r['product'], r['time_multiplier']) or 0) * int(r['signoff_count']),
            axis=1
        )

        # Per user per period: total earned minutes and active days
        # Active days = distinct IST dates a user had ANY signoff (not just per step)
        # Need a separate active-days query per user per period
        active_days_query = f"""
            SELECT
                w.signature as userId,
                {period_expr} as period,
                COUNT(DISTINCT {ist_date}) as active_days
            FROM {tbl('WorkModuleSignoff')} w
            LEFT JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
            LEFT JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
            JOIN {tbl('Case')} f ON w.caseId = f.id
            JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
            WHERE {sig_where} AND {case_where} AND w.signature IS NOT NULL AND w.signature != ''
            GROUP BY w.signature, period
        """
        ad_df = client.query(active_days_query).to_dataframe()
        ad_map = {}
        for _, r in ad_df.iterrows():
            ad_map[(r['userId'], str(r['period']))] = int(r['active_days'])

        # Aggregate earned minutes per user per period
        user_period = df.groupby(['userId', 'period']).agg(
            earned_min=('earned_min', 'sum'),
            signoff_count=('signoff_count', 'sum')
        ).reset_index()

        user_period['active_days'] = user_period.apply(
            lambda r: ad_map.get((r['userId'], str(r['period'])), 1), axis=1
        )
        user_period['capacity_min'] = user_period['active_days'] * MINUTES_PER_DAY
        user_period['utilization'] = (user_period['earned_min'] / user_period['capacity_min'] * 100).round(1)

        # Per-user summary
        user_summary = user_period.groupby('userId').agg(
            total_earned=('earned_min', 'sum'),
            total_active_days=('active_days', 'sum'),
            total_signoffs=('signoff_count', 'sum')
        ).reset_index()
        user_summary['utilization'] = (user_summary['total_earned'] / (user_summary['total_active_days'] * MINUTES_PER_DAY) * 100).round(1)

        # Team average
        team_earned = user_summary['total_earned'].sum()
        team_days = user_summary['total_active_days'].sum()
        team_avg = round(team_earned / (team_days * MINUTES_PER_DAY) * 100, 1) if team_days > 0 else 0

        # Build period-level data for trend chart
        periods = sorted(user_period['period'].unique().tolist())
        period_data = []
        for p in periods:
            p_df = user_period[user_period['period'] == str(p)]
            p_earned = p_df['earned_min'].sum()
            p_days = p_df['active_days'].sum()
            p_util = round(p_earned / (p_days * MINUTES_PER_DAY) * 100, 1) if p_days > 0 else 0
            period_data.append({'period': str(p), 'utilization': p_util, 'earned_min': int(p_earned), 'active_days': int(p_days)})

        users = []
        for _, r in user_summary.iterrows():
            users.append({
                'userId': r['userId'],
                'utilization': float(r['utilization']),
                'earned_min': int(r['total_earned']),
                'active_days': int(r['total_active_days']),
                'signoff_count': int(r['total_signoffs'])
            })
        users.sort(key=lambda x: x['utilization'], reverse=True)

        return jsonify({
            "users": users,
            "team_avg": team_avg,
            "granularity": granularity,
            "periods": period_data,
            "user_periods": user_period[['userId','period','utilization','earned_min','active_days','signoff_count']].to_dict(orient='records')
        })
    except Exception as e:
        return handle_error(request.endpoint, e)

_LOG = "`restor3d-data-warehouse.production3_logging_public.Log`"

_EFF_STEPS   = ['Scan Assessment','Segmentation','Jigs Design','Planning','Design','Proposed Surgical Plan']

_EFF_MIN_SEC_SCAN = 120   # 2 min floor for Scan Assessment

_EFF_MIN_SEC      = 600   # 10 min floor for all other steps

_EFF_MAX_SEC = 28800

_EFF_LOOKBACK = 30

def _eff_case_where(args):
    product_filter = args.get('product','').strip()
    product_group  = args.get('product_group','').strip()
    conds = ["f.deleted = false","f.canceled = false"]
    if product_filter:
        prods = [p.strip() for p in product_filter.split(',')]
        joined = "','".join(prods)
        conds.append("cc.name IN ('" + joined + "')")
    elif product_group:
        all_prods = []
        for g in [g.strip() for g in product_group.split(',')]:
            all_prods.extend(PRODUCT_GROUPS.get(g,[]))
        if all_prods:
            joined = "','".join(all_prods)
            conds.append("cc.name IN ('" + joined + "')")
    else:
        conds.append("cc.name IN ('Reverse Total Shoulder Arthroplasty','Total Ankle Replacement')")
    return " AND ".join(conds)

def _eff_date_filter(args, ts_expr):
    date_from = args.get('date_from','').strip()
    date_to   = args.get('date_to','').strip()
    parts = []
    if date_from:
        validate_date(date_from)
        parts.append(f"DATE({ts_expr}) >= '{date_from}'")
    if date_to:
        validate_date(date_to)
        parts.append(f"DATE({ts_expr}) <= '{date_to}'")
    return ("AND " + " AND ".join(parts)) if parts else ""

def _eff_valid_steps(args):
    step_filter = args.get('step','').strip()
    if step_filter:
        steps = [s.strip() for s in step_filter.split(',')]
        return [s for s in steps if s in _EFF_STEPS] or _EFF_STEPS
    return _EFF_STEPS

def _eff_core_query(args):
    """Returns the core classified CTE as a SQL string."""
    case_where  = _eff_case_where(args)
    valid_steps = _eff_valid_steps(args)
    steps_sql   = "','".join(valid_steps)
    ist_sig     = "TIMESTAMP_ADD(sig.createdAt, INTERVAL 330 MINUTE)"
    date_filter = _eff_date_filter(args, ist_sig)

    # User filter — step_user contains full names matching User table nameFirst + nameLast
    user_filter = args.get('step_user','').strip()
    if user_filter:
        users = [u.strip() for u in user_filter.split(',')]
        joined_users = "','".join(users)
        user_having = "AND TRIM(CONCAT(COALESCE(u.nameFirst,''),' ',COALESCE(u.nameLast,''))) IN ('" + joined_users + "')"
    else:
        user_having = ""

    return f"""
    cases AS (
      SELECT f.id AS caseId, cc.name AS product,
             f.laterality AS laterality, c2.preoperativeState AS preoperativeState,
             f.proposedIndication AS proposedIndication, f.designNotes AS designNotes
      FROM {tbl('vw_fact_case')} f
      JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
      LEFT JOIN {tbl('Case')} c2 ON f.id = c2.id
      WHERE {case_where}
    ),
    -- All passes for Planning TAR with actual seconds pre-computed
    planning_tar_passes AS (
      SELECT
        sig.refId AS caseId, sig.userId, sig.createdAt AS end_time,
        'Planning' AS step_name, c_filter.product AS product,
        c_filter.laterality AS laterality, c_filter.preoperativeState AS preoperativeState,
        c_filter.proposedIndication AS proposedIndication, c_filter.designNotes AS designNotes,
        TIMESTAMP_DIFF(sig.createdAt,
          MAX(asn.createdAt) OVER (
            PARTITION BY sig.refId, sig.userId
            ORDER BY sig.createdAt
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ), SECOND
        ) AS actual_sec_raw
      FROM {_LOG} sig
      INNER JOIN cases c_filter ON sig.refId = c_filter.caseId
      LEFT JOIN {_LOG} asn
        ON asn.refId = sig.refId
        AND asn.userId = sig.userId
        AND asn.type = 'r3idCaseRoleAssignmentUpdate'
        AND asn.createdAt <= sig.createdAt
        AND asn.createdAt >= TIMESTAMP_SUB(sig.createdAt, INTERVAL {_EFF_LOOKBACK} DAY)
      WHERE sig.type = 'r3idWorkModuleSignoff-Planning-WORKER-ACCEPT'
        AND c_filter.product = 'Total Ankle Replacement'
        {date_filter}
    ),
    -- For each case+user: pick the pass closest to standard (150 min = 9000 sec)
    -- On tie, pick the slower one (higher actual_sec)
    planning_tar_best AS (
      SELECT *
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY caseId, userId
            ORDER BY
              ABS(actual_sec_raw - 9000) ASC,  -- closest to 150 min
              actual_sec_raw DESC               -- on tie, slower
          ) AS rn
        FROM planning_tar_passes
        WHERE actual_sec_raw IS NOT NULL
      )
      WHERE rn = 1
        -- Only include if within 50%-150% of standard (75-225 min = 4500-13500 sec)
        AND actual_sec_raw BETWEEN 4500 AND 18000
    ),
    signoffs_raw AS (
      SELECT
        sig.refId AS caseId, sig.userId, sig.createdAt AS end_time,
        REGEXP_EXTRACT(sig.type,
          r'r3idWorkModuleSignoff-(.+)-(?:WORKER|REVIEWER|APPROVER)-ACCEPT'
        ) AS step_name,
        ROW_NUMBER() OVER (
          PARTITION BY sig.refId,
            REGEXP_EXTRACT(sig.type,
              r'r3idWorkModuleSignoff-(.+)-(?:WORKER|REVIEWER|APPROVER)-ACCEPT'
            )
          ORDER BY sig.createdAt DESC
        ) AS rn
      FROM {_LOG} sig
      INNER JOIN cases c_filter ON sig.refId = c_filter.caseId
      WHERE sig.type LIKE 'r3idWorkModuleSignoff-%-WORKER-ACCEPT'
        AND sig.type != 'r3idWorkModuleSignoff-Planning-WORKER-ACCEPT'
        {date_filter}
    ),
    signoffs AS (
      SELECT signoffs_raw.caseId, signoffs_raw.userId, signoffs_raw.end_time,
             signoffs_raw.step_name, c_filter2.product,
             c_filter2.laterality, c_filter2.preoperativeState,
             c_filter2.proposedIndication, c_filter2.designNotes
      FROM signoffs_raw
      JOIN cases c_filter2 ON signoffs_raw.caseId = c_filter2.caseId
      WHERE signoffs_raw.rn = 1 AND signoffs_raw.step_name IN ('{steps_sql}')
      UNION ALL
      -- Add Planning TAR best passes (already filtered to qualified range)
      SELECT caseId, userId, end_time, step_name, product,
             laterality, preoperativeState, proposedIndication, designNotes
      FROM planning_tar_best
      WHERE 'Planning' IN ('{steps_sql}')
    ),
    paired AS (
      SELECT
        s.caseId, s.userId, s.step_name, s.end_time, s.product,
        s.laterality, s.preoperativeState, s.proposedIndication, s.designNotes,
        MAX(asn.createdAt) AS start_time
      FROM signoffs s
      LEFT JOIN {_LOG} asn
        ON asn.refId = s.caseId
        AND asn.userId = s.userId
        AND asn.type = 'r3idCaseRoleAssignmentUpdate'
        AND asn.createdAt <= s.end_time
        AND asn.createdAt >= TIMESTAMP_SUB(s.end_time, INTERVAL {_EFF_LOOKBACK} DAY)
      GROUP BY s.caseId, s.userId, s.step_name, s.end_time, s.product,
               s.laterality, s.preoperativeState, s.proposedIndication, s.designNotes
    ),
    classified AS (
      SELECT
        p.caseId, p.userId, p.step_name, p.end_time, p.product,
        p.laterality, p.preoperativeState, p.proposedIndication, p.designNotes,
        TRIM(CONCAT(COALESCE(u.nameFirst,''),' ',COALESCE(u.nameLast,''))) AS worker_name,
        CASE
          -- Planning TAR: already validated in planning_tar_best, always valid
          WHEN p.step_name = 'Planning' AND p.product = 'Total Ankle Replacement'
            THEN 'valid'
          WHEN p.start_time IS NULL THEN 'no_pair'
          WHEN p.step_name = 'Scan Assessment'
               AND TIMESTAMP_DIFF(p.end_time, p.start_time, SECOND) < {_EFF_MIN_SEC_SCAN} THEN 'click_through'
          WHEN p.step_name != 'Scan Assessment'
               AND TIMESTAMP_DIFF(p.end_time, p.start_time, SECOND) < {_EFF_MIN_SEC} THEN 'click_through'
          WHEN TIMESTAMP_DIFF(p.end_time, p.start_time, SECOND) > {_EFF_MAX_SEC} THEN 'over_standard'
          ELSE 'valid'
        END AS pair_status,
        CASE
          -- Planning TAR: use pre-computed actual_sec from planning_tar_best
          WHEN p.step_name = 'Planning' AND p.product = 'Total Ankle Replacement'
            THEN pt.actual_sec_raw
          ELSE TIMESTAMP_DIFF(p.end_time, p.start_time, SECOND)
        END AS actual_sec
      FROM paired p
      LEFT JOIN {tbl('User')} u ON p.userId = u.id
      LEFT JOIN planning_tar_best pt
        ON pt.caseId = p.caseId AND pt.userId = p.userId
      WHERE 1=1 {user_having}
    )"""

@app.route("/analytics/process-efficiency", methods=["GET"])
def process_efficiency():
    """Process efficiency: standard_time / actual_time * 100%.
    Workers only. Last signoff per (caseId, step). Excludes <2min and >8hr pairs.
    """
    try:
        args = request.args
        granularity = args.get('granularity','daily')
        ist_sig = "TIMESTAMP_ADD(end_time, INTERVAL 330 MINUTE)"
        if granularity == 'daily':
            period_expr = f"DATE({ist_sig})"
        elif granularity == 'monthly':
            period_expr = f"FORMAT_DATE('%Y-%m', {ist_sig})"
        else:
            period_expr = f"FORMAT_DATE('%G-W%V', {ist_sig})"

        core = _eff_core_query(args)
        query = f"""
        WITH {core},
        valid_only AS (
          SELECT *, {period_expr} AS period
          FROM classified WHERE pair_status = 'valid'
        )
        SELECT
          worker_name, step_name, product, period, caseId,
          laterality, preoperativeState, proposedIndication, designNotes,
          actual_sec / 60.0 AS actual_min
        FROM valid_only
        ORDER BY worker_name, step_name, period
        """

        df = client.query(query).to_dataframe()
        if df.empty:
            return jsonify({"users":[],"team_avg":0,"granularity":granularity,"periods":[]})

        # Bilateral/revision multiplier computed per case (not per aggregated group —
        # a worker/step/period bucket can contain a mix of bilateral and non-bilateral
        # cases, so the multiplier has to be applied before any averaging happens).
        df['time_multiplier'] = df.apply(
            lambda r: get_case_time_multiplier(
                r['laterality'], r['preoperativeState'], r['proposedIndication'], r['designNotes']
            ), axis=1
        )
        df['standard_min'] = df.apply(
            lambda r: get_design_time(r['step_name'], 'WORKER', r['product'], r['time_multiplier']) or 0, axis=1
        )
        df_valid = df[df['standard_min'] > 0].copy()
        df_valid['eff'] = df_valid['standard_min'] / df_valid['actual_min'] * 100
        # Exclude rows where efficiency > 200% of what's expected — the cap itself
        # scales with the same bilateral/revision multiplier as standard_min, so a
        # doubled-standard case isn't unfairly capped at the same flat threshold.
        df_valid = df_valid[df_valid['eff'] <= 200.0 * df_valid['time_multiplier']]

        # Per-user weighted efficiency (each row is now one case, so this is a
        # straight sum of standard vs actual minutes across all its cases)
        user_eff = df_valid.groupby('worker_name').apply(
            lambda g: round(g['standard_min'].sum() / g['actual_min'].sum() * 100, 1)
        ).reset_index()
        user_eff.columns = ['userId','efficiency']
        user_eff = user_eff.sort_values('efficiency', ascending=False)

        total_std = df_valid['standard_min'].sum()
        total_act = df_valid['actual_min'].sum()
        team_avg  = round(total_std / total_act * 100, 1) if total_act > 0 else 0

        periods_sorted = sorted(df_valid['period'].unique().tolist())
        period_data = []
        for p in periods_sorted:
            p_df = df_valid[df_valid['period'] == str(p)]
            p_std = p_df['standard_min'].sum()
            p_act = p_df['actual_min'].sum()
            period_data.append({'period': str(p), 'efficiency': round(p_std/p_act*100,1) if p_act>0 else 0})

        return jsonify({
            "users": [{'userId':r['userId'],'efficiency':float(r['efficiency'])} for _,r in user_eff.iterrows()],
            "team_avg": team_avg,
            "granularity": granularity,
            "periods": period_data
        })
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/process-efficiency/exclusions", methods=["GET"])
def process_efficiency_exclusions():
    """Counts of excluded signoffs by reason, grouped by user and step.
    All cases appear in either efficiency chart or here.
    Planning TAR cases with no qualifying pass shown as out_of_range.
    """
    try:
        args = request.args
        case_where  = _eff_case_where(args)
        valid_steps = _eff_valid_steps(args)
        steps_sql   = "\',\'".join(valid_steps)
        ist_sig     = "TIMESTAMP_ADD(sig.createdAt, INTERVAL 330 MINUTE)"
        date_filter = _eff_date_filter(args, ist_sig)
        user_filter = args.get('step_user','').strip()
        if user_filter:
            users = [u.strip() for u in user_filter.split(',')]
            joined_users = "\',\'".join(users)
            user_having = "AND TRIM(CONCAT(COALESCE(u.nameFirst,\'\'),' ',COALESCE(u.nameLast,\'\'))) IN (\'" + joined_users + "\')"
        else:
            user_having = ""

        # Part 1: standard exclusions from classified CTE
        core = _eff_core_query(args)
        std_query = f"""
        WITH {core}
        SELECT worker_name, step_name, pair_status AS excl_reason, COUNT(*) AS cnt
        FROM classified
        WHERE pair_status != 'valid'
        GROUP BY worker_name, step_name, pair_status
        """
        df_std = client.query(std_query).to_dataframe()

        # Part 2: Planning TAR cases excluded because no pass fell in 75-225 min window
        import pandas as pd
        df_tar = pd.DataFrame()
        if 'Planning' in valid_steps:
            # User filter for this query uses worker_name (already resolved), not u.nameFirst
            if user_filter:
                users = [u.strip() for u in user_filter.split(',')]
                joined_users = "','".join(users)
                tar_user_having = "AND worker_name IN ('" + joined_users + "')"
            else:
                tar_user_having = ""

            planning_tar_excl_query = f"""
            WITH
            cases AS (
              SELECT f.id AS caseId
              FROM {tbl('vw_fact_case')} f
              JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
              WHERE {case_where} AND cc.name = 'Total Ankle Replacement'
            ),
            all_passes AS (
              SELECT
                sig.refId AS caseId,
                sig.userId,
                TRIM(CONCAT(COALESCE(u.nameFirst,''),' ',COALESCE(u.nameLast,''))) AS worker_name,
                TIMESTAMP_DIFF(sig.createdAt, MAX(asn.createdAt), SECOND) AS actual_sec
              FROM {_LOG} sig
              INNER JOIN cases c ON sig.refId = c.caseId
              LEFT JOIN {_LOG} asn
                ON asn.refId = sig.refId
                AND asn.userId = sig.userId
                AND asn.type = 'r3idCaseRoleAssignmentUpdate'
                AND asn.createdAt <= sig.createdAt
                AND asn.createdAt >= TIMESTAMP_SUB(sig.createdAt, INTERVAL {_EFF_LOOKBACK} DAY)
              LEFT JOIN {tbl('User')} u ON sig.userId = u.id
              WHERE sig.type = 'r3idWorkModuleSignoff-Planning-WORKER-ACCEPT'
                {date_filter}
              GROUP BY sig.refId, sig.userId, sig.createdAt, u.nameFirst, u.nameLast
            ),
            best_pass AS (
              SELECT caseId, userId, worker_name, actual_sec,
                ROW_NUMBER() OVER (
                  PARTITION BY caseId, userId
                  ORDER BY ABS(actual_sec - 9000) ASC, actual_sec DESC
                ) AS rn
              FROM all_passes
              WHERE actual_sec IS NOT NULL
            ),
            excluded_cases AS (
              SELECT caseId, userId, worker_name,
                CASE
                  WHEN actual_sec < {_EFF_MIN_SEC} THEN 'click_through'
                  ELSE 'over_standard'
                END AS excl_reason
              FROM best_pass
              WHERE rn = 1
                AND NOT (actual_sec BETWEEN 4500 AND 18000)
            )
            SELECT worker_name, 'Planning' AS step_name, excl_reason, COUNT(*) AS cnt
            FROM excluded_cases
            WHERE 1=1 {tar_user_having}
            GROUP BY worker_name, excl_reason
            """
            df_tar = client.query(planning_tar_excl_query).to_dataframe()

        df = pd.concat([df_std, df_tar], ignore_index=True) if not df_tar.empty else df_std

        # Total = classified rows + Planning TAR excluded cases
        core2 = _eff_core_query(args)
        tot_q = f"WITH {core2} SELECT COUNT(*) AS n FROM classified"
        tot_df = client.query(tot_q).to_dataframe()
        classified_count = int(tot_df['n'].iloc[0]) if not tot_df.empty else 0
        tar_excl_count = int(df_tar['cnt'].sum()) if not df_tar.empty else 0
        total_signoffs = classified_count + tar_excl_count

        by_user, by_step = {}, {}
        total_excluded = 0
        for _, row in df.iterrows():
            wn   = (row['worker_name'] or 'Unknown').strip()
            step = row['step_name']
            ps   = row['excl_reason']
            cnt  = int(row['cnt'])
            total_excluded += cnt
            if wn not in by_user:
                by_user[wn] = {'label':wn,'click_through':0,'over_standard':0,'no_pair':0}
            by_user[wn][ps] = by_user[wn].get(ps,0) + cnt
            if step not in by_step:
                by_step[step] = {'label':step,'click_through':0,'over_standard':0,'no_pair':0}
            by_step[step][ps] = by_step[step].get(ps,0) + cnt

        sort_key = lambda x: sum([x.get(k,0) for k in ['click_through','over_standard','no_pair']])
        return jsonify({
            "total_excluded": total_excluded,
            "total_signoffs": total_signoffs,
            "by_user":  sorted(by_user.values(),  key=sort_key, reverse=True),
            "by_step":  sorted(by_step.values(),  key=sort_key, reverse=True),
        })
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/fpy", methods=["GET"])
def first_pass_yield():
    """Calculate FPY per worker: % of cases where reviewer's FIRST signoff was ACCEPT.
    FPY is only for workers — reviewer rejects count against the worker.
    Date filter applied to reviewer ACCEPT date (UTC) — matches Power BI which filters
    on when the step was completed, not when the worker submitted.
    """
    try:
        args = request.args
        granularity = args.get('granularity', 'weekly')
        date_from = args.get('date_from', '')
        date_to = args.get('date_to', '')
        step_filter = args.get('step', '').strip()
        user_filter = args.get('step_user', '').strip()
        product_filter = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()

        case_conditions = ["f.deleted = false", "f.canceled = false"]
        if product_filter:
            prods = [p.strip() for p in product_filter.split(',')]
            joined = "','".join(prods)
            case_conditions.append(f"cc.name IN ('{joined}')" if len(prods) > 1 else f"cc.name = '{prods[0]}'")
        elif product_group:
            all_prods = []
            for g in [g.strip() for g in product_group.split(',')]:
                all_prods.extend(PRODUCT_GROUPS.get(g, []))
            if all_prods:
                joined = "','".join(all_prods)
                case_conditions.append(f"cc.name IN ('{joined}')")
        else:
            case_conditions.append("cc.name IN ('Reverse Total Shoulder Arthroplasty','Total Ankle Replacement')")
        case_where = " AND ".join(case_conditions)

        # Date filter on REVIEWER accept date (UTC) — matches Power BI slicer behaviour
        rev_date = "DATE(CAST(w_rev.createdAt AS TIMESTAMP))"
        date_conds = ""
        if date_from:
            validate_date(date_from)
            date_conds += f" AND {rev_date} >= '{date_from}'"
        if date_to:
            validate_date(date_to)
            date_conds += f" AND {rev_date} <= '{date_to}'"

        # Period grouping — use reviewer date for period bucketing
        if granularity == 'daily':
            period_expr = rev_date
        elif granularity == 'monthly':
            period_expr = f"FORMAT_DATE('%Y-%m', {rev_date})"
        else:
            period_expr = f"FORMAT_DATE('%G-W%V', {rev_date})"

        if step_filter:
            steps = [s.strip() for s in step_filter.split(',')]
            joined_steps = "','".join(steps)
            step_cond = f"AND wm.name = '{steps[0]}'" if len(steps) == 1 else "AND wm.name IN ('" + joined_steps + "')"
        else:
            step_cond = ""
        if user_filter:
            users = [u.strip() for u in user_filter.split(',')]
            joined_users = "','".join(users)
            user_cond = f"AND w_worker.signature = '{users[0]}'" if len(users) == 1 else "AND w_worker.signature IN ('" + joined_users + "')"
        else:
            user_cond = ""

        # FPY logic:
        # 1. Find WORKER ACCEPT signoffs (the worker completed the step)
        # 2. For each (caseId, workModuleInstance), find reviewer's FIRST signoff
        # 3. ACCEPT → pass. REJECT → fail. No reviewer → exclude.
        # 4. Date filter applied to reviewer ACCEPT date (step completion date)
        query = f"""
        WITH reviewer_first AS (
            SELECT
                w_rev.caseId,
                wi_rev.id as work_module_instance_id,
                wm_rev.name as step_name,
                w_rev.workModuleSignatureType as rev_result,
                {period_expr} as period,
                ROW_NUMBER() OVER (
                    PARTITION BY w_rev.caseId, wi_rev.id
                    ORDER BY w_rev.createdAt ASC
                ) as rn
            FROM {tbl('WorkModuleSignoff')} w_rev
            LEFT JOIN {tbl('WorkModuleInstance')} wi_rev ON w_rev.workModuleInstanceId = wi_rev.id
            LEFT JOIN {tbl('WorkModule')} wm_rev ON wi_rev.workModuleId = wm_rev.id
            WHERE w_rev.deleted = false
              AND w_rev.workModuleUserType IN ('REVIEWER', 'APPROVER')
              AND w_rev.workModuleSignatureType IN ('ACCEPT', 'REJECT')
              {date_conds}
        ),
        worker_signoffs AS (
            SELECT
                w_worker.caseId,
                w_worker.signature as worker_id,
                wm.name as step_name,
                wi.id as work_module_instance_id
            FROM {tbl('WorkModuleSignoff')} w_worker
            LEFT JOIN {tbl('WorkModuleInstance')} wi ON w_worker.workModuleInstanceId = wi.id
            LEFT JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
            JOIN {tbl('Case')} f ON w_worker.caseId = f.id
            JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
            WHERE w_worker.deleted = false
              AND w_worker.workModuleSignatureType = 'ACCEPT'
              AND w_worker.workModuleUserType = 'WORKER'
              {step_cond} {user_cond}
              AND {case_where}
        )
        SELECT
            ws.worker_id,
            rf.period,
            ws.step_name,
            COUNT(DISTINCT CASE WHEN rf.rev_result IS NOT NULL THEN ws.caseId END) as total_cases,
            COUNT(DISTINCT CASE WHEN rf.rev_result = 'ACCEPT' THEN ws.caseId END) as passed_cases
        FROM worker_signoffs ws
        JOIN reviewer_first rf
            ON ws.caseId = rf.caseId
            AND ws.work_module_instance_id = rf.work_module_instance_id
            AND rf.rn = 1
        GROUP BY ws.worker_id, rf.period, ws.step_name
        ORDER BY ws.worker_id, rf.period
        """
        df = client.query(query).to_dataframe()

        if df.empty:
            return jsonify({"users": [], "team_avg": 0, "granularity": granularity, "periods": []})

        # Per-user summary
        user_summary = df.groupby('worker_id').agg(
            total=('total_cases', 'sum'),
            passed=('passed_cases', 'sum')
        ).reset_index()
        user_summary['fpy'] = user_summary.apply(
            lambda r: round(r['passed'] / r['total'] * 100, 1) if r['total'] > 0 else 0, axis=1)

        # Team average
        team_total = int(user_summary['total'].sum())
        team_passed = int(user_summary['passed'].sum())
        team_avg = round(team_passed / team_total * 100, 1) if team_total > 0 else 0

        # Period trend (team-level)
        period_summary = df.groupby('period').agg(
            total=('total_cases', 'sum'),
            passed=('passed_cases', 'sum')
        ).reset_index()
        period_summary['fpy'] = period_summary.apply(
            lambda r: round(r['passed'] / r['total'] * 100, 1) if r['total'] > 0 else 0, axis=1)
        periods = sorted(period_summary['period'].unique().tolist())
        period_data = []
        for _, r in period_summary.iterrows():
            period_data.append({'period': str(r['period']), 'fpy': float(r['fpy']), 'total': int(r['total']), 'passed': int(r['passed'])})
        period_data.sort(key=lambda x: x['period'])

        users = []
        for _, r in user_summary.iterrows():
            users.append({
                'userId': r['worker_id'],
                'fpy': float(r['fpy']),
                'total_cases': int(r['total']),
                'passed_cases': int(r['passed'])
            })
        users.sort(key=lambda x: x['fpy'], reverse=True)

        # Per user per period (for detailed trend)
        user_periods = df.copy()
        user_periods['fpy'] = user_periods.apply(
            lambda r: round(r['passed_cases'] / r['total_cases'] * 100, 1) if r['total_cases'] > 0 else None, axis=1)

        def safe_float(v):
            if v is None: return None
            try:
                f = float(v)
                return None if math.isnan(f) or math.isinf(f) else f
            except: return None

        return jsonify({
            "users": [{**u, 'fpy': safe_float(u['fpy'])} for u in users],
            "team_avg": team_avg,
            "granularity": granularity,
            "periods": [{**p, 'fpy': safe_float(p['fpy'])} for p in period_data],
            "user_periods": [{
                'worker_id': r['worker_id'],
                'period': str(r['period']),
                'step_name': r['step_name'],
                'total_cases': int(r['total_cases']),
                'passed_cases': int(r['passed_cases']),
                'fpy': safe_float(r['fpy'])
            } for _, r in user_periods.iterrows()]
        })
    except Exception as e:
        return handle_error(request.endpoint, e)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)

