"""Pure contracts shared by ingest, tests and methodology documentation."""
import datetime as dt
import re

def normalize_name(name):
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name.strip())
    return re.sub(r'[^a-zA-Z0-9]+', '_', name).strip('_').lower()

def casualty_count(value):
    text = str(value).strip()
    return int(text) if re.fullmatch(r'\d+', text) else None

def recency_days(date):
    date = dt.date.fromisoformat(date)
    if date.year != 2025:
        raise ValueError('RFM must use a crash date in 2025')
    return (dt.date(2025, 12, 31) - date).days

def severity_m(injured, killed):
    return None if injured is None or killed is None else injured + killed

def reconcile_rows(source, retained, rejected, duplicate_extras):
    if min(source, retained, rejected, duplicate_extras) < 0 or source != retained + rejected + duplicate_extras:
        raise ValueError('Input rows do not reconcile with retained, rejected and duplicate rows')

def event_key(run_id, collision_id):
    return f'{run_id}:{collision_id}'

def valid_california_point(lat, lon):
    # Bounding envelope only; this is not a polygon membership test.
    return lat is not None and lon is not None and 32 <= lat <= 42.1 and -124.6 <= lon <= -114
