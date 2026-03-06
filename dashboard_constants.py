"""
Baseline reference dates and policy timeline for the congestion dashboard.
ERP = Electronic Road Pricing (Singapore).
"""
from datetime import date
from typing import List, Tuple

# (label, date_iso) — legacy refs for compatibility
LONDON_REFERENCES: List[Tuple[str, str]] = [
    ("Congestion Charge start", "2003-02-17"),
    ("Western Extension start", "2007-10-22"),
    ("Western Extension removed", "2011-01-04"),
    ("Evening charge removed", "2011-01-02"),
    ("Temporary suspension (COVID)", "2020-03-23"),
    ("Current charging hours", "2020-06-22"),
]

SINGAPORE_REFERENCES: List[Tuple[str, str]] = [
    ("ALS start (Area Licensing Scheme)", "1975-06-02"),
    ("ERP start (Electronic Road Pricing)", "1998-04-01"),
    ("ERP 2.0 (new OBU)", "2024-05-01"),
]

ERP_ACRONYM = "ERP = Electronic Road Pricing"


# Policy timeline for briefing UI. Each event: id, date_iso (or year for ranges), short_label, description
LONDON_TIMELINE_EVENTS: List[dict] = [
    {"id": "cc_start", "date_iso": "2003-02-17", "short_label": "Feb 2003", "description": "Congestion Charge introduced"},
    {"id": "western_ext", "date_iso": "2007-10-22", "short_label": "Oct 2007", "description": "Western Extension added"},
    {"id": "western_removed", "date_iso": "2011-01-04", "short_label": "Jan 2011", "description": "Western Extension removed"},
    {"id": "covid", "date_iso": "2020-03-23", "short_label": "2020–2022", "description": "COVID temporary suspension and modified hours"},
    {"id": "current", "date_iso": "2020-06-22", "short_label": "Current", "description": "Congestion Charge active"},
]

SINGAPORE_TIMELINE_EVENTS: List[dict] = [
    {"id": "erp_active", "date_iso": "2004-01-01", "short_label": "2004–Present", "description": "ERP congestion pricing active"},
    {"id": "erp2", "date_iso": "2024-05-01", "short_label": "May 2024", "description": "ERP 2.0 with new On-Board Units"},
]


def get_reference_date(city: str, label: str) -> date:
    """Return datetime.date for the given city and preset label."""
    refs = LONDON_REFERENCES if city == "london" else SINGAPORE_REFERENCES
    for l, d in refs:
        if l == label:
            return date.fromisoformat(d)
    raise ValueError(f"Unknown reference: {city} / {label}")
