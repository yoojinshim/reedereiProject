"""Normalize messy Open Positions vessel strings to canonical 'Nord Xxx' names."""

from __future__ import annotations

import re


_SPECIAL = {
    "STR": "Nord Star",
    "POL": "Nord Polaris",
    "HRZ": "Nord Horizon",
    "APX": "Nord Apex",
}


def _title_ship_part(rest: str) -> str:
    rest = rest.strip()
    if not rest:
        return ""
    parts = re.split(r"[\s_]+", rest)
    out = []
    for p in parts:
        if not p:
            continue
        if p.isupper() and len(p) > 1:
            out.append(p.capitalize())
        else:
            out.append(p[:1].upper() + p[1:].lower() if len(p) > 1 else p.upper())
    return " ".join(out)


def normalize_vessel(raw: str | None) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return s

    compact = re.sub(r"\s+", "", s).upper()
    if compact in _SPECIAL:
        return _SPECIAL[compact]

    m = re.match(r"^N[.\s\-]+(.+)$", s, re.I)
    if m:
        return "Nord " + _title_ship_part(m.group(1))

    m = re.match(r"^Nord\s*(.+)$", s, re.I)
    if m:
        rest = m.group(1).strip()
        if not rest:
            return "Nord"
        rest_spaced = re.sub(r"([a-z])([A-Z])", r"\1 \2", rest)
        return "Nord " + _title_ship_part(rest_spaced)

    m = re.match(r"^Nd\s*(.+)$", s, re.I)
    if m:
        return "Nord " + _title_ship_part(m.group(1))

    m = re.match(r"^ND\s+(.+)$", s, re.I)
    if m:
        return "Nord " + _title_ship_part(m.group(1))

    m = re.match(r"^ND([A-Za-z]+)$", s)
    if m:
        return "Nord " + _title_ship_part(m.group(1))

    if not re.search(r"\s", s):
        return "Nord " + _title_ship_part(s)

    return "Nord " + _title_ship_part(s)
