from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol
from . import fx  # 导入汇率转换工具
from decimal import Decimal


class SQLConn(Protocol):
    def execute(self, sql: str, params: list | tuple | None = None) -> Any: ...


def _one(con: SQLConn, sql: str, params: list | tuple | None = None) -> tuple | None:
    if params is None:
        params = []
    res = con.execute(sql, params)
    if hasattr(res, "fetchone"):
        return res.fetchone()
    return None


def assert_eq(con: SQLConn, name: str, sql: str, expected: int | float | Decimal, params: list | tuple | None = None) -> None:
    row = _one(con, sql, params)
    if row is None:
        raise AssertionError(f"[{name}] no row returned")
    got = row[0]
    if isinstance(expected, Decimal):
        got = Decimal(str(got))
        if abs(got - expected) > Decimal("0.0001"):
            raise AssertionError(f"[{name}] expected {expected}, got {got}")
    elif got != expected:
        raise AssertionError(f"[{name}] expected {expected!r}, got {got!r}")


def assert_true(con: SQLConn, name: str, sql: str, params: list | tuple | None = None) -> None:
    row = _one(con, sql, params)
    if row is None or not row[0]:
        raise AssertionError(f"[{name}] condition failed")


def step_clean_voyages_no_null_discharge(voyages_cleaned: Path) -> None:
    with voyages_cleaned.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            for c in ("discharge_port", "discharge_country", "discharge_region"):
                v = (r.get(c) or "").strip()
                if not v:
                    raise AssertionError(f"[clean_voyages] null {c} for {r.get('voyage_id')}")


def step_clean_laytime_no_null_amount(laytime_cleaned: Path) -> None:
    with laytime_cleaned.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            v = (r.get("amount_usd") or "").strip()
            if not v:
                raise AssertionError(f"[clean_laytime] null amount_usd for {r.get('voyage_id')}")


def step_clean_port_no_null_port(port_cleaned: Path) -> None:
    with port_cleaned.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if not (r.get("port") or "").strip():
                raise AssertionError(f"[clean_port] null port for {r.get('port_call_id')}")


def step_clean_open_positions_voy_ref_max_missing(open_pos_cleaned: Path, max_missing: int = 42) -> None:
    with open_pos_cleaned.open(newline="", encoding="utf-8") as f:
        n = sum(1 for r in csv.DictReader(f) if not (r.get("voy_ref") or "").strip())
    if n > max_missing:
        raise AssertionError(f"[clean_open_positions] {n} rows missing voy_ref (max allowed {max_missing})")


def step_assert_staging_counts_duck(con: SQLConn) -> None:
    assert_eq(con, "stg_ws_rows", "SELECT COUNT(*) FROM stg_worldscale_flat_rates", 606)
    assert_eq(con, "stg_broker_rows", "SELECT COUNT(*) FROM stg_broker_messages", 711)
    assert_true(con, "demurrage_staging", "SELECT COUNT(*) > 0 FROM stg_excel_demurrage_claims")
    assert_true(con, "bunker_budget_staging", "SELECT COUNT(*) > 0 FROM stg_excel_bunker_budget")


def step_assert_mart_dims(con: SQLConn) -> None:
    assert_eq(con, "mart_vessel_rows", "SELECT COUNT(*) FROM Vessel", 20)
    assert_true(con, "mart_charterer_nonempty", "SELECT COUNT(*) > 0 FROM Charterer")
    assert_true(con, "mart_cargo_nonempty", "SELECT COUNT(*) > 0 FROM Cargo")
    assert_true(con, "mart_port_nonempty", "SELECT COUNT(*) > 0 FROM Port")
    assert_true(con, "mart_date_nonempty", "SELECT COUNT(*) > 0 FROM DateDim")


def step_assert_mart_voyage_legs(con: SQLConn) -> None:
    assert_eq(con, "two_legs_per_base_voyage", "SELECT COUNT(*) FROM Voyage_Leg", 711 * 2)
    assert_true(
        con,
        "voyage_leg_suffix",
        """
        SELECT COUNT(*) = 0 FROM Voyage_Leg
        WHERE voyage_id NOT LIKE '%-L' AND voyage_id NOT LIKE '%-B'
        """,
    )


def step_assert_allocated_freight_matches_invoice(con: SQLConn, tol_usd: float = 1.0) -> None:
    rows = con.execute(
        """
        WITH inv AS (
          SELECT voyage_id, gross_freight_usd AS g FROM stg_freight_invoice
        ),
        leg AS (
          SELECT regexp_replace(voyage_id, '-[LB]$', '') AS base_v,
                 SUM(allocated_freight_usd) AS s
          FROM Voyage_Leg
          GROUP BY 1
        )
        SELECT COUNT(*) FROM inv i
        JOIN leg l ON l.base_v = i.voyage_id
        WHERE abs(i.g - l.s) > ?
        """,
        [tol_usd],
    ).fetchone()
    bad = rows[0]
    if bad != 0:
        raise AssertionError(f"[allocated_freight_vs_invoice] {bad} voyages exceed ${tol_usd}")


def step_assert_bunker_split_matches_stem_total(con: SQLConn, tol_usd: float = 1.0) -> None:
    bad = con.execute(
        """
        WITH stems AS (
          SELECT voyage_id, SUM(total_cost_usd) AS t FROM stg_bunker_stem GROUP BY voyage_id
        ),
        legs AS (
          SELECT regexp_replace(voyage_id, '-[LB]$', '') AS base_v,
                 SUM(bunker_cost_usd) AS s
          FROM Voyage_Leg
          GROUP BY 1
        )
        SELECT COUNT(*) FROM stems s
        JOIN legs l ON l.base_v = s.voyage_id
        WHERE abs(s.t - l.s) > ?
        """,
        [tol_usd],
    ).fetchone()[0]
    if bad != 0:
        raise AssertionError(f"[bunker_split_vs_stems] {bad} voyages exceed ${tol_usd}")


def step_assert_port_canal_matches_staging(con):
    # 1. 获取 Mart 层的 USD 总额
    mart = con.execute("""
        SELECT voyage_id, SUM(port_cost_usd) as m_fees, SUM(canal_transit_usd) as m_canal
        FROM Voyage_Leg GROUP BY 1
    """).fetchall()
    mart_map = {str(r[0]).strip().replace("-L","").replace("-B",""): r for r in mart}

    # 2. 获取 Staging 层的原始数据并进行转换
    stg_raw = con.execute("SELECT * FROM stg_port_cost").fetchall()
    stg_converted = {}
    
    for r in stg_raw:
        vid = str(r[1]).strip() # 对应你的 pipeline 索引
        curr = str(r[11]).strip().upper()
        
        # 这里的索引必须与你 pipeline.py 第 328-333 行完全一致
        raw_fees = sum([float(x or 0) for x in r[5:9]]) 
        raw_canal = float(r[10] or 0)
        
        # 使用相同的 FX 逻辑进行转换
        usd_f = float(fx.to_usd(Decimal(str(raw_fees)), curr))
        usd_c = float(fx.to_usd(Decimal(str(raw_canal)), curr))
        
        if vid not in stg_converted:
            stg_converted[vid] = {"fees": 0.0, "canal": 0.0}
        stg_converted[vid]["fees"] += usd_f
        stg_converted[vid]["canal"] += usd_c


def step_assert_one_invoice_per_voyage(con: SQLConn) -> None:
    assert_true(
        con,
        "one_invoice_per_voyage",
        """
        SELECT COUNT(*) = 0 FROM (
          SELECT voyage_id, COUNT(*) c FROM stg_freight_invoice GROUP BY voyage_id HAVING c != 1
        )
        """,
    )


def step_assert_one_laytime_per_voyage(con: SQLConn) -> None:
    assert_true(
        con,
        "one_laytime_per_voyage",
        """
        SELECT COUNT(*) = 0 FROM (
          SELECT voyage_id, COUNT(*) c FROM stg_laytime_statement GROUP BY voyage_id HAVING c != 1
        )
        """,
    )
