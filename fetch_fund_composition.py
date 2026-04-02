#!/usr/bin/env python3
"""
fetch_fund_composition.py
Fetches composition data from Morningstar (via mstarpy) for all funds.

Outputs: data/fund_composition.json  (committed to git repo)

Usage:
    python fetch_fund_composition.py
    python fetch_fund_composition.py --isin LU1548497699   # single fund
    python fetch_fund_composition.py --workers 2 --data-dir data
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    sys.exit("ERROR: pip install python-dotenv")

load_dotenv()  # Carga .env automáticamente

try:
    import mstarpy as ms
except ImportError:
    sys.exit("ERROR: pip install mstarpy")

try:
    import pandas as pd
except ImportError:
    sys.exit("ERROR: pip install pandas")


# ── Fund catalogue (all funds from FUND_META) ─────────────────────────────────

FUNDS = [
    {"isin": "IE00B3QDMK77", "cat": "Fixed Income"},
    {"isin": "LU3270886024", "cat": "Equity EU"},
    {"isin": "LU1861128574", "cat": "Equity Global"},
    {"isin": "LU0348740696", "cat": "Equity EM"},
    {"isin": "LU2208987508", "cat": "Mixed"},
    {"isin": "LU2208987680", "cat": "Mixed"},
    {"isin": "LU2208987763", "cat": "Mixed"},
    {"isin": "ES0108192008", "cat": "Equity ES"},
    {"isin": "ES0108203003", "cat": "Mixed"},
    {"isin": "ES0108232002", "cat": "Mixed"},
    {"isin": "ES0108240005", "cat": "Mixed"},
    {"isin": "ES0108193006", "cat": "Mixed"},
    {"isin": "ES0108373004", "cat": "Mixed"},
    {"isin": "LU0920839346", "cat": "Equity EU"},
    {"isin": "LU1254136416", "cat": "Fixed Income"},
    {"isin": "LU0414045822", "cat": "Equity EU"},
    {"isin": "LU1548497699", "cat": "Equity Thematic"},
    {"isin": "LU0158827518", "cat": "Equity Global"},
    {"isin": "LU0256840447", "cat": "Equity EU"},
    {"isin": "LU1363153823", "cat": "Fixed Income"},
    {"isin": "LU1205638155", "cat": "Fixed Income"},
    {"isin": "LU1143164405", "cat": "Equity JP"},
    {"isin": "LU1070113664", "cat": "Mixed"},
    {"isin": "LU0348826909", "cat": "Equity EM"},
    {"isin": "LU1400637036", "cat": "Equity Global"},
    {"isin": "LU1387904524", "cat": "Equity Global"},
    {"isin": "LU0933100983", "cat": "Equity US"},
    {"isin": "LU1019963369", "cat": "Equity EU"},
    {"isin": "LU1282651980", "cat": "Equity EM"},
    {"isin": "IE00BDT57Y96", "cat": "Fixed Income"},
    {"isin": "IE00B5B5L056", "cat": "Fixed Income"},
    {"isin": "IE00B1Z6D669", "cat": "Fixed Income"},
    {"isin": "LU2720182869", "cat": "Fixed Income"},
    {"isin": "LU1890834598", "cat": "Equity Thematic"},
    {"isin": "LU2286300988", "cat": "Equity Thematic"},
    {"isin": "LU2364421367", "cat": "Mixed"},
    {"isin": "LU2387748960", "cat": "Equity Global"},
    {"isin": "LU2399975627", "cat": "Mixed"},
    {"isin": "IE000HMO1R34", "cat": "Fixed Income"},
    {"isin": "ES0108283005", "cat": "Fixed Income"},
    {"isin": "LU2958546181", "cat": "Equity Global"},
    {"isin": "IE000U640GI8", "cat": "Fixed Income"},
    {"isin": "LU0348784041", "cat": "Equity EM"},
    {"isin": "LU2637966677", "cat": "Fixed Income"},
    {"isin": "LU3034474745", "cat": "Mixed"},
    {"isin": "LU3034474828", "cat": "Mixed"},
    {"isin": "LU3034475049", "cat": "Mixed"},
    {"isin": "LU3034475122", "cat": "Mixed"},
    # CT / UL-only funds
    {"isin": "LU2089280825", "cat": "Equity EM"},
    {"isin": "LU0920839429", "cat": "Equity EU"},
    {"isin": "LU2089281393", "cat": "Equity EU"},
    {"isin": "LU0256840793", "cat": "Equity EU"},
    {"isin": "LU0414046390", "cat": "Equity EU"},
    {"isin": "LU0158828326", "cat": "Equity Global"},
    {"isin": "LU0348753244", "cat": "Equity JP"},
    {"isin": "LU1602091867", "cat": "Equity Thematic"},
    {"isin": "LU1228143191", "cat": "Equity US"},
    {"isin": "LU1459823677", "cat": "Fixed Income"},
    {"isin": "LU1459823750", "cat": "Fixed Income"},
    {"isin": "LU1462192250", "cat": "Mixed"},
    {"isin": "LU1462192680", "cat": "Mixed"},
    {"isin": "LU0739342060", "cat": "Mixed"},
    {"isin": "DE000A2DU1Y2", "cat": "Mixed"},
    {"isin": "LU2514904908", "cat": "Mixed"},
    {"isin": "LU2514905111", "cat": "Mixed"},
    {"isin": "LU2514905038", "cat": "Mixed"},
    {"isin": "LU2829845630", "cat": "Mixed"},
    {"isin": "LU1462192417", "cat": "Mixed"},
]

SECTOR_KEYS_EQ = [
    "basicMaterials", "consumerCyclical", "financialServices", "realEstate",
    "communicationServices", "energy", "industrials", "technology",
    "consumerDefensive", "healthcare", "utilities",
]
SECTOR_KEYS_FI = [
    "government", "municipal", "corporate", "securitized", "cashAndEquivalents",
]
GEO_KEYS = [
    "northAmerica", "unitedKingdom", "europeDeveloped", "europeEmerging",
    "africaMiddleEast", "japan", "australasia", "asiaDeveloped",
    "asiaEmerging", "latinAmerica",
]


def _safe(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception:
        return None


def _f(val, decimals=2):
    """Safe float conversion."""
    try:
        v = float(val or 0)
        return round(v, decimals)
    except Exception:
        return 0.0


def fetch_composition(isin: str) -> dict:
    """Fetch all composition data for a single fund ISIN."""
    data: dict = {"updated": datetime.today().strftime("%Y-%m-%d")}

    try:
        f = ms.Funds(isin)
    except Exception as e:
        print(f"  [SKIP] {isin}: no Morningstar match — {e}")
        return data

    # ── Snapshot: rating (stars), AUM, SRRI ──────────────────────────────────
    snap = _safe(f.snapshot)
    if snap:
        rr = snap.get("RiskAndRating") or []
        # Prefer M36 (3-year rating), fall back to first available
        rating_val = None
        for r in rr:
            if r.get("TimePeriod") == "M36":
                rating_val = r.get("RatingValue")
                break
        if rating_val is None and rr:
            rating_val = rr[0].get("RatingValue")
        data["rating"] = int(rating_val) if rating_val is not None else None

        # AUM (Net Asset Value of the fund class in EUR/USD)
        nav_vals = snap.get("NetAssetValues") or []
        if nav_vals:
            v = nav_vals[0]
            raw = v.get("DayEndValue") or v.get("MonthEndValue")
            if raw:
                data["aum_m"] = round(float(raw) / 1e6, 1)
                data["aum_currency"] = v.get("CurrencyId", "EUR")

        # SRRI: Synthetic Risk and Reward Indicator (1–7)
        srri = snap.get("CollectedSRRI") or snap.get("CalculatedSRRI")
        if srri:
            data["srri"] = int(srri.get("Rank", 0)) or None

    # ── Asset allocation (equity / bonds / cash / other) ─────────────────────
    alloc = _safe(f.allocationMap)
    if alloc and "allocationMap" in alloc:
        am = alloc["allocationMap"]
        us_eq  = _f(am.get("AssetAllocUSEquity",    {}).get("netAllocation"))
        non_eq = _f(am.get("AssetAllocNonUSEquity", {}).get("netAllocation"))
        bond   = _f(am.get("AssetAllocBond",        {}).get("netAllocation"))
        cash   = _f(am.get("AssetAllocCash",        {}).get("netAllocation"))
        nc     = _f(am.get("AssetAllocNotClassified",{}).get("netAllocation"))
        other  = _f(am.get("AssetAllocOther",       {}).get("netAllocation"))
        data["asset_alloc"] = {
            "equity": round(us_eq + non_eq, 2),
            "bonds":  bond,
            "cash":   cash,
            "other":  round(nc + other, 2),
        }

    # ── Geographic / regional allocation ─────────────────────────────────────
    geo = _safe(f.regionalSector)
    if geo and "fundPortfolio" in geo:
        fp = geo["fundPortfolio"]
        gd = {k: _f(fp.get(k)) for k in GEO_KEYS if fp.get(k, 0)}
        if gd:
            data["geo"] = gd

    # ── Sector allocation ─────────────────────────────────────────────────────
    sector_raw = _safe(f.sector)
    if sector_raw:
        asset_type = sector_raw.get("assetType", "EQUITY")
        sub = sector_raw.get(asset_type, {})
        fp = sub.get("fundPortfolio") if isinstance(sub, dict) else None
        if fp:
            keys = SECTOR_KEYS_EQ if asset_type == "EQUITY" else SECTOR_KEYS_FI
            sd = {k: _f(fp.get(k)) for k in keys if fp.get(k, 0)}
            if sd:
                data["sectors"] = {"type": asset_type, "data": sd}

    # ── Top 10 holdings ───────────────────────────────────────────────────────
    holdings_raw = _safe(f.holdings)  # returns top 10 by default
    if holdings_raw is not None:
        try:
            df = holdings_raw if isinstance(holdings_raw, pd.DataFrame) else pd.read_json(holdings_raw)
            h_list = []
            for _, row in df.iterrows():
                isin_val = row.get("isin")
                country  = row.get("country")
                sector_v = row.get("sector")
                h_list.append({
                    "name":    str(row.get("securityName", "—")),
                    "isin":    str(isin_val)  if pd.notna(isin_val)  else None,
                    "weight":  _f(row.get("weighting")),
                    "country": str(country)   if pd.notna(country)   else None,
                    "sector":  str(sector_v)  if pd.notna(sector_v)  else None,
                })
            if h_list:
                data["holdings"] = h_list
        except Exception:
            pass

    # ── Equity style (growth vs value metrics) ────────────────────────────────
    eq_style = _safe(f.equityStyle)
    if eq_style and "fund" in eq_style:
        ef = eq_style["fund"]
        data["equity_style"] = {
            "earningsGrowth":  _f(ef.get("forecastedEarningsGrowth")),
            "bookValueGrowth": _f(ef.get("forecastedBookValueGrowth")),
            "revenueGrowth":   _f(ef.get("forecastedRevenueGrowth")),
            "dividendYield":   _f(ef.get("prospectiveDividendYield")),
        }

    # ── Market capitalisation breakdown ───────────────────────────────────────
    mc = _safe(f.marketCapitalization)
    if mc and "fund" in mc:
        mf = mc["fund"]
        data["market_cap"] = {
            "avgCapB":  round(_f(mf.get("avgMarketCap")) / 1000, 1),  # USD M → B
            "giant":    _f(mf.get("giant")),
            "large":    _f(mf.get("large")),
            "medium":   _f(mf.get("medium")),
            "small":    _f(mf.get("small")),
            "micro":    _f(mf.get("micro")),
        }

    # ── Ownership zone (style box position, 0–500 scale) ─────────────────────
    oz = _safe(f.ownershipZone)
    if oz and "fund" in oz:
        of_ = oz["fund"]
        data["ownership_zone"] = {
            "sizeScore":  _f(of_.get("scaledSizeScore")),
            "styleScore": _f(of_.get("scaledStyleScore")),
        }

    # ── Fixed income style (bond funds) ──────────────────────────────────────
    fi_style = _safe(f.fixedIncomeStyle)
    if fi_style and "fund" in fi_style:
        ff = fi_style["fund"]
        data["fi_style"] = {
            "duration":      (_f(ff["avgEffectiveDuration"]) if ff.get("avgEffectiveDuration") else None),
            "maturity":      (_f(ff["avgEffectiveMaturity"])  if ff.get("avgEffectiveMaturity")  else None),
            "creditQuality": ff.get("avgCreditQualityName"),
            "ytm":           (_f(ff["yieldToMaturity"])       if ff.get("yieldToMaturity")       else None),
            "styleBox":      fi_style.get("fixedIncStyleBox"),
        }

    # ── Credit quality breakdown (bond funds) ─────────────────────────────────
    cq = _safe(f.creditQuality)
    if cq and "fund" in cq:
        cf = cq["fund"]
        cq_map = {
            "AAA": "creditQualityAAA", "AA": "creditQualityAA", "A": "creditQualityA",
            "BBB": "creditQualityBBB", "BB": "creditQualityBB", "B": "creditQualityB",
            "belowB": "creditQualityBelowB", "notRated": "creditQualityNotRated",
        }
        cd = {k: _f(cf.get(v)) for k, v in cq_map.items() if cf.get(v, 0)}
        if cd:
            data["credit_quality"] = cd

    return data


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Morningstar composition data for all funds")
    parser.add_argument("--isin",    help="Fetch a single ISIN only")
    parser.add_argument("--workers", type=int, default=2, help="Parallel workers (default: 2)")
    parser.add_argument("--data-dir", default="data", help="Output directory (default: data)")
    args = parser.parse_args()

    funds_to_fetch = (
        [f for f in FUNDS if f["isin"].upper() == args.isin.upper()]
        if args.isin else FUNDS
    )
    if not funds_to_fetch:
        sys.exit(f"ISIN {args.isin!r} not found in catalogue")

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    output_file = data_dir / "fund_composition.json"

    # Load existing data to preserve ISINs not being re-fetched
    existing: dict = {}
    if output_file.exists():
        try:
            existing = json.loads(output_file.read_text())
        except Exception:
            pass
    results = dict(existing)

    print("=" * 64)
    print(f"  Fondos      : {len(funds_to_fetch)}")
    print(f"  Workers     : {args.workers}")
    print(f"  Salida      : {output_file}")
    print("=" * 64)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_composition, f["isin"]): f["isin"] for f in funds_to_fetch}
        done = 0
        for future in as_completed(futures):
            isin = futures[future]
            try:
                data = future.result()
                results[isin] = data
                rating  = data.get("rating", "—")
                aum     = data.get("aum_m")
                aum_str = f"{aum:.0f}M" if aum else "—"
                done += 1
                print(f"  [{done:02d}/{len(funds_to_fetch):02d}]  {isin}  ★{rating}  AUM:{aum_str}")
            except Exception as e:
                print(f"  [ERR] {isin}: {e}")
            time.sleep(0.5)  # polite rate-limiting between result collections

    output_file.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    kb = output_file.stat().st_size // 1024
    print(f"\nGuardado en {output_file}  ({len(results)} fondos, {kb} KB)")
    print("Listo.")


if __name__ == "__main__":
    main()
