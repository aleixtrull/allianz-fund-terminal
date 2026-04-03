"""
fetch_funds_full_history.py  [v6 — fondos + benchmarks]
=========================================================
Descarga el historico COMPLETO de NAV para los 47 fondos
y los 25 benchmarks relacionados.

Pipeline por fondo:
  1. mstarpy(ISIN)
  2. Yahoo Finance (busqueda ISIN → ticker → yfinance)
  3. OpenFIGI → ticker → mstarpy / yfinance
  4. Morningstar scrape por nombre

Benchmarks: descarga directa via yfinance (indices y ETFs).

Requisitos:
    pip install mstarpy yfinance pandas requests

Uso:
    python fetch_funds_full_history.py              # fondos + benchmarks
    python fetch_funds_full_history.py --only-benchmarks
    python fetch_funds_full_history.py --only-funds
    python fetch_funds_full_history.py --update     # solo nuevos datos
    python fetch_funds_full_history.py --isin LU1548497699
    python fetch_funds_full_history.py --debug
"""

import argparse
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
import pandas as pd

try:
    import mstarpy as ms
except ImportError:
    print("ERROR: pip install mstarpy yfinance pandas requests")
    raise SystemExit(1)

try:
    import yfinance as yf
except ImportError:
    print("ERROR: pip install yfinance")
    raise SystemExit(1)


# ── Catalogo de los 48 fondos ─────────────────────────────────────────────────

FUNDS = [
    {"isin": "IE00B3QDMK77", "name": "PIMCO GIS Euro Income Bond Fund",                    "cat": "Fixed Income"},
    {"isin": "LU3270886024", "name": "Allianz Europe Equity Crescendo",                     "cat": "Equity EU"},
    {"isin": "LU1861128574", "name": "Allianz Global Small Cap Equity",                     "cat": "Equity Global"},
    {"isin": "LU0348740696", "name": "Allianz India Equity",                                "cat": "Equity EM"},
    {"isin": "LU2208987508", "name": "Allianz ActiveInvest Balanced",                       "cat": "Mixed"},
    {"isin": "LU2208987680", "name": "Allianz ActiveInvest Dynamic",                        "cat": "Mixed"},
    {"isin": "LU2208987763", "name": "Allianz ActiveInvest Defensive",                      "cat": "Mixed"},
    {"isin": "ES0108192008", "name": "Allianz Bolsa Espanola",                               "cat": "Equity ES"},
    {"isin": "ES0108203003", "name": "Allianz Conservador Dinamico",                        "cat": "Mixed"},
    {"isin": "ES0108232002", "name": "Allianz Cartera Dinamica",                            "cat": "Mixed"},
    {"isin": "ES0108240005", "name": "Allianz Cartera Decidida",                            "cat": "Mixed"},
    {"isin": "ES0108193006", "name": "Allianz Cartera Bonos 26",                            "cat": "Mixed"},
    {"isin": "ES0108373004", "name": "Allianz Cartera Moderada",                            "cat": "Mixed"},
    {"isin": "LU0920839346", "name": "Allianz Europe Equity Growth Select",                 "cat": "Equity EU"},
    {"isin": "LU1254136416", "name": "Allianz Capital Plus",                                "cat": "Fixed Income"},
    {"isin": "LU0414045822", "name": "Allianz European Equity Dividend",                    "cat": "Equity EU"},
    {"isin": "LU1548497699", "name": "Allianz Global Artificial Intelligence",              "cat": "Equity Thematic"},
    {"isin": "LU0158827518", "name": "Allianz Global Sustainability",                       "cat": "Equity Global"},
    {"isin": "LU0256840447", "name": "Allianz Euroland Equity Growth",                      "cat": "Equity EU"},
    {"isin": "LU1363153823", "name": "Allianz US Short Duration High Yield Bond",           "cat": "Fixed Income"},
    {"isin": "LU1205638155", "name": "Allianz Advanced Fixed Income Euro",                  "cat": "Fixed Income"},
    {"isin": "LU1143164405", "name": "Allianz Japan Equity",                               "cat": "Equity JP"},
    {"isin": "LU1070113664", "name": "Allianz Income and Growth",                           "cat": "Mixed"},
    {"isin": "LU0348826909", "name": "Allianz All China Equity",                            "cat": "Equity EM"},
    {"isin": "LU1400637036", "name": "Allianz Best Styles Global Equity",                   "cat": "Equity Global"},
    {"isin": "LU1387904524", "name": "Allianz Best Styles Global AC Equity",                "cat": "Equity Global"},
    {"isin": "LU0933100983", "name": "Allianz Best Styles US Equity",                       "cat": "Equity US"},
    {"isin": "LU1019963369", "name": "Allianz Best Styles Europe Equity",                   "cat": "Equity EU"},
    {"isin": "LU1282651980", "name": "Allianz Emerging Markets Equity",                     "cat": "Equity EM"},
    {"isin": "IE00BDT57Y96", "name": "PIMCO GIS Low Duration Income Fund",                  "cat": "Fixed Income"},
    {"isin": "IE00B5B5L056", "name": "PIMCO GIS Dynamic Bond Fund",                         "cat": "Fixed Income"},
    {"isin": "IE00B1Z6D669", "name": "PIMCO GIS Diversified Income Fund",                   "cat": "Fixed Income"},
    {"isin": "LU2720182869", "name": "Allianz Target Maturity Euro Bond III",              "cat": "Fixed Income"},
    {"isin": "LU1890834598", "name": "Allianz Global Water",                                "cat": "Equity Thematic"},
    {"isin": "LU2286300988", "name": "Allianz Cyber Security",                            "cat": "Equity Thematic"},
    {"isin": "LU2364421367", "name": "Allianz Better World Moderate",                       "cat": "Mixed"},
    {"isin": "LU2387748960", "name": "Allianz Global Equity Unconstrained",                 "cat": "Equity Global"},
    {"isin": "LU2399975627", "name": "Allianz Better World Dynamic",                        "cat": "Mixed"},
    {"isin": "IE000HMO1R34", "name": "Objetivo Inflación 2030",                             "cat": "Fixed Income"},
    {"isin": "ES0108283005", "name": "Allianz Rendimiento",                                  "cat": "Fixed Income"},
    {"isin": "LU2958546181", "name": "Allianz Dynamic Allocation Plus Equity",              "cat": "Equity Global"},
    {"isin": "IE000U640GI8", "name": "Target 2025 II",                                      "cat": "Fixed Income"},
    {"isin": "LU0348784041", "name": "Allianz Oriental Income",                              "cat": "Equity EM"},
    {"isin": "LU2637966677", "name": "Allianz Target Maturity Euro Bond II",               "cat": "Fixed Income"},
    {"isin": "LU3034474745", "name": "Allianz Dynamic Multi Asset Strategy SRI 15", "cat": "Mixed"},
    {"isin": "LU3034474828", "name": "Allianz Dynamic Multi Asset Strategy SRI 30", "cat": "Mixed"},
    {"isin": "LU3034475049", "name": "Allianz Dynamic Multi Asset Strategy SRI 50", "cat": "Mixed"},
    {"isin": "LU3034475122", "name": "Allianz Dynamic Multi Asset Strategy SRI 75", "cat": "Mixed"},
    # ── Clases Unit Linked (UL) — share classes distintas comercializadas en seguro ──
    {"isin": "ES0108282007", "name": "Allianz Multi Asset Global 85",                       "cat": "Mixed"},
    {"isin": "LU1462192417", "name": "Allianz Dynamic Multi Asset Strategy SRI 50 CT",     "cat": "Mixed"},
    {"isin": "LU1462192250", "name": "Allianz Dynamic Multi Asset Strategy SRI 15 CT",     "cat": "Mixed"},
    {"isin": "LU1228143191", "name": "Allianz Best Styles US Equity CT",                    "cat": "Equity US"},
    {"isin": "LU0739342060", "name": "Allianz Income & Growth CT H2-EUR",                   "cat": "Mixed"},
    {"isin": "LU0920839429", "name": "Allianz Europe Equity Growth Select CT",              "cat": "Equity EU"},
    {"isin": "LU1602091867", "name": "Allianz Global Artificial Intelligence CT",           "cat": "Equity Thematic"},
    {"isin": "LU0348753244", "name": "Allianz Japan Equity CT",                             "cat": "Equity JP"},
    {"isin": "DE000A2DU1Y2", "name": "Kapital Plus",                                        "cat": "Mixed"},
    {"isin": "LU1462192680", "name": "Allianz Dynamic Multi Asset Strategy SRI 75 CT",     "cat": "Mixed"},
    {"isin": "LU0158828326", "name": "Allianz Global Sustainability CT",                    "cat": "Equity Global"},
    {"isin": "LU1459823677", "name": "Allianz Advanced Fixed Income Euro CT",               "cat": "Fixed Income"},
    {"isin": "LU1459823750", "name": "Allianz US Short Duration High Income Bond CT",       "cat": "Fixed Income"},
    {"isin": "LU0414046390", "name": "Allianz European Equity Dividend CT",                 "cat": "Equity EU"},
    {"isin": "LU0256840793", "name": "Allianz Euroland Equity Growth CT",                   "cat": "Equity EU"},
    {"isin": "LU2089280825", "name": "Allianz Best Styles Emerging Markets Equity",         "cat": "Equity EM"},
    {"isin": "LU2089281393", "name": "Allianz Best Styles Europe Equity CT",                "cat": "Equity EU"},
    {"isin": "LU2514904908", "name": "Allianz Better World Defensive CT2",                  "cat": "Mixed"},
    {"isin": "LU2514905038", "name": "Allianz Better World Moderate CT2",                   "cat": "Mixed"},
    {"isin": "LU2514905111", "name": "Allianz Better World Dynamic CT2",                    "cat": "Mixed"},
    {"isin": "LU2829845630", "name": "Allianz Dynamic Multi Asset Strategy SRI 30 CT",     "cat": "Mixed"},
]

# ── 25 Benchmarks via yfinance ────────────────────────────────────────────────
# Organized by asset class matching the fund universe
BENCHMARKS = [
    # ── RENTA VARIABLE GLOBAL ──────────────────────────────────────────
    {"id": "BM_MSCI_WORLD",    "ticker": "IWRD.L",    "name": "MSCI World (iShares ETF)",         "cat": "RV Global"},
    {"id": "BM_MSCI_ACWI",     "ticker": "ISAC.L",    "name": "MSCI ACWI (iShares ETF)",          "cat": "RV Global"},
    {"id": "BM_SP500",         "ticker": "^GSPC",      "name": "S&P 500",                          "cat": "RV EEUU"},
    {"id": "BM_SP500_ETF",     "ticker": "CSPX.L",    "name": "S&P 500 (iShares UCITS ETF)",      "cat": "RV EEUU"},
    {"id": "BM_NASDAQ100",     "ticker": "^NDX",       "name": "NASDAQ 100",                       "cat": "RV EEUU"},
    {"id": "BM_DOWJONES",      "ticker": "^DJI",       "name": "Dow Jones Industrial",             "cat": "RV EEUU"},
    # ── RENTA VARIABLE EUROPA ──────────────────────────────────────────
    {"id": "BM_STOXX50",       "ticker": "^STOXX50E",  "name": "Euro Stoxx 50",                    "cat": "RV Europa"},
    {"id": "BM_STOXX600",      "ticker": "^STOXX",     "name": "Stoxx Europe 600",                 "cat": "RV Europa"},
    {"id": "BM_IBEX35",        "ticker": "^IBEX",      "name": "IBEX 35",                          "cat": "RV España"},
    {"id": "BM_DAX",           "ticker": "^GDAXI",     "name": "DAX 40",                           "cat": "RV Europa"},
    {"id": "BM_CAC40",         "ticker": "^FCHI",      "name": "CAC 40",                           "cat": "RV Europa"},
    {"id": "BM_FTSE100",       "ticker": "^FTSE",      "name": "FTSE 100",                         "cat": "RV Europa"},
    {"id": "BM_EU_SMALLCAP",   "ticker": "ZPRX.DE",   "name": "SPDR MSCI Europe Small Cap ETF",   "cat": "RV Europa"},
    # ── RENTA VARIABLE ASIA / EMERGENTES ───────────────────────────────
    {"id": "BM_NIKKEI",        "ticker": "^N225",      "name": "Nikkei 225",                       "cat": "RV Asia"},
    {"id": "BM_MSCI_JAPAN",    "ticker": "IJPN.L",    "name": "MSCI Japan ETF (iShares)",         "cat": "RV Asia"},
    {"id": "BM_HSI",           "ticker": "^HSI",       "name": "Hang Seng Index",                  "cat": "RV Asia"},
    {"id": "BM_MSCI_EM",       "ticker": "IEEM.L",    "name": "MSCI Emerging Markets ETF",        "cat": "RV Emergentes"},
    {"id": "BM_MSCI_CHINA",    "ticker": "CNYA.L",    "name": "MSCI China ETF (iShares)",         "cat": "RV Emergentes"},
    {"id": "BM_MSCI_INDIA",    "ticker": "NDIA.L",    "name": "MSCI India ETF (iShares)",         "cat": "RV Emergentes"},
    # ── RENTA VARIABLE TEMÁTICA ────────────────────────────────────────
    {"id": "BM_CLEAN_ENERGY",  "ticker": "INRG.L",    "name": "iShares Global Clean Energy ETF",  "cat": "RV Tematico"},
    {"id": "BM_CYBERSEC",      "ticker": "CIBR",       "name": "First Trust Cyber Security ETF",   "cat": "RV Tematico"},
    {"id": "BM_WATER",         "ticker": "PHO",        "name": "Invesco Water Resources ETF",      "cat": "RV Tematico"},
    {"id": "BM_METALS",        "ticker": "XME",        "name": "SPDR S&P Metals & Mining ETF",     "cat": "RV Tematico"},
    # ── RENTA FIJA ─────────────────────────────────────────────────────
    {"id": "BM_EURO_AGG",      "ticker": "IEAG.L",    "name": "iShares Euro Aggregate Bond ETF",  "cat": "Renta Fija"},
    {"id": "BM_EUR_CORP",      "ticker": "IEAC.L",    "name": "iShares EUR Corp Bond ETF",        "cat": "Renta Fija"},
    {"id": "BM_EUR_HY",        "ticker": "IHYG.L",    "name": "iShares EUR High Yield Bond ETF",  "cat": "Renta Fija"},
    {"id": "BM_US_TREASURY",   "ticker": "IDTL.L",    "name": "iShares $ Treasury Bond 20yr ETF", "cat": "Renta Fija"},
]

HISTORY_START = datetime(1990, 1, 1)
HISTORY_END   = datetime.today()
OPENFIGI_URL  = "https://api.openfigi.com/v3/mapping"
YF_EXCHANGES  = [".F", ".DE", ".L", ".PA", ".MI", ".AS", ".SW", ".BE", ""]

# ── Fondos con fuente exclusiva Allianz GI (no disponibles en mstarpy/yfinance) ─
# Clave: ISIN  →  {slug: nombre-en-url, page_id: UUID datasource, table_id: UUID tabla}
ALLIANZGI_FUNDS = {
    "LU2208987508": {
        "slug":     "allianz-activeinvest-balanced-ct-eur",
        "page_id":  "43dc692d-f6cc-41f1-b44e-9f91901d0a5e",
        "table_id": "aab8dd70-0a03-4c7e-bd94-c0ae051b8b75",
    },
    "LU2208987680": {
        "slug":     "allianz-activeinvest-dynamic-ct-eur",
        "page_id":  "43dc692d-f6cc-41f1-b44e-9f91901d0a5e",
        "table_id": "aab8dd70-0a03-4c7e-bd94-c0ae051b8b75",
    },
    "LU2208987763": {
        "slug":     "allianz-activeinvest-defensive-ct-eur",
        "page_id":  "43dc692d-f6cc-41f1-b44e-9f91901d0a5e",
        "table_id": "aab8dd70-0a03-4c7e-bd94-c0ae051b8b75",
    },
}
AGI_BASE = "https://es.allianzgi.com"


# ══════════════════════════════════════════════════════════════════════════════
# SUPABASE CATALOG
# ══════════════════════════════════════════════════════════════════════════════

def fetch_catalog_from_supabase() -> list[dict] | None:
    """Fetch active funds from fund_catalog table via Supabase REST API.

    Returns a list of fund dicts with keys:
        isin, name, cat,
        agi_slug, agi_page_id, agi_table_id  (None when not an AllianzGI fund)

    Returns None if credentials are absent or the request fails — caller
    falls back to the hardcoded FUNDS / ALLIANZGI_FUNDS lists.
    """
    import os

    url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    if not url or not key:
        return None

    endpoint = f"{url}/rest/v1/fund_catalog"
    headers = {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
        "Accept":        "application/json",
    }
    params = {
        "select": "isin,name,cat,agi_slug,agi_page_id,agi_table_id",
        "active": "eq.true",
    }

    try:
        resp = requests.get(endpoint, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or not data:
            print("  [catalog] Supabase devolvió lista vacía — usando FUNDS local.")
            return None
        funds = [
            {
                "isin":         row["isin"],
                "name":         row.get("name") or row["isin"],
                "cat":          row.get("cat") or "Otro",
                "agi_slug":     row.get("agi_slug"),
                "agi_page_id":  row.get("agi_page_id"),
                "agi_table_id": row.get("agi_table_id"),
            }
            for row in data
            if row.get("isin")
        ]
        n_agi = sum(1 for f in funds if f["agi_slug"])
        print(f"  [catalog] {len(funds)} fondos activos desde Supabase ({n_agi} vía AllianzGI web).")
        return funds
    except Exception as exc:
        print(f"  [catalog] Error al consultar Supabase: {exc} — usando FUNDS local.")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def safe_filename(id_str: str, name: str) -> str:
    clean = (name.replace("/", "-").replace(" ", "_")
                 .replace(",", "").replace("(", "").replace(")", ""))
    return f"{id_str}_{clean[:40]}.csv"


def get_last_date(csv_path: Path):
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        return df.index.max().to_pydatetime()
    except Exception:
        return None


def nav_df_to_series(df: pd.DataFrame):
    df.columns = [c.lower() for c in df.columns]
    if "nav" not in df.columns or "date" not in df.columns:
        return None
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "nav"]).sort_values("date").set_index("date")
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    s = df["nav"].dropna()
    return s if len(s) > 1 else None


def merge_with_existing(new_df: pd.DataFrame, csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        return new_df
    try:
        existing = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        existing.columns = [c.lower() for c in existing.columns]
        combined = pd.concat([existing, new_df[~new_df.index.isin(existing.index)]])
        return combined.sort_index()
    except Exception:
        return new_df


def calc_metrics(series: pd.Series) -> dict:
    s = series.dropna()
    if len(s) < 2:
        return {}
    total_ret  = (s.iloc[-1] - s.iloc[0]) / s.iloc[0] * 100
    n_years    = (s.index[-1] - s.index[0]).days / 365.25
    cagr       = ((s.iloc[-1] / s.iloc[0]) ** (1 / n_years) - 1) * 100 if n_years > 0.1 else float("nan")
    dr         = s.pct_change().dropna()
    annual_vol = dr.std() * math.sqrt(252) * 100
    annual_ret = dr.mean() * 252 * 100
    sharpe     = (annual_ret / 100 - 0.04) / (annual_vol / 100) if annual_vol > 0 else float("nan")
    max_dd     = ((s - s.cummax()) / s.cummax() * 100).min()
    calmar     = abs(annual_ret / max_dd) if max_dd != 0 else float("nan")
    return {
        "first_date":     str(s.index[0].date()),
        "last_date":      str(s.index[-1].date()),
        "years_of_data":  round(n_years, 1),
        "data_points":    len(s),
        "total_return_%": round(total_ret, 2),
        "cagr_%":         round(cagr, 2),
        "annual_vol_%":   round(annual_vol, 2),
        "sharpe_ratio":   round(sharpe, 3),
        "max_drawdown_%": round(max_dd, 2),
        "calmar_ratio":   round(calmar, 3),
        "var_95_daily_%": round(dr.quantile(0.05) * 100, 3),
        "win_rate_%":     round((dr > 0).mean() * 100, 1),
        "last_nav":       round(float(s.iloc[-1]), 6),
    }


# ══════════════════════════════════════════════════════════════════════════════
# BENCHMARK DOWNLOAD via yfinance
# ══════════════════════════════════════════════════════════════════════════════

def fetch_benchmark(bm: dict, out_dir: Path, update_mode: bool) -> tuple:
    ticker = bm["ticker"]
    name   = bm["name"]
    bm_id  = bm["id"]
    csv_path = out_dir / safe_filename(bm_id, name)

    start_dt = HISTORY_START
    if update_mode:
        last = get_last_date(csv_path)
        if last:
            start_dt = last

    last_err = None
    for attempt in range(3):
        if attempt > 0:
            time.sleep(2 ** attempt)  # 2s, 4s backoff
        try:
            t    = yf.Ticker(ticker)
            hist = t.history(
                start=start_dt.strftime("%Y-%m-%d"),
                end=HISTORY_END.strftime("%Y-%m-%d"),
                auto_adjust=True,
                actions=False,
            )
            min_rows = 1 if update_mode else 5
            if hist is None or hist.empty or len(hist) < min_rows:
                last_err = f"yfinance returned empty data for {ticker}"
                continue

            series = hist["Close"].dropna()
            series.index = series.index.tz_localize(None)
            series.index = pd.to_datetime(series.index).normalize()
            series.name  = "nav"

            # Remove yfinance dividend-adjustment artifacts: days with an
            # impossible spike (>25%) that is immediately reversed the next day.
            pct = series.pct_change()
            nxt = pct.shift(-1)
            spikes = (pct.abs() > 0.25) & (nxt.abs() > 0.12) & (pct * nxt < 0)
            if spikes.any():
                bad = spikes[spikes].index
                print(f"    [outlier] {bm_id}: dropping {list(bad)}", file=sys.stderr)
                series = series.drop(bad)

            if update_mode and csv_path.exists():
                try:
                    existing = pd.read_csv(csv_path, index_col=0, parse_dates=True)
                    existing.columns = ["nav"]
                    new_rows = series[~series.index.isin(existing.index)]
                    series = pd.concat([existing["nav"], new_rows]).sort_index()
                except Exception:
                    pass

            series.to_frame("nav").to_csv(csv_path)

            info     = t.info
            currency = info.get("currency", "EUR") if info else "EUR"
            meta = {"id": bm_id, "name": name, "ticker": ticker, "category": bm["cat"], "currency": currency, "type": "benchmark"}
            return series, meta, None

        except Exception as e:
            last_err = str(e)

    return None, None, last_err


# ══════════════════════════════════════════════════════════════════════════════
# FUND PIPELINE (mstarpy → Yahoo → OpenFIGI → Morningstar scrape)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_via_mstarpy(term: str, start_dt: datetime):
    try:
        fund_obj = ms.Funds(term)
        history  = fund_obj.nav(start_dt, HISTORY_END)
        if not history:
            return None, None, None
        series = nav_df_to_series(pd.DataFrame(history))
        if series is None:
            return None, None, None
        name     = getattr(fund_obj, "name", None) or term
        currency = getattr(fund_obj, "currency", "EUR") or "EUR"
        return series, name, currency
    except Exception:
        return None, None, None


def yahoo_search_ticker(isin: str):
    url = (f"https://query2.finance.yahoo.com/v1/finance/search"
           f"?q={isin}&quotesCount=5&newsCount=0&enableFuzzyQuery=false"
           "&quotesQueryId=tss_match_phrase_query")
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json",
               "Referer": "https://finance.yahoo.com"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return []
        quotes = resp.json().get("quotes", [])
        tickers = []
        for q in quotes:
            sym = q.get("symbol", "")
            qt  = q.get("quoteType", "")
            if sym.startswith("0P") or qt in ("MUTUALFUND", "ETF", "FUND"):
                tickers.append(sym)
        for q in quotes:
            sym = q.get("symbol", "")
            if sym and sym not in tickers:
                tickers.append(sym)
        return tickers
    except Exception:
        return []


def fetch_via_yfinance(isin: str, name: str, start_dt: datetime, verbose: bool):
    tickers = yahoo_search_ticker(isin)
    for ticker in tickers[:6]:
        try:
            t    = yf.Ticker(ticker)
            hist = t.history(start=start_dt.strftime("%Y-%m-%d"),
                             end=HISTORY_END.strftime("%Y-%m-%d"),
                             auto_adjust=True, actions=False)
            if hist.empty or len(hist) < 5:
                continue
            series = hist["Close"].dropna()
            series.index = series.index.tz_localize(None)
            series.index = pd.to_datetime(series.index).normalize()
            if len(series) < 5:
                continue
            try:
                info     = t.info
                yf_name  = info.get("longName") or info.get("shortName") or name
                currency = info.get("currency") or "EUR"
            except Exception:
                yf_name  = name
                currency = "EUR"
            return series, yf_name, currency, f"yfinance (ticker={ticker})"
        except Exception:
            continue
    return None, None, None, "Yahoo Finance: sin datos"


def openfigi_lookup(isin: str, api_key: str):
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key
    payloads = [
        [{"idType": "ID_ISIN", "idValue": isin}],
        [{"idType": "ID_ISIN", "idValue": isin, "marketSecDes": "Fund"}],
        [{"idType": "ID_ISIN", "idValue": isin, "exchCode": "GY"}],
        [{"idType": "ID_ISIN", "idValue": isin, "exchCode": "LX"}],
    ]
    for payload in payloads:
        try:
            resp = requests.post(OPENFIGI_URL, headers=headers, json=payload, timeout=15)
            if resp.status_code == 429:
                time.sleep(12)
                resp = requests.post(OPENFIGI_URL, headers=headers, json=payload, timeout=15)
            if resp.status_code != 200:
                continue
            results = resp.json()
            if not results or "error" in results[0] or "data" not in results[0]:
                continue
            for item in results[0]["data"]:
                if item.get("ticker"):
                    return {"ticker": item["ticker"], "name": item.get("name"),
                            "exchCode": item.get("exchCode")}
        except Exception:
            continue
        time.sleep(1 if not api_key else 0.2)
    return None


def fetch_via_openfigi(isin: str, name: str, start_dt: datetime, api_key: str, verbose: bool):
    figi = openfigi_lookup(isin, api_key)
    if not figi:
        return None, None, None, "OpenFIGI: sin resultado"
    ticker = figi["ticker"]
    series, ms_name, currency = fetch_via_mstarpy(ticker, start_dt)
    if series is not None:
        return series, ms_name or figi.get("name") or name, currency, f"openfigi+mstarpy (ticker={ticker})"
    for exch in YF_EXCHANGES:
        yf_ticker = ticker + exch
        try:
            t    = yf.Ticker(yf_ticker)
            hist = t.history(start=start_dt.strftime("%Y-%m-%d"),
                             end=HISTORY_END.strftime("%Y-%m-%d"),
                             auto_adjust=True, actions=False)
            if not hist.empty and len(hist) > 5:
                series = hist["Close"].dropna()
                series.index = series.index.tz_localize(None)
                series.index = pd.to_datetime(series.index).normalize()
                if len(series) > 5:
                    return series, figi.get("name") or name, "EUR", f"openfigi+yfinance ({yf_ticker})"
        except Exception:
            continue
    return None, None, None, f"OpenFIGI ticker={ticker} sin datos"


def morningstar_search_secid(name: str):
    keywords = " ".join(name.split()[:4])
    url = (f"https://www.morningstar.es/es/util/SecuritySearch.ashx"
           f"?q={requests.utils.quote(keywords)}&limit=10&sites=es")
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list) and data[0].get("i"):
                return data[0]["i"]
    except Exception:
        pass
    return None


def morningstar_nav_by_secid(sec_id: str):
    url = (f"https://tools.morningstar.co.uk/api/rest.svc/timeseries_price/t92wz0sj7c"
           f"?currencyId=EUR&idtype=Morningstar&frequency=daily&outputType=COMPACTJSON"
           f"&startDate={HISTORY_START.strftime('%Y-%m-%d')}&id={sec_id}]2]0]FOESP$$ALL")
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data or not isinstance(data, list):
            return None
        df = pd.DataFrame(data, columns=["date", "nav"])
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna().sort_values("date").set_index("date")
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        s = df["nav"].dropna()
        return s if len(s) > 5 else None
    except Exception:
        return None


def fetch_via_allianzgi(isin: str, name: str) -> tuple:
    """Descarga NAV histórico desde es.allianzgi.com para fondos ActiveInvest.

    Flujo (descubierto via ingeniería inversa del JS de la web):
      1. POST  /api/funddata/exportpricesexcel/{slug}/{page_id}/{table_id}
             → JSON {"URL": "https://...exporthistoricaltableexcel/..."}
      2. GET   esa URL  → Excel (.xlsx) con hoja 'HistoricalPricesDataTable'
             Columnas: Fecha | Divisa | Precio emisión | Valor liquidativo | ...
    """
    cfg = ALLIANZGI_FUNDS.get(isin)
    if cfg is None:
        return None, None, None

    try:
        import openpyxl
    except ImportError:
        import subprocess, sys as _sys
        subprocess.check_call([_sys.executable, "-m", "pip", "install", "openpyxl", "-q"])
        import openpyxl

    fund_page_url = f"{AGI_BASE}/es-es/fondos/fondos/list/{cfg['slug']}?nav=historical-data"
    base_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "es-ES,es;q=0.9",
    }

    try:
        import io
        session = requests.Session()
        session.headers.update(base_headers)

        # Paso 0 — Visitar la página del fondo para establecer cookies de sesión
        session.get(fund_page_url, timeout=20)
        time.sleep(1)

        # Paso 1 — POST para obtener la URL temporal del Excel
        post_url = f"{AGI_BASE}/api/funddata/exportpricesexcel/{cfg['slug']}/{cfg['page_id']}/{cfg['table_id']}"
        r1 = session.post(
            post_url,
            headers={
                "Referer":          fund_page_url,
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type":     "application/json",
                "Accept":           "application/json, text/javascript, */*; q=0.01",
            },
            json={},
            timeout=20,
        )
        if r1.status_code != 200:
            return None, None, None
        data = r1.json()
        excel_url = data.get("URL")
        if not excel_url:
            return None, None, None

        # Paso 2 — GET del Excel con la misma sesión (cookies mantenidas)
        r2 = session.get(
            excel_url,
            headers={"Referer": fund_page_url, "Accept": "*/*"},
            timeout=30,
        )
        if r2.status_code != 200 or len(r2.content) < 100:
            return None, None, None

        # Paso 3 — Parsear Excel
        wb = openpyxl.load_workbook(io.BytesIO(r2.content), data_only=True)
        ws = wb.active
        records = []
        for row in ws.iter_rows(min_row=4, values_only=True):   # filas 1-3: cabeceras
            date_raw, _, _, nav_raw = row[0], row[1], row[2], row[3]
            if date_raw is None or nav_raw is None:
                continue
            try:
                date = pd.to_datetime(str(date_raw), dayfirst=True)
                nav  = float(str(nav_raw).replace(".", "").replace(",", "."))
                records.append((date, nav))
            except Exception:
                continue

        if not records:
            return None, None, None

        series = pd.Series(
            {d: v for d, v in records},
            name="nav",
            dtype=float,
        )
        series.index = pd.to_datetime(series.index)
        series = series.sort_index().dropna()
        return series, "EUR", "allianzgi/excel"

    except Exception:
        return None, None, None


def fetch_fund(fund: dict, out_dir: Path, update_mode: bool, api_key: str, verbose: bool):
    isin     = fund["isin"]
    name     = fund["name"]
    csv_path = out_dir / safe_filename(isin, name)

    start_dt = HISTORY_START
    if update_mode:
        last = get_last_date(csv_path)
        if last:
            start_dt = last

    # 0. Allianz GI web (fuente exclusiva para fondos ActiveInvest)
    series, currency, source = fetch_via_allianzgi(isin, name)
    ms_name = name
    if series is not None:
        # La web devuelve siempre el histórico completo; aplicamos filtro de fecha
        series = series[series.index >= pd.Timestamp(start_dt)]

    # 1. mstarpy
    if series is None:
        series, ms_name, currency = fetch_via_mstarpy(isin, start_dt)
        source = "mstarpy"

    # 2. Yahoo Finance
    if series is None:
        series, ms_name, currency, info = fetch_via_yfinance(isin, name, start_dt, verbose)
        source = info if series is not None else source

    # 3. OpenFIGI
    if series is None:
        series, ms_name, currency, info = fetch_via_openfigi(isin, name, start_dt, api_key, verbose)
        source = info if series is not None else source

    # 4. Morningstar scrape
    if series is None:
        sec_id = morningstar_search_secid(name)
        if sec_id:
            series = morningstar_nav_by_secid(sec_id)
            if series is not None:
                source = f"morningstar/scrape ({sec_id})"
                ms_name, currency = name, "EUR"

    if series is None:
        return None, None, "todos los metodos fallaron"

    df = series.to_frame("nav")
    df = merge_with_existing(df, csv_path)
    df.to_csv(csv_path)
    series = df["nav"].dropna()

    meta = {"isin": isin, "name": ms_name or name, "category": fund["cat"],
            "currency": currency or "EUR", "source": source, "type": "fund"}
    return series, meta, source


# ══════════════════════════════════════════════════════════════════════════════
# DEBUG
# ══════════════════════════════════════════════════════════════════════════════

def run_debug():
    print(f"\n  mstarpy  : {getattr(ms, '__version__', '?')}")
    print(f"  yfinance : {yf.__version__}")
    print()

    print("  [1] mstarpy test (LU1548497699)...", end=" ", flush=True)
    try:
        f = ms.Funds("LU1548497699")
        end = datetime.today()
        hist = f.nav(datetime(end.year - 1, end.month, end.day), end)
        print(f"OK — {f.name} — {len(hist)} filas")
    except Exception as e:
        print(f"ERROR: {e}")

    print("  [2] yfinance benchmark test (IWRD.L)...", end=" ", flush=True)
    try:
        t = yf.Ticker("IWRD.L")
        hist = t.history(period="1mo", auto_adjust=True, actions=False)
        print(f"OK — {len(hist)} filas — ultimo: {hist['Close'].iloc[-1]:.2f}")
    except Exception as e:
        print(f"ERROR: {e}")

    print("  [3] Yahoo search test (LU2208987508)...", end=" ", flush=True)
    tickers = yahoo_search_ticker("LU2208987508")
    print(f"OK — {tickers[:4]}" if tickers else "sin resultados")
    print()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Descarga NAV historico — 47 fondos + 25 benchmarks"
    )
    parser.add_argument("--isin",            default=None)
    parser.add_argument("--outdir",          default="nav_data")
    parser.add_argument("--update",          action="store_true")
    parser.add_argument("--delay",           type=float, default=1.5)
    parser.add_argument("--debug",           action="store_true")
    parser.add_argument("--verbose",         action="store_true")
    parser.add_argument("--only-funds",      action="store_true", dest="only_funds")
    parser.add_argument("--only-benchmarks", action="store_true", dest="only_benchmarks")
    parser.add_argument("--openfigi-key",    default="", dest="openfigi_key")
    parser.add_argument("--workers",         type=int, default=4, dest="workers",
                        help="Parallel workers for fund fetching (default: 4)")
    parser.add_argument("--data-dir",        default=".", dest="data_dir",
                        help="Directory for combined CSVs (nav_all_funds.csv, etc.) default: current dir")
    args = parser.parse_args()

    # En CI (GitHub Actions) forzar siempre modo --update para proteger el histórico
    import os
    if os.environ.get("CI") and not args.update and not args.debug:
        print("ERROR: En CI solo se permite --update. Usa --update para ejecutar.")
        raise SystemExit(1)

    if args.debug:
        run_debug()
        return

    out_dir  = Path(args.outdir)
    out_dir.mkdir(exist_ok=True)
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    run_funds      = not args.only_benchmarks
    run_benchmarks = not args.only_funds

    # ── Cargar catálogo desde Supabase (fuente de verdad) o fallback local ────
    _db_catalog = fetch_catalog_from_supabase()
    if _db_catalog:
        catalog        = _db_catalog
        catalog_source = "Supabase"
        # Reconstruir ALLIANZGI_FUNDS desde el catálogo (fuente de verdad)
        ALLIANZGI_FUNDS.clear()
        for _f in _db_catalog:
            if _f.get("agi_slug") and _f.get("agi_page_id") and _f.get("agi_table_id"):
                ALLIANZGI_FUNDS[_f["isin"]] = {
                    "slug":     _f["agi_slug"],
                    "page_id":  _f["agi_page_id"],
                    "table_id": _f["agi_table_id"],
                }
    else:
        catalog        = FUNDS
        catalog_source = "local (fallback)"
        # ALLIANZGI_FUNDS conserva sus valores hardcodeados como fallback

    mode_str = "ACTUALIZACION" if args.update else "HISTORICO COMPLETO (1990→hoy)"

    print()
    print("=" * 68)
    print("  Fund NAV Fetcher  v6  |  Fondos + Benchmarks")
    print("=" * 68)
    print(f"  Modo       : {mode_str}")
    print(f"  Fondos     : {len(catalog) if run_funds else 0}  (fuente: {catalog_source})")
    print(f"  Benchmarks : {len(BENCHMARKS) if run_benchmarks else 0}")
    print(f"  Salida     : {out_dir.resolve()}")
    print(f"  Pipeline   : mstarpy → yfinance → OpenFIGI → Morningstar scrape")
    print("=" * 68)
    print()

    all_navs  = {}
    meta_rows = []
    failed    = []

    # ── FONDOS ────────────────────────────────────────────────────────────────
    if run_funds:
        funds_to_fetch = (
            [f for f in catalog if f["isin"].upper() == args.isin.upper()]
            if args.isin else catalog
        )
        total = len(funds_to_fetch)
        workers = min(args.workers, total) if total > 0 else 1
        print(f"  ── FONDOS ({total}) — {workers} workers ──")

        results_map = {}  # isin → (series, meta, source)

        def _fetch_one(fund):
            return fund, fetch_fund(fund, out_dir, args.update, args.openfigi_key, args.verbose)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_fetch_one, f): f for f in funds_to_fetch}
            done_count = 0
            for future in as_completed(futures):
                done_count += 1
                fund, (series, meta, source) = future.result()
                isin = fund["isin"]
                name = fund["name"]
                results_map[isin] = (series, meta, source, name)
                status = "OK" if (series is not None and len(series) > 1) else "FALLO"
                if status == "OK":
                    src = f"[{source.split('(')[0].strip()[:14]}]"
                    print(f"  [{done_count:02d}/{total:02d}]  {isin}  {name[:36]:<36}  OK  {len(series):>5} filas  {series.index[0].date()} → {series.index[-1].date()}  {src}")
                else:
                    print(f"  [{done_count:02d}/{total:02d}]  {isin}  {name[:36]:<36}  FALLO")

        # Preserve FUNDS order in all_navs / meta_rows
        for fund in funds_to_fetch:
            isin = fund["isin"]
            series, meta, source, name = results_map[isin]
            if series is not None and len(series) > 1:
                all_navs[isin] = series
                meta_rows.append(meta)
            else:
                failed.append({"isin": isin, "name": name})
        print()

    # ── BENCHMARKS ────────────────────────────────────────────────────────────
    if run_benchmarks:
        print(f"  ── BENCHMARKS ({len(BENCHMARKS)}) ──")
        bm_navs  = {}
        bm_meta  = []
        bm_failed = []
        for i, bm in enumerate(BENCHMARKS, 1):
            print(f"  [{i:02d}/{len(BENCHMARKS):02d}]  {bm['ticker']:<12}  {bm['name'][:36]:<36}", end="  ", flush=True)
            series, meta, err = fetch_benchmark(bm, out_dir, args.update)
            if series is not None and len(series) > 5:
                bm_navs[bm["id"]] = series
                bm_meta.append(meta)
                print(f"OK  {len(series):>5} filas  {series.index[0].date()} → {series.index[-1].date()}")
            else:
                bm_failed.append(bm["id"])
                print(f"FALLO  ({err})")
            time.sleep(0.5)
        print()

    # ── RESUMEN ───────────────────────────────────────────────────────────────
    print("=" * 68)
    if run_funds:
        print(f"  Fondos descargados   : {len(all_navs)} / {len(FUNDS)}")
        if failed:
            print(f"  Fondos fallidos      : {len(failed)}")
            for f in failed:
                print(f"    - {f['isin']}  {f['name']}")
    if run_benchmarks:
        print(f"  Benchmarks descargados: {len(bm_navs)} / {len(BENCHMARKS)}")
        if bm_failed:
            print(f"  Benchmarks fallidos  : {', '.join(bm_failed)}")
    print("=" * 68)

    # ── CSV COMBINADOS ────────────────────────────────────────────────────────
    # nav_all_funds.csv — fondos (ISINs como columnas)
    if run_funds and all_navs:
        wide_path = data_dir / "nav_all_funds.csv"
        if wide_path.exists():
            try:
                existing = pd.read_csv(wide_path, index_col=0, parse_dates=True)
                new_df   = pd.DataFrame(all_navs)
                new_df.index = pd.to_datetime(new_df.index)
                combined = pd.concat([existing, new_df])
                wide = combined[~combined.index.duplicated(keep="last")].sort_index()
            except Exception:
                wide = pd.DataFrame(all_navs).sort_index()
                wide.index.name = "date"
        else:
            wide = pd.DataFrame(all_navs).sort_index()
            wide.index.name = "date"
        wide.to_csv(wide_path)
        print(f"\n  Fondos CSV      → {wide_path}  ({wide.shape[0]} filas × {wide.shape[1]} fondos)")

    # nav_benchmarks.csv — benchmarks (IDs como columnas)
    if run_benchmarks and bm_navs:
        bm_path = data_dir / "nav_benchmarks.csv"
        if bm_path.exists():
            try:
                existing_bm = pd.read_csv(bm_path, index_col=0, parse_dates=True)
                new_bm = pd.DataFrame(bm_navs)
                new_bm.index = pd.to_datetime(new_bm.index)
                # combine_first: new_bm wins where non-null, existing_bm fills the rest.
                # This preserves history of any benchmark that failed to fetch this run,
                # instead of overwriting its entire history with NaN.
                bm_wide = new_bm.combine_first(existing_bm).sort_index()
            except Exception:
                bm_wide = pd.DataFrame(bm_navs).sort_index()
                bm_wide.index.name = "date"
        else:
            bm_wide = pd.DataFrame(bm_navs).sort_index()
            bm_wide.index.name = "date"
        bm_wide.to_csv(bm_path)
        print(f"  Benchmarks CSV  → {bm_path}  ({bm_wide.shape[0]} filas × {bm_wide.shape[1]} benchmarks)")

    # ── CSV DE MÉTRICAS ───────────────────────────────────────────────────────
    rows = []
    if run_funds:
        for m in meta_rows:
            if m["isin"] in all_navs:
                rows.append({**m, **calc_metrics(all_navs[m["isin"]])})
    if run_benchmarks:
        for m in bm_meta:
            if m["id"] in bm_navs:
                rows.append({"isin": m["id"], **m, **calc_metrics(bm_navs[m["id"]])})

    if rows:
        summary = pd.DataFrame(rows).sort_values("total_return_%", ascending=False)
        summary.to_csv(data_dir / "nav_summary.csv", index=False)
        print(f"  Metricas CSV    → {data_dir / 'nav_summary.csv'}")

        cols = ["name", "type", "years_of_data", "total_return_%", "cagr_%", "sharpe_ratio", "max_drawdown_%"]
        c = [x for x in cols if x in summary.columns]

        print()
        print("  TOP 5 — Mayor rentabilidad total")
        print("  " + "-" * 63)
        print(summary[c].head(5).to_string(index=False))

        print()
        print("  TOP 5 — Mayor Sharpe ratio")
        print("  " + "-" * 63)
        print(summary.sort_values("sharpe_ratio", ascending=False)[c].head(5).to_string(index=False))

    print(f"\n  Listo. Archivos en: {data_dir.resolve()}\n")


if __name__ == "__main__":
    main()