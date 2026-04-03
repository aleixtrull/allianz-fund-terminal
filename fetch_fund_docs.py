"""
fetch_fund_docs.py — Extrae documentos de fondos (KID, Factsheet, Folleto, Informes)
======================================================================================
Fuentes:
  - AllianzGI (es.allianzgi.com / lu.allianzgi.com): Playwright → ?nav=documents
  - CNMV (fondos españoles ES*): HTML estático → Folleto + DFI/KID
  - PIMCO (fondos IE*): redirect handler → Factsheet PDF

Salida: data/fund_docs.json
Formato: {ISIN: {doc_type: {title, url, updated_at}}}

Uso:
    python fetch_fund_docs.py               # actualiza todos
    python fetch_fund_docs.py --dry-run     # muestra cambios sin guardar
    python fetch_fund_docs.py --source allianz-lu
    python fetch_fund_docs.py --source cnmv
    python fetch_fund_docs.py --source pimco
    python fetch_fund_docs.py --data-dir data

Dependencias:
    pip install requests playwright
    playwright install chromium
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import date
from typing import Optional

import requests

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_DATA_DIR = "data"
OUTPUT_FILE      = "fund_docs.json"

TODAY = date.today().isoformat()

# ── Document type labels ───────────────────────────────────────────────────────

DOC_TYPE_LABELS = {
    "kid":               "KID",
    "factsheet":         "Ficha Mensual",
    "prospecto":         "Folleto",
    "informe_anual":     "Informe Anual",
    "informe_semestral": "Informe Semianual",
}

# AllianzGI data-filter → doc_type interno
ALLIANZ_DOC_MAP = {
    "PRIIP KID":           "kid",
    "Fichas Mensuales":    "factsheet",
    "Informe Anual":       "informe_anual",
    "Informe Semianual":   "informe_semestral",
    "Folleto Informativo": "prospecto",
    # lu.allianzgi.com (inglés)
    "Prospectus":          "prospecto",
    "Annual Report":       "informe_anual",
    "Semi-Annual Report":  "informe_semestral",
}

# ── Listas de fondos ──────────────────────────────────────────────────────────

# Fondos AllianzGI LU/IE → es.allianzgi.com (slug verificado)
ALLIANZ_LU_FUNDS = [
    ("LU0920839346", "Allianz Europe Equity Growth Select",         "allianz-europe-equity-growth-select-ct-eur"),
    ("LU1254136416", "Allianz Capital Plus",                        "allianz-capital-plus-ct-eur"),
    ("LU0414045822", "Allianz European Equity Dividend",            "allianz-european-equity-dividend-ct-eur"),
    ("LU1548497699", "Allianz Global Artificial Intelligence",      "allianz-global-artificial-intelligence-ct-eur"),
    ("LU0158827518", "Allianz Global Sustainability",               "allianz-global-sustainability-ct-eur"),
    ("LU0256840447", "Allianz Euroland Equity Growth",              "allianz-euroland-equity-growth-ct-eur"),
    ("LU1205638155", "Allianz Advanced Fixed Income Euro",          "allianz-advanced-fixed-income-euro-ct-eur"),
    ("LU1400637036", "Allianz Best Styles Global Equity",           "allianz-best-styles-global-equity-ct-eur"),
    ("LU1387904524", "Allianz Best Styles Global AC Equity",        "allianz-best-styles-global-ac-equity-ct-eur"),
    ("LU1019963369", "Allianz Best Styles Europe Equity",           "allianz-best-styles-europe-equity-ct-eur"),
    ("LU1282651980", "Allianz Emerging Markets Equity",             "allianz-emerging-markets-equity-ct-eur"),
    ("LU2720182869", "Allianz Target Maturity Euro Bond III",       "allianz-target-maturity-euro-bond-iii-ct-eur"),
    ("LU2637966677", "Allianz Target Maturity Euro Bond II",        "allianz-target-maturity-euro-bond-ii-ct-eur"),
    ("LU3034474745", "Allianz Dynamic Multi Asset Strategy SRI 15", "allianz-dynamic-multi-asset-strategy-sri-15-ct-eur"),
    ("LU1861128574", "Allianz Global Small Cap Equity",             "allianz-global-small-cap-equity-at-eur"),
    ("LU0348740696", "Allianz India Equity",                        "allianz-india-equity-at-eur"),
    ("LU0348826909", "Allianz All China Equity",                    "allianz-all-china-equity-at-eur"),
    ("LU0933100983", "Allianz Best Styles US Equity",               "allianz-best-styles-us-equity-at-eur"),
    ("LU1890834598", "Allianz Global Water",                        "allianz-global-water-at-eur"),
    ("LU2286300988", "Allianz Cyber Security",                      "allianz-cyber-security-at-eur"),
    ("LU0348784041", "Allianz Oriental Income",                     "allianz-oriental-income-at-eur"),
    ("LU1070113664", "Allianz Income and Growth",                   "allianz-income-and-growth-at-usd"),
    ("LU2364421367", "Allianz Better World Moderate",               "allianz-better-world-moderate-ct2-eur"),
    ("LU2399975627", "Allianz Better World Dynamic",                "allianz-better-world-dynamic-ct2-eur"),
    ("LU2958546181", "Allianz Dynamic Allocation Plus Equity",      "allianz-dynamic-allocation-plus-equity-ct2-eur"),
    ("LU3034474828", "Allianz Dynamic Multi Asset Strategy SRI 30", "allianz-dynamic-multi-asset-strategy-sri-30-ct2-eur"),
    ("LU3270886024", "Allianz Europe Equity Crescendo",             "allianz-europe-equity-crescendo-ct2-eur"),
    ("LU2208987508", "Allianz ActiveInvest Balanced",               "allianz-activeinvest-balanced-ct-eur"),
    ("LU2208987680", "Allianz ActiveInvest Dynamic",                "allianz-activeinvest-dynamic-ct-eur"),
    ("LU2208987763", "Allianz ActiveInvest Defensive",              "allianz-activeinvest-defensive-ct-eur"),
    ("LU1143164405", "Allianz Japan Equity",                        "allianz-japan-equity-at-eur"),
    ("LU2387748960", "Allianz Global Equity Unconstrained",         "allianz-global-equity-unconstrained-ct-eur"),
    ("LU3034475049", "Allianz Dynamic Multi Asset Strategy SRI 50", "allianz-dynamic-multi-asset-strategy-sri-50-ct2-eur"),
    ("LU3034475122", "Allianz Dynamic Multi Asset Strategy SRI 75", "allianz-dynamic-multi-asset-strategy-sri-75-ct2-eur"),
]

# Fondos AllianzGI solo disponibles en lu.allianzgi.com
ALLIANZ_LU_DOMAIN_FUNDS = [
    ("LU1363153823", "Allianz US Short Duration High Yield Bond",
     "allianz-us-short-duration-high-income-bond-at-h2-eur-eur"),
]

# Fondos PIMCO IE — factsheet via redirect handler
PIMCO_FUNDS = {
    "IE00B3QDMK77": "PIMCO GIS Euro Income Bond Fund",
    "IE00BDT57Y96": "PIMCO GIS Low Duration Income Fund",
    "IE00B5B5L056": "PIMCO GIS Dynamic Bond Fund",
    "IE00B1Z6D669": "PIMCO GIS Diversified Income Fund",
}
PIMCO_FACTSHEET_URL = "https://www.pimco.com/en-gb/handlers/displaydocument.ashx"

# Fondos españoles (domicilio ES) — CNMV: Folleto + DFI
CNMV_FUNDS = [
    ("ES0108192008", "Allianz Bolsa Española FI"),
    ("ES0108203003", "Allianz Conservador Dinámico"),
    ("ES0108232002", "Allianz Cartera Dinámica FI"),
    ("ES0108240005", "Allianz Cartera Decidida FI"),
    ("ES0108193006", "Allianz Cartera Bonos 26"),
    ("ES0108373004", "Allianz Cartera Moderada"),
    ("ES0108283005", "Allianz Rendimiento FI"),
]

# ── Playwright contexts ────────────────────────────────────────────────────────

_pw = None
_pw_browser = None
_pw_ctx = None
_pw_lu_ctx = None


def _ensure_pw_ctx():
    global _pw, _pw_browser, _pw_ctx
    if _pw_ctx is not None:
        return _pw_ctx
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  WARN: playwright no instalado — ejecuta: pip install playwright && playwright install chromium",
              file=sys.stderr)
        return None

    _pw = sync_playwright().start()
    _pw_browser = _pw.chromium.launch(headless=True)
    _pw_ctx = _pw_browser.new_context(
        locale="es-ES",
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    )
    # Aceptar cookies OneTrust y cerrar overlay de rol
    page = _pw_ctx.new_page()
    try:
        page.goto("https://es.allianzgi.com/es-es/fondos/fondos/list/",
                  wait_until="domcontentloaded", timeout=60000)
        try:
            page.click('#onetrust-accept-btn-handler', timeout=5000)
            page.wait_for_timeout(600)
        except Exception:
            pass
        page.evaluate("document.getElementById('roleTnCOverlay')?.remove()")
    except Exception as e:
        print(f"  WARN: init es.allianzgi.com falló ({e}), continuando sin aceptar cookies",
              file=sys.stderr)
    finally:
        page.close()
    return _pw_ctx


def _ensure_pw_lu_ctx():
    global _pw, _pw_browser, _pw_lu_ctx
    if _pw_lu_ctx is not None:
        return _pw_lu_ctx
    _ensure_pw_ctx()
    if _pw_browser is None:
        return None
    _pw_lu_ctx = _pw_browser.new_context(
        locale="en-GB",
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    )
    page = _pw_lu_ctx.new_page()
    try:
        page.goto("https://lu.allianzgi.com/en-lu/b2c/our-funds/funds/list/",
                  wait_until="domcontentloaded", timeout=60000)
        try:
            page.click('#onetrust-accept-btn-handler', timeout=5000)
            page.wait_for_timeout(600)
        except Exception:
            pass
        page.evaluate("document.getElementById('roleTnCOverlay')?.remove()")
    except Exception as e:
        print(f"  WARN: init lu.allianzgi.com falló ({e}), continuando sin aceptar cookies",
              file=sys.stderr)
    finally:
        page.close()
    return _pw_lu_ctx


def _stop_pw():
    global _pw, _pw_browser, _pw_ctx, _pw_lu_ctx
    if _pw_browser:
        try:
            _pw_browser.close()
        except Exception:
            pass
    if _pw:
        try:
            _pw.stop()
        except Exception:
            pass
    _pw = _pw_browser = _pw_ctx = _pw_lu_ctx = None


# ── AllianzGI scraper (Playwright, ?nav=documents) ────────────────────────────

def scrape_allianzgi_docs(slug: str,
                          base_url: str = "https://es.allianzgi.com/es-es/fondos/fondos/list"
                          ) -> dict[str, str]:
    """Extrae todos los documentos disponibles del fondo en AllianzGI.
    Devuelve {doc_type: pdf_url}.
    """
    is_lu = "lu.allianzgi.com" in base_url
    ctx = _ensure_pw_lu_ctx() if is_lu else _ensure_pw_ctx()
    if ctx is None:
        return {}

    url = f"{base_url}/{slug}?nav=documents"
    page = ctx.new_page()
    docs: dict[str, str] = {}
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.evaluate("document.getElementById('roleTnCOverlay')?.remove()")
        page.wait_for_timeout(1500)
        html = page.content()

        seen: set[str] = set()
        for row_filter, row_html in re.findall(
            r'<tr[^>]+data-filter="([^"]+)"[^>]*>(.*?)</tr>', html, re.DOTALL
        ):
            if row_filter == "all" or row_filter in seen:
                continue
            seen.add(row_filter)
            doc_type = ALLIANZ_DOC_MAP.get(row_filter)
            if not doc_type:
                continue
            # Captura URLs PDF de cualquier subdominio de allianzgi.com
            pdfs = re.findall(
                r'href="(https://[a-z]+\.allianzgi\.com/-/media/[^"]+\.pdf[^"]*)"',
                row_html
            )
            if pdfs:
                docs[doc_type] = pdfs[0].split("?")[0]
    except Exception as e:
        print(f"  WARN allianzgi {slug}: {e}", file=sys.stderr)
    finally:
        page.close()
    return docs


# ── CNMV scraper (HTML estático) ──────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    })
    return s


def scrape_cnmv(session: requests.Session, isin: str) -> dict[str, str]:
    """Extrae Folleto (prospecto) y DFI (KID) de la CNMV para un fondo español."""
    url = f"https://www.cnmv.es/portal/consultas/iic/fondo?isin={isin}"
    docs: dict[str, str] = {}
    try:
        r = session.get(url, timeout=20)
        if r.status_code != 200:
            return docs
        html = r.text
        for cell in re.finditer(r'<td[^>]+data-th="([^"]+)"[^>]*>(.*?)</td>', html, re.DOTALL):
            label   = cell.group(1)
            content = cell.group(2)
            link = re.search(
                r'href="(https://www\.cnmv\.es/webservices/verdocumento/ver\?[^"]+)"', content
            )
            if not link:
                continue
            if label == "Folleto":
                docs["prospecto"] = link.group(1)
            elif label.startswith("DFI"):
                docs["kid"] = link.group(1)
    except Exception as e:
        print(f"  WARN cnmv {isin}: {e}", file=sys.stderr)
    return docs


# ── PIMCO factsheet ────────────────────────────────────────────────────────────

def fetch_pimco_factsheet_url(isin: str, session: requests.Session) -> Optional[str]:
    """Sigue el redirect de PIMCO para obtener la URL final del factsheet PDF."""
    try:
        r = session.get(
            PIMCO_FACTSHEET_URL,
            params={"type": "FactSheet", "isin": isin},
            headers={"User-Agent": "Mozilla/5.0 (compatible)"},
            allow_redirects=True,
            timeout=20,
        )
        ct = r.headers.get("Content-Type", "")
        if r.status_code == 200 and ("pdf" in ct.lower() or r.url.lower().endswith(".pdf")):
            return r.url
        return None
    except Exception:
        return None


# ── Gestión del JSON de salida ────────────────────────────────────────────────

def load_docs(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_docs(path: str, data: dict):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def upsert_doc(docs: dict, isin: str, doc_type: str, title: str, url: str,
               dry_run: bool, stats: dict) -> str:
    """Inserta o actualiza un documento en el dict en memoria. Devuelve el estado."""
    existing = docs.get(isin, {}).get(doc_type)
    if existing:
        if existing["url"] == url:
            stats["unchanged"] += 1
            return "unchanged"
        if not dry_run:
            docs.setdefault(isin, {})[doc_type] = {"title": title, "url": url, "updated_at": TODAY}
        stats["updated"] += 1
        return "updated"
    else:
        if not dry_run:
            docs.setdefault(isin, {})[doc_type] = {"title": title, "url": url, "updated_at": TODAY}
        stats["inserted"] += 1
        return "inserted"


# ── Procesadores por fuente ───────────────────────────────────────────────────

def process_allianzgi_fund(isin: str, name: str, slug: str, docs: dict,
                           dry_run: bool, stats: dict,
                           base_url: str = "https://es.allianzgi.com/es-es/fondos/fondos/list"):
    scraped = scrape_allianzgi_docs(slug, base_url)
    if not scraped:
        print(f"  ✗ {isin}  sin documentos")
        stats["failed"] += 1
        return
    for doc_type, url in scraped.items():
        label  = DOC_TYPE_LABELS.get(doc_type, doc_type)
        title  = f"{label} - {name}"
        status = upsert_doc(docs, isin, doc_type, title, url, dry_run, stats)
        icon   = {"inserted": "✚", "updated": "↻", "unchanged": "·"}.get(status, "?")
        print(f"  {icon} {isin}  {doc_type:<20}  {status}")


def process_cnmv_fund(isin: str, name: str, docs: dict, session: requests.Session,
                      dry_run: bool, stats: dict):
    scraped = scrape_cnmv(session, isin)
    if not scraped:
        print(f"  ✗ {isin}  no encontrado en CNMV")
        stats["failed"] += 1
        return
    for doc_type, url in scraped.items():
        label  = DOC_TYPE_LABELS.get(doc_type, doc_type)
        title  = f"{label} - {name}"
        status = upsert_doc(docs, isin, doc_type, title, url, dry_run, stats)
        icon   = {"inserted": "✚", "updated": "↻", "unchanged": "·"}.get(status, "?")
        print(f"  {icon} {isin}  {doc_type:<20}  {status}")


def process_pimco_fund(isin: str, name: str, docs: dict, session: requests.Session,
                       dry_run: bool, stats: dict):
    url = fetch_pimco_factsheet_url(isin, session)
    if not url:
        print(f"  ✗ {isin}  factsheet no encontrado")
        stats["failed"] += 1
        return
    title  = f"Ficha Mensual - {name}"
    status = upsert_doc(docs, isin, "factsheet", title, url, dry_run, stats)
    icon   = {"inserted": "✚", "updated": "↻", "unchanged": "·"}.get(status, "?")
    print(f"  {icon} {isin}  factsheet             {status}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Extrae documentos de fondos a fund_docs.json")
    parser.add_argument("--dry-run",  action="store_true", help="Muestra cambios sin guardar")
    parser.add_argument("--source",   choices=["allianz-lu", "cnmv", "pimco", "all"], default="all")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR, help="Directorio de salida")
    args = parser.parse_args()

    output_path = os.path.join(args.data_dir, OUTPUT_FILE)
    docs   = load_docs(output_path)
    stats  = {"inserted": 0, "updated": 0, "unchanged": 0, "failed": 0}
    session = make_session()

    if args.source in ("allianz-lu", "all"):
        print("── AllianzGI LU/IE (es.allianzgi.com) ───────────────────")
        for isin, name, slug in ALLIANZ_LU_FUNDS:
            process_allianzgi_fund(isin, name, slug, docs, args.dry_run, stats)
            time.sleep(0.3)
        if ALLIANZ_LU_DOMAIN_FUNDS:
            print("── AllianzGI LU (lu.allianzgi.com) ───────────────────────")
            LU_BASE = "https://lu.allianzgi.com/en-lu/b2c/our-funds/funds/list"
            for isin, name, slug in ALLIANZ_LU_DOMAIN_FUNDS:
                process_allianzgi_fund(isin, name, slug, docs, args.dry_run, stats, base_url=LU_BASE)
                time.sleep(0.3)
        _stop_pw()

    if args.source in ("cnmv", "all"):
        print("── CNMV (fondos españoles) ───────────────────────────────")
        for isin, name in CNMV_FUNDS:
            process_cnmv_fund(isin, name, docs, session, args.dry_run, stats)
            time.sleep(0.5)

    if args.source in ("pimco", "all"):
        print("── PIMCO GIS (factsheets) ────────────────────────────────")
        for isin, name in PIMCO_FUNDS.items():
            process_pimco_fund(isin, name, docs, session, args.dry_run, stats)
            time.sleep(0.5)

    if not args.dry_run:
        save_docs(output_path, docs)
        print(f"\n✓ Guardado en {output_path}")
    else:
        print("\n[DRY RUN] Sin cambios guardados")

    print(f"Resumen — insertados:{stats['inserted']} actualizados:{stats['updated']} "
          f"sin cambios:{stats['unchanged']} fallidos:{stats['failed']}")


if __name__ == "__main__":
    main()
