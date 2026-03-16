"""
source_scraper.py  —  Petrol Prices via GlobalPetrolPrices.com
Saves to CSV in the data/ folder.
"""

import os, re, time, random, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

try:
    from config import NOW, COUNTRIES_EXPECTED, FRESHNESS_DAYS
except ImportError:
    NOW                = datetime.now()
    FRESHNESS_DAYS     = 7
    COUNTRIES_EXPECTED = ["India","USA","UK","Germany","France","Japan","Australia","Canada",
                          "Brazil","South Africa","Pakistan","China","Russia","Mexico",
                          "Turkey","Nigeria","Indonesia","Saudi Arabia","UAE","Egypt"]

# ── Save to your existing data/ folder ────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

GPP_BASE = "https://www.globalpetrolprices.com"

COUNTRY_SLUGS = {
    "India":"India","USA":"USA","UK":"United-Kingdom","Germany":"Germany",
    "France":"France","Japan":"Japan","Australia":"Australia","Canada":"Canada",
    "Brazil":"Brazil","South Africa":"South-Africa","Pakistan":"Pakistan",
    "China":"China","Russia":"Russia","Mexico":"Mexico","Turkey":"Turkey",
    "Nigeria":"Nigeria","Indonesia":"Indonesia","Saudi Arabia":"Saudi-Arabia",
    "UAE":"United-Arab-Emirates","Egypt":"Egypt",
}

HEADERS = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language":"en-US,en;q=0.9",
    "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer":"https://www.globalpetrolprices.com/",
}


def fetch() -> dict:
    meta = {"source":"Web Scraping — GlobalPetrolPrices.com",
            "method":"requests + BeautifulSoup","url":GPP_BASE,
            "fetched_at":NOW.isoformat(),"records":0,
            "missing_fields":0,"stale_records":0,"strategy_used":None}

    df = _scrape_country_pages(meta)
    if df is None or df.empty:
        print("  [Scrape] Live failed — using offline mock")
        df = _scrape_mock()
        meta["strategy_used"] = "mock_html"
        meta["url"]           = "local://mock"

    meta["records"]        = len(df)
    meta["missing_fields"] = int(df.isnull().sum().sum())
    cutoff = (NOW - timedelta(days=FRESHNESS_DAYS)).strftime("%Y-%m-%d")
    meta["stale_records"]  = int((df["last_updated"] < cutoff).sum())
    return {"df": df, "meta": meta}


def _scrape_country_pages(meta):
    print(" Scraping GPP country pages ...")
    rows, failed = [], []
    for country, slug in COUNTRY_SLUGS.items():
        url = f"{GPP_BASE}/{slug}/gasoline_prices/"
        try:
            r = requests.get(url, headers=HEADERS, timeout=8)
            if r.status_code == 404:
                failed.append((country, "404")); continue
            r.raise_for_status()
            soup  = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table", class_="graph_price_data_table") or soup.find("table")
            if not table:
                failed.append((country, "no table")); continue
            data_rows = [tr for tr in table.find_all("tr") if tr.find("td")]
            if not data_rows:
                failed.append((country, "empty table")); continue
            cells     = [td.get_text(strip=True) for td in data_rows[0].find_all("td")]
            date_str  = cells[0] if cells else NOW.strftime("%Y-%m-%d")
            usd_price = None
            for cell in reversed(cells):
                v = _to_float(cell)
                if v and 0.05 < v < 10:
                    usd_price = v; break
            rows.append({"country":country,"price_usd_per_liter":usd_price,
                         "currency":"USD","last_updated":_parse_date(date_str),
                         "source_note":"GlobalPetrolPrices.com"})
            time.sleep(0.3)
        except Exception as e:
            failed.append((country, str(e)[:50]))
    if failed:
        print(f"  Failed ({len(failed)}): " + ", ".join(f"{c}[{r}]" for c,r in failed))
    if rows:
        df = pd.DataFrame(rows)
        print(f"  [Scrape] ✓ {len(df)} countries, {df['price_usd_per_liter'].notna().sum()} with price")
        meta["strategy_used"] = "gpp_country_pages"
        return df
    return None


def _scrape_mock():
    random.seed(42)
    rows = []
    for i, c in enumerate(COUNTRIES_EXPECTED):
        rows.append({"country":c,
                     "price_usd_per_liter":round(random.uniform(0.28,2.20),3) if i not in [1,14] else None,
                     "currency":"USD",
                     "last_updated":(NOW-timedelta(days=random.randint(0,12))).strftime("%Y-%m-%d"),
                     "source_note":"Mock HTML (offline fallback)"})
    return pd.DataFrame(rows)


def _to_float(val):
    cleaned = re.sub(r"[^\d.\-]", "", str(val))
    try:    return float(cleaned) if cleaned else None
    except: return None


def _parse_date(raw):
    for fmt in ("%d-%b-%Y","%d %b %Y","%Y-%m-%d","%b %d, %Y","%d/%m/%Y"):
        try: return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError: continue
    return NOW.strftime("%Y-%m-%d")


def save(df: pd.DataFrame, tag: str = "scraper"):
    """Saves CSV to the data/ folder — no extra libraries needed."""
    os.makedirs(DATA_DIR, exist_ok=True)
    csv_path = os.path.join(DATA_DIR, f"{tag}.csv")
    df.to_csv(csv_path, index=False)
    print(f"  Saved → {csv_path}")
    return csv_path


if __name__ == "__main__":
    print("=" * 60)
    print("  SOURCE: GlobalPetrolPrices.com — country pages")
    print("=" * 60)
    print()

    result = fetch()
    df     = result["df"]

    print(f"\nStrategy : {result['meta']['strategy_used']}")
    print(f"Records  : {result['meta']['records']}")
    print(f"Missing  : {result['meta']['missing_fields']}")
    print(f"Stale    : {result['meta']['stale_records']}")
    print()

    if not df.empty:
        print(df[["country","price_usd_per_liter","currency","last_updated"]].to_string(index=False))
        print()

    save(df, tag="petrol_scraper")