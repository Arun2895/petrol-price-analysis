"""
source_api.py  —  World Bank API v2 — International Petrol Prices
═══════════════════════════════════════════════════════════════════

SOURCE VERDICT
──────────────────────────────────────────────────────────────────
  ✅  WINNER: World Bank Indicators API v2
      URL    : https://api.worldbank.org/v2/
      Why    : Free, no API key, 150+ countries, JSON, official data
      Indicator: EP.PMP.SGAS.CD  — Pump price for gasoline (USD/litre)
               : EP.PMP.DESL.CD  — Pump price for diesel  (USD/litre)
      Coverage: Annual data, most countries updated to 2022-2023
      Docs   : https://datahelpdesk.worldbank.org/knowledgebase/articles/889392

  ❌  EIA (api.eia.gov)          — USA only
  ❌  GlobalPetrolPrices API     — paid subscription required
  ❌  OilPriceAPI                — crude oil only (Brent/WTI), not retail pump
  ❌  CollectAPI / RapidAPI      — USA/Europe only, 10 req/month free tier
  ❌  Barchart OnDemand          — USA zip-code level only
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta

try:
    from config import NOW, FRESHNESS_DAYS
except ImportError:
    NOW            = datetime.now()
    FRESHNESS_DAYS = 7

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
)

# ── World Bank API ─────────────────────────────────────────────────────────────
WB_BASE = "https://api.worldbank.org/v2"

INDICATORS = {
    "EP.PMP.SGAS.CD": "Gasoline pump price (USD/litre)",
    "EP.PMP.DESL.CD": "Diesel pump price (USD/litre)",
}

# 20 countries — ISO2 codes for World Bank API
COUNTRIES = {
    "IN": "India",         "US": "USA",           "GB": "UK",
    "DE": "Germany",       "FR": "France",         "JP": "Japan",
    "AU": "Australia",     "CA": "Canada",         "BR": "Brazil",
    "ZA": "South Africa",  "PK": "Pakistan",       "CN": "China",
    "RU": "Russia",        "MX": "Mexico",         "TR": "Turkey",
    "NG": "Nigeria",       "ID": "Indonesia",      "SA": "Saudi Arabia",
    "AE": "UAE",           "EG": "Egypt",
}


def fetch() -> dict:
    meta = {
        "source":         "World Bank API v2 — Pump Prices",
        "method":         "requests.get(api.worldbank.org/v2)",
        "url":            WB_BASE,
        "fetched_at":     NOW.isoformat(),
        "records":        0,
        "missing_fields": 0,
        "stale_records":  0,
        "strategy_used":  None,
    }

    df = _fetch_worldbank(meta)

    if df is None or df.empty:
        print("  [API] World Bank fetch failed — using mock data")
        df = _mock_data()
        meta["strategy_used"] = "mock"

    meta["records"]        = len(df)
    meta["missing_fields"] = int(df.isnull().sum().sum())
    cutoff = (NOW - timedelta(days=FRESHNESS_DAYS)).strftime("%Y-%m-%d")
    meta["stale_records"]  = int((df["last_updated"] < cutoff).sum())
    return {"df": df, "meta": meta}


def _fetch_worldbank(meta: dict):
    """
    Calls World Bank Indicators API for gasoline + diesel pump prices.
    Endpoint pattern:
      /v2/country/{iso2_codes}/indicator/{indicator}?format=json&mrv=1&per_page=100
      mrv=1  → most recent value only (1 year)
    """
    all_rows = []
    iso_codes = ";".join(COUNTRIES.keys())   # e.g. "IN;US;GB;DE;..."

    for indicator_code, indicator_label in INDICATORS.items():
        url = (
            f"{WB_BASE}/country/{iso_codes}/indicator/{indicator_code}"
            f"?format=json&mrv=1&per_page=100"
        )
        print(f"  [API] Fetching: {indicator_label}")
        print(f"        {url}")

        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            payload = r.json()

            if not isinstance(payload, list) or len(payload) < 2:
                print(f"  [API] Unexpected response format")
                continue

            data = payload[1]
            if not data:
                print(f"  [API] No data returned for {indicator_code}")
                continue

            print(f"  [API] {len(data)} rows received")

            for item in data:
                value = item.get("value")
                if value is None:
                    continue

                iso2    = item.get("countryiso3code", "")[:2]  
                iso2    = item.get("country", {}).get("id", "")
                country = COUNTRIES.get(iso2, item.get("country", {}).get("value", iso2))
                year    = str(item.get("date", NOW.year))

                all_rows.append({
                    "country":             country,
                    "iso2":                iso2,
                    "indicator":           indicator_label,
                    "indicator_code":      indicator_code,
                    "price_usd_per_liter": round(float(value), 4),
                    "currency":            "USD",
                    "last_updated":        f"{year}-12-31",   # annual data
                    "source_note":         "World Bank API v2",
                })

        except Exception as e:
            print(f"  [API] Failed for {indicator_code}: {e}")
            continue

    if all_rows:
        df = pd.DataFrame(all_rows)
        
        gasoline = df[df["indicator_code"] == "EP.PMP.SGAS.CD"][
            ["country", "iso2", "price_usd_per_liter", "last_updated", "source_note"]
        ].rename(columns={"price_usd_per_liter": "gasoline_usd_per_liter"})

        diesel = df[df["indicator_code"] == "EP.PMP.DESL.CD"][
            ["country", "iso2", "price_usd_per_liter"]
        ].rename(columns={"price_usd_per_liter": "diesel_usd_per_liter"})

        merged = gasoline.merge(diesel, on=["country", "iso2"], how="outer")
        merged["currency"] = "USD"

        ok = merged["gasoline_usd_per_liter"].notna().sum()
        print(f"  [API] ✓ World Bank OK — {len(merged)} countries, {ok} with gasoline price")
        meta["strategy_used"] = "worldbank_api"
        return merged

    return None


def _mock_data() -> pd.DataFrame:
    """
    Realistic mock — World Bank annual pump prices (approximate 2023 values).
    These reflect actual World Bank reported figures.
    """
    data = [
        ("India",        "IN", 1.02, 0.89),
        ("USA",          "US", 0.99, 1.01),
        ("UK",           "GB", 1.80, 1.85),
        ("Germany",      "DE", 1.94, 1.82),
        ("France",       "FR", 1.88, 1.75),
        ("Japan",        "JP", 1.28, 1.31),
        ("Australia",    "AU", 1.33, 1.40),
        ("Canada",       "CA", 1.20, 1.25),
        ("Brazil",       "BR", 1.17, 1.09),
        ("South Africa", "ZA", 1.05, 1.08),
        ("Pakistan",     "PK", 0.88, 0.91),
        ("China",        "CN", 1.16, 1.05),
        ("Russia",       "RU", 0.62, 0.72),
        ("Mexico",       "MX", 1.11, 1.08),
        ("Turkey",       "TR", 1.55, 1.48),
        ("Nigeria",      "NG", 0.54, 0.59),
        ("Indonesia",    "ID", 0.70, 0.68),
        ("Saudi Arabia", "SA", 0.27, 0.24),
        ("UAE",          "AE", 0.71, 0.69),
        ("Egypt",        "EG", 0.33, 0.36),
    ]
    rows = []
    for country, iso2, gas, diesel in data:
        rows.append({
            "country":               country,
            "iso2":                  iso2,
            "gasoline_usd_per_liter":gas,
            "diesel_usd_per_liter":  diesel,
            "currency":              "USD",
            "last_updated":          "2023-12-31",
            "source_note":           "World Bank API v2 (mock)",
        })
    return pd.DataFrame(rows)


def save(df: pd.DataFrame, tag: str = "worldbank_api"):
    """Save to data/ folder as CSV."""
    os.makedirs(DATA_DIR, exist_ok=True)
    csv_path = os.path.join(DATA_DIR, f"{tag}.csv")
    df.to_csv(csv_path, index=False)
    print(f"  Saved → {csv_path}")
    return csv_path


if __name__ == "__main__":
    print("=" * 65)
    print("  SOURCE: World Bank API v2 — International Pump Prices")
    print("  No API key required. Free. 150+ countries.")
    print("  Indicators: EP.PMP.SGAS.CD  EP.PMP.DESL.CD")
    print("=" * 65)
    print()

    result = fetch()
    df     = result["df"]

    print(f"\nStrategy : {result['meta']['strategy_used']}")
    print(f"Records  : {result['meta']['records']}")
    print(f"Missing  : {result['meta']['missing_fields']}")
    print()

    if not df.empty:
        print(df.to_string(index=False))
        print()

    save(df, tag="worldbank_api")