#!/usr/bin/env python3
import os
from pathlib import Path
from datetime import date
from typing import List

import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# ---------- Nastavenia z ENV s rozumnými defaultmi ----------
LANGUAGE_ID = int(os.getenv("LANGUAGE_ID", "1000"))  # 1000=SK, 1019=HU, 1003=EN
LOCATION_IDS = [int(x.strip()) for x in os.getenv("LOCATION_IDS", "2392").split(",") if x.strip()]  # 2392=SK, 2012=HU
NETWORK = os.getenv("NETWORK", "GOOGLE_SEARCH")  # GOOGLE_SEARCH alebo GOOGLE_SEARCH_AND_PARTNERS

SHEET_ID = os.getenv("SHEET_ID", "").strip()
SHEET_TAB = os.getenv("SHEET_TAB", "").strip()      # napr. "Keywords" (ak prázdne, vezme prvý list)
SHEET_RANGE = (os.getenv("SHEET_RANGE", "A:A") or "A:A").strip()  # napr. "A2:A"
OUT_TAB = os.getenv("OUT_TAB", "Results")           # do tohto listu zapíšeme výsledky

# Info stĺpce (čisto kozmetika do výsledkov; nemajú vplyv na dotaz)
COUNTRY = os.getenv("COUNTRY", "SK")
LANG_TAG = os.getenv("LANG_TAG", "sk")

# ---------- Helpery ----------
def ensure_output_dir() -> Path:
    outdir = Path("output")
    outdir.mkdir(exist_ok=True)
    return outdir

def load_keywords_from_sheet() -> List[str]:
    """Načíta keywords zo Sheetu cez service account súbor sa.json."""
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID nie je nastavený. Daj ho do GitHub Actions Variables.")

    # scopes: write (aby sme vedeli aj zapisovať výsledky), funguje aj pre read
    from google.oauth2.service_account import Credentials
    import gspread

    creds = Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"]  # nie readonly, chceme vedieť aj zapisovať
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(SHEET_TAB) if SHEET_TAB else sh.sheet1

    values = ws.get(SHEET_RANGE)
    col = [row[0] for row in values if row] if values else []
    if col and col[0].strip().lower() == "keyword":
        col = col[1:]

    kws = (
        pd.Series(col)
          .dropna().astype(str).map(str.strip)
          .replace({"": None}).dropna().unique().tolist()
    )
    if not kws:
        raise RuntimeError("V zadanom SHEET_RANGE sa nenašli žiadne kľúčové slová.")
    return kws

def write_results_to_sheet(rows: List[dict]):
    """Zapíše výsledky do OUT_TAB v rovnakom Sheete."""
    from google.oauth2.service_account import Credentials
    import gspread

    if not SHEET_ID:
        # ak nie je SHEET_ID, Sheet skipni (stále však máme CSV)
        return

    creds = Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)

    try:
        ws_out = sh.worksheet(OUT_TAB)
    except Exception:
        # ak list neexistuje, vytvoríme ho a dáme hlavičku
        ws_out = sh.add_worksheet(title=OUT_TAB, rows=1000, cols=20)
        ws_out.append_row([
            "keyword","country","language","avg_monthly_searches","competition",
            "low_top_of_page_bid_micros","high_top_of_page_bid_micros",
            "location_ids","language_id","date_yyyy","date_mm","date_dd"
        ], value_input_option="RAW")

    # ak je list prázdny, pre istotu dopíš hlavičku
    if not ws_out.get_all_values():
        ws_out.append_row([
            "keyword","country","language","avg_monthly_searches","competition",
            "low_top_of_page_bid_micros","high_top_of_page_bid_micros",
            "location_ids","language_id","date_yyyy","date_mm","date_dd"
        ], value_input_option="RAW")

    today = date.today()
    comp_map = {2: "LOW", 3: "MEDIUM", 4: "HIGH"}

    payload = []
    for r in rows:
        payload.append([
            r["keyword"], COUNTRY, LANG_TAG,
            r.get("avg_monthly_searches") or 0,
            comp_map.get(r.get("competition"), "UNKNOWN"),
            r.get("low_top_of_page_bid_micros") or 0,
            r.get("high_top_of_page_bid_micros") or 0,
            ",".join(str(x) for x in LOCATION_IDS),
            str(LANGUAGE_ID),
            today.year, today.month, today.day
        ])

    if payload:
        # append_rows = rýchlejšie než append_row v slučke
        ws_out.append_rows(payload, value_input_option="RAW")

def build_ads_client() -> GoogleAdsClient:
    """Vytvorí dočasný google-ads.yaml z ENV a vráti inicializovaný client."""
    req = ["ADS_DEVELOPER_TOKEN","ADS_CLIENT_ID","ADS_CLIENT_SECRET","ADS_REFRESH_TOKEN","ADS_CLIENT_CUSTOMER_ID"]
    missing = [k for k in req if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Chýbajú Ads secrets: {', '.join(missing)}")

    ads_yaml = f"""developer_token: '{os.environ["ADS_DEVELOPER_TOKEN"]}'
client_id: '{os.environ["ADS_CLIENT_ID"]}'
client_secret: '{os.environ["ADS_CLIENT_SECRET"]}'
refresh_token: '{os.environ["ADS_REFRESH_TOKEN"]}'
client_customer_id: '{os.environ["ADS_CLIENT_CUSTOMER_ID"]}'
"""
    Path("google-ads.yaml").write_text(ads_yaml, encoding="utf-8")
    return GoogleAdsClient.load_from_storage(path="google-ads.yaml")

def fetch_keyword_metrics(client: GoogleAdsClient, keywords: List[str]) -> List[dict]:
    """Získa metriky cez KeywordPlanIdeaService pre zoznam keywordov."""
    idea_service = client.get_service("KeywordPlanIdeaService")
    customer_id = client.configuration.client_customer_id

    def generate(batch: List[str]):
        locs = [client.get_type("LocationInfo") for _ in LOCATION_IDS]
        for li, lid in zip(locs, LOCATION_IDS):
            li.geo_target_constant = f"geoTargetConstants/{lid}"

        lang = client.get_type("LanguageInfo")
        lang.language_constant = f"languageConstants/{LANGUAGE_ID}"

        request = client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = customer_id
        request.language.CopyFrom(lang)
        request.geo_target_constants.extend([l.geo_target_constant for l in locs])
        request.include_adult_keywords = False
        request.keyword_plan_network = 2 if NETWORK == "GOOGLE_SEARCH" else 3
        request.keyword_seed.keywords.extend(batch)
        return idea_service.generate_keyword_ideas(request=request)

    out_rows = []
    # Google odporúča batche do ~100
    for i in range(0, len(keywords), 100):
        resp = generate(keywords[i:i+100])
        for r in resp.results:
            km = r.keyword_idea_metrics
            out_rows.append({
                "keyword": r.text,
                "avg_monthly_searches": getattr(km, "avg_monthly_searches", None),
                "competition": getattr(km, "competition", None),  # 2=LOW, 3=MEDIUM, 4=HIGH
                "low_top_of_page_bid_micros": getattr(km, "low_top_of_page_bid_micros", None),
                "high_top_of_page_bid_micros": getattr(km, "high_top_of_page_bid_micros", None),
            })
    return out_rows

# ---------- Main ----------
def main():
    # 1) Načítaj keywords zo Sheetu (alebo fallback keywords.csv, ak by si chcel)
    keywords = load_keywords_from_sheet()

    # 2) Google Ads client
    client = build_ads_client()

    # 3) Fetch metriky
    rows = fetch_keyword_metrics(client, keywords)

    # 4) Ulož CSV
    outdir = ensure_output_dir()
    outfile = outdir / "ads_keyword_metrics.csv"
    (pd.DataFrame(rows)
        .sort_values("avg_monthly_searches", ascending=False, na_position="last")
        .to_csv(outfile, index=False, encoding="utf-8-sig"))
    print(f"Wrote {outfile} ({len(rows)} rows)")

    # 5) (Voliteľne) zapíš do Sheetu do OUT_TAB
    write_results_to_sheet(rows)

if __name__ == "__main__":
    try:
        main()
    except GoogleAdsException as ex:
        # Prehľadná hláška z Ads API
        print("GoogleAdsException:", ex.failure)
        raise
    except Exception as e:
        print("ERROR:", e)
        raise
