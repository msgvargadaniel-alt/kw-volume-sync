#!/usr/bin/env python3
import os
from pathlib import Path
import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

def load_keywords(csv_path: str):
    sheet_id = os.getenv("SHEET_ID", "").strip()
    if sheet_id:
        # čítanie z Google Sheetu cez service account (sa.json)
        import gspread
        from google.oauth2.service_account import Credentials

        creds = Credentials.from_service_account_file(
            "sa.json",
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)

        sheet_tab = os.getenv("SHEET_TAB", "").strip()
        ws = sh.worksheet(sheet_tab) if sheet_tab else sh.sheet1

        sheet_range = (os.getenv("SHEET_RANGE", "A:A") or "A:A").strip()
        values = ws.get(sheet_range)

        col = [row[0] for row in values if row] if values else []
        if col and col[0].strip().lower() == "keyword":
            col = col[1:]

        return (pd.Series(col)
                  .dropna().astype(str).map(str.strip)
                  .replace({"": None}).dropna().unique().tolist())

    # fallback: CSV
    df = pd.read_csv(csv_path)
    colname = None
    for c in df.columns:
        if c.strip().lower() in ("keyword","keywords","kw","term"):
            colname = c; break
    if colname is None: colname = df.columns[0]
    return (df[colname]
            .dropna().astype(str).map(str.strip)
            .replace({"": None}).dropna().unique().tolist())

def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def main():
    language_id = int(os.getenv("LANGUAGE_ID", "1000"))
    location_ids = [int(x.strip()) for x in (os.getenv("LOCATION_IDS","2392")).split(",") if x.strip()]
    network = os.getenv("NETWORK","GOOGLE_SEARCH")
    keywords = load_keywords("keywords.csv")

    # postavíme google-ads.yaml z ENV (Secrets)
    ads_yaml = f"""developer_token: '{os.environ["ADS_DEVELOPER_TOKEN"]}'
client_id: '{os.environ["ADS_CLIENT_ID"]}'
client_secret: '{os.environ["ADS_CLIENT_SECRET"]}'
refresh_token: '{os.environ["ADS_REFRESH_TOKEN"]}'
client_customer_id: '{os.environ["ADS_CLIENT_CUSTOMER_ID"]}'
"""
    Path("google-ads.yaml").write_text(ads_yaml, encoding="utf-8")
    client = GoogleAdsClient.load_from_storage(path="google-ads.yaml")
    customer_id = client.configuration.client_customer_id

    idea_service = client.get_service("KeywordPlanIdeaService")

    def generate(batch):
        locs = [client.get_type("LocationInfo") for _ in location_ids]
        for li, lid in zip(locs, location_ids):
            li.geo_target_constant = f"geoTargetConstants/{lid}"
        lang = client.get_type("LanguageInfo")
        lang.language_constant = f"languageConstants/{language_id}"

        request = client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = customer_id
        request.language.CopyFrom(lang)
        request.geo_target_constants.extend([l.geo_target_constant for l in locs])
        request.include_adult_keywords = False
        request.keyword_plan_network = 2 if network=="GOOGLE_SEARCH" else 3
        request.keyword_seed.keywords.extend(batch)
        return idea_service.generate_keyword_ideas(request=request)

    rows = []
    for batch in chunked(keywords, 100):
        resp = generate(batch)
        for r in resp.results:
            km = r.keyword_idea_metrics
            rows.append({
                "keyword": r.text,
                "avg_monthly_searches": getattr(km, "avg_monthly_searches", None),
                "competition": getattr(km, "competition", None),
                "low_top_of_page_bid_micros": getattr(km, "low_top_of_page_bid_micros", None),
                "high_top_of_page_bid_micros": getattr(km, "high_top_of_page_bid_micros", None),
            })

    outdir = Path("output"); outdir.mkdir(exist_ok=True)
    outfile = outdir / "ads_keyword_metrics.csv"
    pd.DataFrame(rows).sort_values("avg_monthly_searches", ascending=False, na_position="last").to_csv(outfile, index=False, encoding="utf-8-sig")
    print(f"Wrote {outfile} ({len(rows)} rows)")

if __name__ == "__main__":
    try:
        main()
    except GoogleAdsException as ex:
        print("GoogleAdsException:", ex.failure)
        raise
