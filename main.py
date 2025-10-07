import os, json
import gspread
from google.oauth2.service_account import Credentials

SHEET_ID = os.environ["SHEET_ID"]
sa_json = os.environ["GSPREAD_SERVICE_JSON"]
creds_info = json.loads(sa_json)

scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)

# priprav listy
try:
    ws_in = sh.worksheet("Input")
except gspread.exceptions.WorksheetNotFound:
    ws_in = sh.add_worksheet(title="Input", rows=10, cols=5)
    ws_in.append_row(["keyword","country_code","language_code","network","status"])

try:
    ws_out = sh.worksheet("Metrics")
except gspread.exceptions.WorksheetNotFound:
    ws_out = sh.add_worksheet(title="Metrics", rows=10, cols=11)
    ws_out.append_row(["keyword","country","language","avg_monthly_searches","competition","competition_index","low_top_of_page_bid_micros","high_top_of_page_bid_micros","year","month","monthly_searches"])

# testovací zápis
print("OK: zapis do 'Metrics' prebehol.")
