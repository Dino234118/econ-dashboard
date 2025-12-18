import os, json, requests
from datetime import datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore


# ---------- Firestore init ----------

def init_firestore():
    sa_json = os.environ["FIREBASE_SERVICE_ACCOUNT"]
    cred = credentials.Certificate(json.loads(sa_json))
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    return firestore.client()


# ---------- 1) Inflation (World Bank – yearly) ----------

def update_inflation(db):
    url = "https://api.worldbank.org/v2/country/IN/indicator/FP.CPI.TOTL.ZG?format=json"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()[1]

    latest = next(item for item in data if item["value"] is not None)
    year = latest["date"]
    value = float(latest["value"])
    date_str = f"{year}-12-31"

    doc = {
        "date": date_str,
        "value": value,
        "unit": "percent",
        "indicator_name": "inflation_cpi",
        "source": "world_bank",
        "last_updated": datetime.now(timezone.utc).isoformat()
    }

    db.collection("inflation").document(date_str).set(doc)
    print(f"[inflation] {date_str} = {value}")


# ---------- 2) FX USD–INR (exchangerate.host – daily) ----------

def update_fx_inr_usd(db):
    url = "https://api.exchangerate.host/latest?base=USD&symbols=INR"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    date = data["date"]
    value = float(data["rates"]["INR"])

    doc = {
        "date": date,
        "value": value,
        "unit": "INR_per_USD",
        "indicator_name": "fx_inr_usd",
        "source": "exchangerate.host",
        "last_updated": datetime.now(timezone.utc).isoformat()
    }

    db.collection("fx_inr_usd").document(date).set(doc)
    print(f"[fx] {date} = {value}")


# ---------- 3) Sensex (Yahoo Finance – trading days) ----------

def update_sensex(db):
    url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EBSESN?range=5d&interval=1d"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()["chart"]["result"][0]

    timestamps = data["timestamp"]
    closes = data["indicators"]["quote"][0]["close"]

    latest_ts = timestamps[-1]
    latest_close = closes[-1]

    date = datetime.utcfromtimestamp(latest_ts).strftime("%Y-%m-%d")

    doc = {
        "date": date,
        "value": float(latest_close),
        "unit": "index_points",
        "indicator_name": "sensex_index",
        "source": "yahoo_finance",
        "last_updated": datetime.now(timezone.utc).isoformat()
    }

    db.collection("sensex").document(date).set(doc)
    print(f"[sensex] {date} = {latest_close}")


# ---------- main ----------

def main():
    db = init_firestore()
    update_inflation(db)
    update_fx_inr_usd(db)
    update_sensex(db)


if __name__ == "__main__":
    main()
