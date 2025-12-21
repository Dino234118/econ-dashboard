import os, json, requests
import firebase_admin
from firebase_admin import credentials, firestore


# ---------- Firestore init ----------

def init_firestore():
    # Service account JSON is passed via env variable in GitHub Actions
    sa_json = os.environ["FIREBASE_SERVICE_ACCOUNT"]
    cred = credentials.Certificate(json.loads(sa_json))
    # Avoid "already initialized" error if run multiple times
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    return firestore.client()


# ---------- 1) Inflation (World Bank) ----------

def update_inflation(db):
    """
    Fetch latest annual CPI inflation for India from World Bank
    and upsert into 'inflation' collection.
    """
    url = (
        "https://api.worldbank.org/v2/country/IN/"
        "indicator/FP.CPI.TOTL.ZG?format=json"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()[1]

    # pick latest non-null value
    latest = next(item for item in data if item["value"] is not None)
    year = latest["date"]            # e.g. '2023'
    value = float(latest["value"])
    date_str = f"{year}-12-31"       # treat as end-of-year value

    doc = {
        "date": date_str,
        "value": value,
        "unit": "percent",
        "indicator_name": "inflation_cpi",
        "source": "world_bank",
    }
    db.collection("inflation").document(date_str).set(doc)
    print(f"[inflation] updated {date_str} = {value}")


# ---------- 2) FX INR–USD (Alpha Vantage FX_DAILY) ----------

def update_fx_inr_usd(db):
    """
    Fetch latest daily USD→INR FX rate and store in 'fx_inr_usd'.
    Uses Alpha Vantage FX_DAILY API.
    """
    api_key = os.environ["ALPHAVANTAGE_API_KEY"]  # set in GitHub secrets
    url = (
        "https://www.alphavantage.co/query"
        "?function=FX_DAILY&from_symbol=USD&to_symbol=INR"
        f"&apikey={api_key}&outputsize=compact"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    ts = data.get("Time Series FX (Daily)")
    if not ts:
        raise RuntimeError(f"Unexpected FX response: {data}")

    # dates are keys like '2024-01-31'; pick the latest
    latest_date = sorted(ts.keys(), reverse=True)[0]
    latest_row = ts[latest_date]
    value = float(latest_row["4. close"])  # closing rate

    doc = {
        "date": latest_date,
        "value": value,
        "unit": "INR_per_USD",
        "indicator_name": "fx_inr_usd",
        "source": "alpha_vantage",
    }
    db.collection("fx_inr_usd").document(latest_date).set(doc)
    print(f"[fx_inr_usd] updated {latest_date} = {value}")


# ---------- 3) Sensex index (Alpha Vantage TIME_SERIES_DAILY) ----------

def update_sensex(db):
    """
    Fetch latest daily Sensex index value and store in 'sensex'.
    Uses Alpha Vantage TIME_SERIES_DAILY API.
    NOTE: If ^BSESN doesn't work with your key, use an ETF symbol
          that tracks Sensex or Nifty instead.
    """
    api_key = os.environ["ALPHAVANTAGE_API_KEY"]
    symbol = "^BSESN"   # change if your data source uses a different symbol
    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY&symbol={symbol}"
        f"&apikey={api_key}&outputsize=compact"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    ts = data.get("Time Series (Daily)")
    if not ts:
        raise RuntimeError(f"Unexpected Sensex response: {data}")

    latest_date = sorted(ts.keys(), reverse=True)[0]
    latest_row = ts[latest_date]
    close_price = float(latest_row["4. close"])

    doc = {
        "date": latest_date,
        "value": close_price,
        "unit": "index_points",
        "indicator_name": "sensex_index",
        "source": "alpha_vantage",
    }
    db.collection("sensex").document(latest_date).set(doc)
    print(f"[sensex] updated {latest_date} = {close_price}")


# ---------- main orchestration ----------

def main():
    db = init_firestore()

    # Run each updater; don't let one failure stop the others
    for fn in (update_inflation, update_fx_inr_usd, update_sensex):
        try:
            fn(db)
        except Exception as e:
            print(f"Error in {fn.__name__}: {e}")


if __name__ == "__main__":
    main()
import os, json, requests
import firebase_admin
from firebase_admin import credentials, firestore


# ---------- Firestore init ----------

def init_firestore():
    # Service account JSON is passed via env variable in GitHub Actions
    sa_json = os.environ["FIREBASE_SERVICE_ACCOUNT"]
    cred = credentials.Certificate(json.loads(sa_json))
    # Avoid "already initialized" error if run multiple times
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    return firestore.client()


# ---------- 1) Inflation (World Bank) ----------

def update_inflation(db):
    """
    Fetch latest annual CPI inflation for India from World Bank
    and upsert into 'inflation' collection.
    """
    url = (
        "https://api.worldbank.org/v2/country/IN/"
        "indicator/FP.CPI.TOTL.ZG?format=json"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()[1]

    # pick latest non-null value
    latest = next(item for item in data if item["value"] is not None)
    year = latest["date"]            # e.g. '2023'
    value = float(latest["value"])
    date_str = f"{year}-12-31"       # treat as end-of-year value

    doc = {
        "date": date_str,
        "value": value,
        "unit": "percent",
        "indicator_name": "inflation_cpi",
        "source": "world_bank",
    }
    db.collection("inflation").document(date_str).set(doc)
    print(f"[inflation] updated {date_str} = {value}")


# ---------- 2) FX INR–USD (Alpha Vantage FX_DAILY) ----------

def update_fx_inr_usd(db):
    """
    Fetch latest daily USD→INR FX rate and store in 'fx_inr_usd'.
    Uses Alpha Vantage FX_DAILY API.
    """
    api_key = os.environ["ALPHAVANTAGE_API_KEY"]  # set in GitHub secrets
    url = (
        "https://www.alphavantage.co/query"
        "?function=FX_DAILY&from_symbol=USD&to_symbol=INR"
        f"&apikey={api_key}&outputsize=compact"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    ts = data.get("Time Series FX (Daily)")
    if not ts:
        raise RuntimeError(f"Unexpected FX response: {data}")

    # dates are keys like '2024-01-31'; pick the latest
    latest_date = sorted(ts.keys(), reverse=True)[0]
    latest_row = ts[latest_date]
    value = float(latest_row["4. close"])  # closing rate

    doc = {
        "date": latest_date,
        "value": value,
        "unit": "INR_per_USD",
        "indicator_name": "fx_inr_usd",
        "source": "alpha_vantage",
    }
    db.collection("fx_inr_usd").document(latest_date).set(doc)
    print(f"[fx_inr_usd] updated {latest_date} = {value}")


# ---------- 3) Sensex index (Alpha Vantage TIME_SERIES_DAILY) ----------

def update_sensex(db):
    """
    Fetch latest daily Sensex index value and store in 'sensex'.
    Uses Alpha Vantage TIME_SERIES_DAILY API.
    NOTE: If ^BSESN doesn't work with your key, use an ETF symbol
          that tracks Sensex or Nifty instead.
    """
    api_key = os.environ["ALPHAVANTAGE_API_KEY"]
    symbol = "^BSESN"   # change if your data source uses a different symbol
    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY&symbol={symbol}"
        f"&apikey={api_key}&outputsize=compact"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    ts = data.get("Time Series (Daily)")
    if not ts:
        raise RuntimeError(f"Unexpected Sensex response: {data}")

    latest_date = sorted(ts.keys(), reverse=True)[0]
    latest_row = ts[latest_date]
    close_price = float(latest_row["4. close"])

    doc = {
        "date": latest_date,
        "value": close_price,
        "unit": "index_points",
        "indicator_name": "sensex_index",
        "source": "alpha_vantage",
    }
    db.collection("sensex").document(latest_date).set(doc)
    print(f"[sensex] updated {latest_date} = {close_price}")


# ---------- main orchestration ----------

def main():
    db = init_firestore()

    # Run each updater; don't let one failure stop the others
    for fn in (update_inflation, update_fx_inr_usd, update_sensex):
        try:
            fn(db)
        except Exception as e:
            print(f"Error in {fn.__name__}: {e}")


if __name__ == "__main__":
    main()