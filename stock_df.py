import yfinance as yf
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# -----------------------------
# 1. Define the tickers
# -----------------------------
tickers = ['AAPL', 'MSFT', 'AMD', 'NVDA', 'GOOG']

# -----------------------------
# 2. Fetch and clean data
# -----------------------------
def fetch_data(tickers):
    frames = []
    for ticker in tickers:
        df = yf.download(ticker, start="2020-01-01", interval="1d")
        if df.empty:
            print(f"[WARN] No data returned for {ticker}")
            continue

        df.reset_index(inplace=True)
        df["ticker"] = ticker

        # Fallback if 'Adj Close' is missing
        if 'Adj Close' not in df.columns:
            df["Adj Close"] = df["Close"]

        # Standardize column names and order
        df = df[["ticker", "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]
        df.columns = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
        frames.append(df)

    if not frames:
        raise ValueError("No valid stock data fetched.")
    
    return pd.concat(frames, ignore_index=True)

stock_df = fetch_data(tickers)

# -----------------------------
# 3. Standardize for Snowflake
# -----------------------------
# Uppercase column names
stock_df.columns = [col.upper() for col in stock_df.columns]

# Fix date format
stock_df["DATE"] = pd.to_datetime(stock_df["DATE"]).dt.date

# -----------------------------
# 4. Connect to Snowflake
# -----------------------------
conn = snowflake.connector.connect(
    user="KARTHIKDOGUPARTHI0824",
    password="mVeyJzKHiJqEj3i",
    account="KUDIGGI-AA12200",   # example: ab12345.us-east-1
    warehouse="COMPUTE_WH",
    database="FINANCE_ANALYTICS",
    schema="RAW"
)

# -----------------------------
# 5. Create target table
# -----------------------------
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS RAW.STOCK_PRICES (
    TICKER STRING,
    DATE DATE,
    OPEN FLOAT,
    HIGH FLOAT,
    LOW FLOAT,
    CLOSE FLOAT,
    ADJ_CLOSE FLOAT,
    VOLUME BIGINT,
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
""")

# -----------------------------
# 6. Upload to Snowflake
# -----------------------------
success, nchunks, nrows, _ = write_pandas(conn, stock_df, "STOCK_PRICES")
print(f"[SUCCESS] Uploaded {nrows} rows to RAW.STOCK_PRICES in Snowflake.")