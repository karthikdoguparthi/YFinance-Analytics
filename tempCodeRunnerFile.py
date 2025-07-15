import yfinance as yf
import pandas as pd

tickers = ['AAPL', 'MSFT', 'AMD', 'NVDA', 'GOOG']
frames = []

for ticker in tickers:
    df = yf.download(ticker, start="2020-01-01", interval="1d")
    if df.empty:
        print(f"[WARN] No data returned for {ticker}")
        continue

    df.reset_index(inplace=True)
    df["ticker"] = ticker

    # Add adj_close only if available
    if 'Adj Close' not in df.columns:
        df["Adj Close"] = df["Close"]  # fallback

    df = df[["ticker", "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]
    df.columns = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
    frames.append(df)

stock_df = pd.concat(frames, ignore_index=True)