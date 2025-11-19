# The GetHistoryFunction retrieves 10 years of daily stock data per ticker and produces one CSV file per symbol with:
- OHLCV data
- Derived features (daily return, rolling vol, price gap, volume z-score)
- Technical indicators from Twelve Data (RSI, SMA/EMA, MACD, ATR, Bollinger Bands, ADX, CCI, etc.)

All features are controlled through get_history/features.yml.

## Output
- python app.py → CSVs written to ./output
- sam local invoke → CSVs written to /tmp/output (inside container)

Missing values are currently left as None (no imputation yet).

---

## ⚠️ Things Still To Be Decided
- Final feature list
- How we handle missing values
- S3 storage structure
- Daily update flow (appending vs overwriting)
- Twelve Data rate limits (free tier cannot support multiple tickers due to many indicator calls)

---

## 🧠 How It Works (Short)

For each ticker:
1. Fetch OHLCV (/time_series, one call)
2. Fetch indicators (multiple API calls, one per indicator)
3. Compute derived features in Python
4. Merge everything into one table
5. Write a single CSV per symbol

Because each indicator requires a separate endpoint, it isn’t possible to fetch all features in one API call.

---

## 🚀 Running the Code

### A) Local
```bash
export TWELVE_DATA_API_KEY="your_key"
python3 get_history/app.py
```
Output CSVs appear in:
`./output`

### B) SAM Lambda Simulation
```bash
sam build
sam local invoke --env-vars env.json GetHistoryFunction
```
Uses `/tmp/output` inside the container.
env.json must include your key:
```json
{
  "GetHistoryFunction": {
    "TWELVE_DATA_API_KEY": "YOUR_KEY"
  }
}
```
