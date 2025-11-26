# Data Retrieval Pipeline (EODHD / AWS Lambda)

This project fetches 10 years of daily OHLCV equity data from EODHD for a list of U.S. tickers and stores the results as monthly Parquet files.
It includes a one-time historical backfill Lambda function and supports local development with AWS SAM.

⸻

## 🚀 Features
- Fetches 10 years of historical daily OHLCV data
- Reads tickers from get_history/tickers.txt
- Parallel API requests using ThreadPoolExecutor
- Writes Parquet files as YYYY-MM.parquet
- Runs locally or inside AWS Lambda (GetHistoryFunction)
- Local Lambda simulation with AWS SAM

⸻

## 🧱 Project Structure
```
data-retrieval/
│
├── get_history/
│   ├── app.py              # Main pipeline (10-year backfill)
│   ├── tickers.txt         # List of tickers
│
├── output/                 # Local Parquet output (created at runtime)
│
├── template.yaml           # SAM template (Lambda definition)
├── env.json                # Local SAM env vars
├── requirements.txt        # Python dependencies
└── README.md
```

## 🔑 Environment Variables
| Variable | Purpose |
| --- | --- |
| EODHD_API_TOKEN | EODHD API key |
| MAX_WORKERS | Parallel request count (default 8) |

.env (for local Python runs)
```
EODHD_API_TOKEN=YOUR_KEY
MAX_WORKERS=8
```

## Local Testing (Simple Python Run)
From the repo root:
```
cd get_history
python3 app.py
```
- Loads .env
- Fetches 10-year OHLCV for all tickers
- Writes Parquet files into ../output/

⸻

## 🧪 Local Testing with AWS SAM (Lambda Simulation)

1. env.json
```
{
  "GetHistoryFunction": {
    "EODHD_API_TOKEN": "YOUR_KEY",
    "MAX_WORKERS": "8"
  }
}
```
2. event.json
```
{}
```
3. Build the project
```
sam build --use-container
```
4. Invoke the Lambda locally
```
sam local invoke GetHistoryFunction \
  --event event.json \
  --env-vars env.json
```
The Lambda writes its Parquet output to /tmp/output inside the container.

⸻

## 📦 Deploy to AWS

Deploy using:
```
sam deploy --guided
```
This uploads and provisions the GetHistoryFunction Lambda in your AWS account.
Later you can extend the project to write Parquet files directly to S3.

⸻

## 📝 Notes
- Heavy dependencies (pandas, pyarrow, numpy) require using
sam build --use-container to match the Lambda environment.
- Output is organized by YYYY-MM for efficient downstream processing.
