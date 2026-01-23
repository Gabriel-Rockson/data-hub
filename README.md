# data-hub

MT5 CSV ingestion to TimescaleDB.

## Setup

```bash
pip install -e .
cp .env.example .env
# Edit .env with DB credentials

# Start TimescaleDB
docker compose up -d timescaledb

# Create database
docker compose exec timescaledb psql -U postgres -c "CREATE DATABASE market_data;"
```

## Getting Data (MT5)

To obtain the required CSV data from MetaTrader 5:
1. Log in to your MT5 account.
2. Press `Ctrl + U` to open the **Symbols** window.
3. Click on the **Ticks** (or **Bars**) tab.
4. Select the desired symbol and timeframe/date range.
5. Click **Export** to save the data as a CSV file.
6. Place the exported file into a broker-specific subdirectory under `data-hub/data/` (e.g., `data-hub/data/IC_Markets/`).

## Directory Structure

Organize CSV files by broker - directory name becomes broker name in database:

```
data/
├── IC_Markets/           → stored as "ic-markets"
│   ├── XAUUSD_H1_*.csv
│   └── EURUSD_M15_*.csv
├── Oanda/                → stored as "oanda"
│   └── XAUUSD_H1_*.csv
├── Interactive_Brokers/  → stored as "interactive-brokers"
│   └── GBPUSD_M5_*.csv
└── My_Custom_Broker/     → stored as "my-custom-broker"
    └── EURUSD_M1_*.csv
```

**Any directory name works** - it's automatically converted to lowercase with hyphens. Use `--broker` to override.

## Usage

```bash
# Single file (auto-detects broker from path)
python -m data_hub.cli --csv-file data/IC_Markets/XAUUSD_H1.csv

# Directory (processes all CSV files)
python -m data_hub.cli --csv-dir data/IC_Markets/

# Override broker name
python -m data_hub.cli --csv-file data/XAUUSD_H1.csv --broker ic-markets

# Dry-run validation (no DB insert)
python -m data_hub.cli --csv-dir data/ --dry-run --validate

# Custom batch size
python -m data_hub.cli --csv-dir data/ --batch-size 5000
```

## File Format

MetaTrader CSV exports (tab-delimited):

```
<DATE>	<TIME>	<OPEN>	<HIGH>	<LOW>	<CLOSE>	<TICKVOL>	<VOL>	<SPREAD>
2010.01.01	00:00:00	1.43259	1.45785	1.38610	1.38611	948949	0	8
```

Symbol and timeframe extracted from filename: `SYMBOL_TIMEFRAME_START_END.csv`

## Normalization

- **Brokers**: Directory name → lowercase-with-hyphens (e.g., `My_Broker` → `my-broker`)
- **Symbols**: XAUUSDm, GOLD, GC → XAUUSD
- **Timeframes**: M1, 1m, 1min → M1; H1, 1h, 60m → H1

## Verification

```bash
# Check inserted data
docker compose exec timescaledb psql -U postgres -d market_data -c \
  "SELECT broker, symbol, timeframe, COUNT(*) FROM market_data GROUP BY 1,2,3;"

# View latest bars
docker compose exec timescaledb psql -U postgres -d market_data -c \
  "SELECT * FROM market_data ORDER BY time DESC LIMIT 10;"
```

## Performance

- Uses PostgreSQL COPY for bulk inserts (~15k records/sec)
- Automatic duplicate handling (ON CONFLICT DO NOTHING)
- TimescaleDB hypertable with weekly chunks
- Space partitioning by symbol
