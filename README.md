# VersoStat PyScraper

Python scripts for scraping sports data sources (FPL, Sportmonks) and loading
them into Postgres, plus scripts to create database views.

## Setup

- Python 3.13.1+
- `pip install -r requirements.txt`

## Configuration

Create `.env.local` with database credentials and API keys.

## Usage

```bash
# Run all scripts
python index.py --schema my_schema

# Run by category
python index.py --schema my_schema --scripts fpl
python index.py --schema my_schema --scripts sm

# Run specific scripts
python index.py --schema my_schema --scripts fpl_events fpl_player

# Use programmatically
from index import run_scripts
run_scripts("my_schema", scripts="fpl")
```

