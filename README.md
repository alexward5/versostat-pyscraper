# VersoStat PyScraper

Python scripts for scraping sports data sources (FPL, Sportmonks) and loading
them into Postgres, plus scripts to create database views.

## Setup

- Python 3.13.1+
- `pip install -r requirements.txt`

## Configuration

Create `.env.local` with database credentials and API keys.

## Usage

### Running Scripts

```bash
# Run all scripts (tables + views)
python index.py --schema my_schema

# Run by category
python index.py --schema my_schema --scripts tables  # All table creation scripts
python index.py --schema my_schema --scripts views   # All view creation scripts
python index.py --schema my_schema --scripts fpl     # FPL tables only
python index.py --schema my_schema --scripts sm      # Sportmonks tables only

# Run specific scripts
python index.py --schema my_schema --scripts fpl_events fpl_player
python index.py --schema my_schema --scripts mv_player_gameweek

# Use programmatically
from index import run_scripts
run_scripts("my_schema", scripts="fpl")
run_scripts("my_schema", scripts="views")
```

### Creating Tables

Table scripts fetch data from APIs and load them into PostgreSQL tables. All table scripts are in `src/scripts/tables/`.

Example: `fpl_player_gameweek.py` fetches gameweek history for all FPL players and creates the `fpl_player_gameweek` table.

### Creating Materialized Views

View scripts create materialized views by joining existing tables. All view scripts are in `src/scripts/views/`.

Example: `mv_player_gameweek.py` creates a materialized view that joins FPL player gameweek data with Sportmonks player fixture data:

```bash
# Create the view
python index.py --schema my_schema --scripts mv_player_gameweek

# Or run all views
python index.py --schema my_schema --scripts views
```

**Note:** View scripts require the underlying tables to exist first. Run table scripts before view scripts.

## Deployment

**Prerequisites:** Create `versostat/sportmonks-api-key` in Secrets Manager. Deploy `VersoStat-ScraperPlatformStack-prod` from versostat-infra first.

**Local:** Run `./deploy_scraper.sh` to build, push to ECR, and deploy the stack. Env overrides: `AWS_REGION`, `IMAGE_TAG`, `SPORTMONKS_SECRET_ID`.

**Pipeline:** Push to `main` to trigger GitHub Actions (build + push to ECR).

**After first deploy:** Confirm SNS email subscription; optionally test with `aws stepfunctions start-execution --state-machine-arn <arn> --input '{"schema":"my_schema"}'`.

