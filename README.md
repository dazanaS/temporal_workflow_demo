# PocketHealth Temporal Workflow Analytics

Databricks App (FastAPI + React) that visualizes healthcare appointment scheduling workflows from Temporal, with a Lakebase (Postgres) serving layer for sub-second dashboard performance.

## Architecture

```
Temporal (JSON exports)
  -> UC Volume (raw files)
    -> Bronze (Auto Loader streaming table)
      -> Silver (parsed, validated, quality constraints)
        -> Gold (6 materialized views)
          -> Lakebase (Postgres serving layer) <- FastAPI app reads here
                                                <- Invoice CRUD lives here
```

- **SQL Warehouse**: Analytics queries on silver table (recent workflows, hourly distribution, regional metrics)
- **Lakebase**: Serves pre-aggregated gold data for fast dashboard reads + invoice management

## Prerequisites

- Databricks CLI v0.285.0+ (`databricks --version`)
- Node.js 18+ and npm
- Python 3.10+
- `psql` client (`brew install postgresql@16`)
- Databricks profile `Dazana-classic-ws` configured

## Workspace & App Details

| Resource | Value |
|----------|-------|
| Workspace | https://fevm-dazana-classic-ws.cloud.databricks.com |
| App Name | pockethealth-temporal-demo |
| App URL | https://pockethealth-temporal-demo-7474647873824811.aws.databricksapps.com |
| SQL Warehouse | a82088b3bfe8752c |
| Catalog/Schema | dazana_classic_ws_catalog.temporal |
| Lakebase Project | temporal-lakebase |
| Lakebase Endpoint | ep-odd-bread-d25xag2n.database.us-east-1.cloud.databricks.com |
| Lakebase Database | temporal |

## Deployment Steps

### 1. Lakebase Setup (one-time)

```bash
PROFILE=Dazana-classic-ws

# Verify project exists
databricks postgres list-projects -p $PROFILE

# Check endpoint is ACTIVE
databricks postgres list-endpoints projects/temporal-lakebase/branches/production -p $PROFILE

# Get connection details
HOST=$(databricks postgres list-endpoints projects/temporal-lakebase/branches/production \
  -p $PROFILE -o json | jq -r '.[0].status.hosts.host')
TOKEN=$(databricks postgres generate-database-credential \
  projects/temporal-lakebase/branches/production/endpoints/primary \
  -p $PROFILE -o json | jq -r '.token')
EMAIL=$(databricks current-user me -p $PROFILE -o json | jq -r '.userName')

# Connect and verify tables
PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=temporal user=$EMAIL sslmode=require" \
  -c "\dt"
```

### 2. Sync Gold Data to Lakebase

```bash
python3 lakebase/sync_gold_to_lakebase.py --profile Dazana-classic-ws
```

Run this whenever the pipeline refreshes to keep Lakebase in sync.

### 3. Build Frontend

```bash
cd app/frontend
npm install
npm run build
cd ../..
```

### 4. Deploy to Databricks

```bash
# Sync source code
cd app && databricks sync . /Workspace/Users/dazana.hasan@databricks.com/pockethealth-temporal-demo \
  --profile Dazana-classic-ws

# Upload built frontend
databricks workspace import-dir frontend/dist \
  /Workspace/Users/dazana.hasan@databricks.com/pockethealth-temporal-demo/frontend/dist \
  --overwrite --profile Dazana-classic-ws

# Deploy the app
databricks apps deploy pockethealth-temporal-demo \
  --source-code-path /Workspace/Users/dazana.hasan@databricks.com/pockethealth-temporal-demo \
  --profile Dazana-classic-ws
```

### 5. Verify

```bash
# Check app status
databricks apps get pockethealth-temporal-demo --profile Dazana-classic-ws
```

## Lakebase Tables

### Serving Layer (synced from gold)
- `daily_workflow_summary` — daily workflow counts by type and status
- `appointment_type_metrics` — appointment type breakdown with success rates
- `facility_utilization` — facility-level utilization metrics
- `provider_workload` — provider workload and patient metrics
- `failure_analysis` — failure breakdown by type and reason
- `billing_summary` — billing data by tenant, facility, type, and date

### Invoice Management (Lakebase-native)
- `invoices` — invoice records with lifecycle status (draft/sent/paid/cancelled)
- `invoice_line_items` — line items for each invoice

## Data Pipeline

The Lakeflow declarative pipeline is defined in `pipeline/temporal_workflow_pipeline.sql`:
- **Bronze**: Auto Loader ingestion from JSON files in UC Volume
- **Silver**: Parsed, flattened, validated (3 quality constraints)
- **Gold**: 7 materialized views for analytics

## Project Structure

```
temporal_workflow_demo/
  app/
    app.py              # FastAPI backend (Lakebase + SQL Warehouse)
    app.yaml            # Databricks App config
    requirements.txt    # Python dependencies
    frontend/           # React + TypeScript + Vite dashboard
  data_generation/
    generate_temporal_data.py  # Synthetic data generator
    output/                    # Generated JSON files
  lakebase/
    sync_gold_to_lakebase.py   # Gold -> Lakebase sync script
  pipeline/
    temporal_workflow_pipeline.sql  # Lakeflow declarative pipeline
```
