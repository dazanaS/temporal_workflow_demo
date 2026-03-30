# PocketHealth Temporal Workflow Analytics

Databricks App (FastAPI + React) that visualizes healthcare appointment scheduling workflows from Temporal, with a Lakebase (Postgres) serving layer for sub-second dashboard performance and transactional invoice management.

## Architecture

```
                    ┌─────────────┐
                    │   Temporal   │  Workflow orchestration engine
                    │   Server     │  Runs healthcare appointment workflows
                    └──────┬──────┘
                           │ JSON exports (daily workflow execution records)
                           ▼
                    ┌─────────────┐
                    │  UC Volume   │  /Volumes/.../temporal/workflow_exports/
                    │  (Raw Files) │  30 days of JSON files (~150 workflows/day)
                    └──────┬──────┘
                           │ Auto Loader (streaming ingestion)
                           ▼
              ┌────────────────────────┐
              │   Lakeflow Pipeline     │  (temporal_workflow_pipeline.sql)
              │                        │
              │  Bronze ─► Silver ─► Gold (7 materialized views)
              │                        │
              └───────────┬────────────┘
                          │
            ┌─────────────┼──────────────┐
            │             │              │
            ▼             ▼              ▼
    ┌──────────────┐ ┌──────────┐ ┌──────────────┐
    │   Lakebase    │ │   SQL     │ │  Genie Space  │
    │  (Postgres)   │ │ Warehouse │ │  (AI/BI)      │
    │               │ │           │ │               │
    │ Gold mirrors  │ │ Silver    │ │ Natural lang  │
    │ Invoice CRUD  │ │ queries   │ │ queries       │
    └──────┬────────┘ └─────┬─────┘ └──────┬────────┘
           │                │              │
           └────────┬───────┘              │
                    ▼                      │
            ┌──────────────┐               │
            │  FastAPI App  │◄──────────────┘
            │  (app.py)     │
            └──────┬────────┘
                   │
                   ▼
            ┌─────────────────┐
            │ React Dashboard  │  7 tabs: Overview, Data Flow, Invoicing,
            │ (Recharts + TS)  │  Facilities, Recent, Failures, Ask AI
            └─────────────────┘
```

## How Each Tool Works Together

### Temporal (Source System)

Temporal is a workflow orchestration engine. In PocketHealth's environment, it runs healthcare appointment workflows — scheduling, rescheduling, cancellations, referral intake, follow-up scheduling, and waitlist promotions. Each completed workflow produces a JSON record containing patient, provider, facility, appointment, tenant, and execution details.

In this demo, Temporal is simulated by `data_generation/generate_temporal_data.py`, which generates realistic workflow records across 12 Canadian healthcare facilities, 15 providers, 4 tenants, and 10 appointment types with a 3% failure rate.

### Unity Catalog Volume (Landing Zone)

Raw JSON files from Temporal land in a UC Volume at `/Volumes/demo_catalog/temporal/workflow_exports/`. This is the ingestion point — Auto Loader watches this directory and picks up new files as they arrive.

The Volume also stores generated invoice PDFs at `/Volumes/demo_catalog/temporal/invoices/`.

### Lakeflow Declarative Pipeline (Medallion ETL)

Defined in `pipeline/temporal_workflow_pipeline.sql`, this is the core data processing:

- **Bronze** (`workflows_bronze`): Raw ingestion via `STREAM read_files()` with Auto Loader. Adds file metadata (source file name, ingestion timestamp). No transformations.
- **Silver** (`workflows_silver`): Flattens nested JSON into 30+ columns. Applies 3 quality constraints — valid workflow_id (not null), valid status (must be Completed/Failed/TimedOut), valid workflow_type (not null). Rows failing constraints are dropped.
- **Gold** (7 materialized views): Pre-aggregated analytics — `daily_workflow_summary`, `appointment_type_metrics`, `facility_utilization`, `provider_workload`, `failure_analysis`, `billing_summary`, `pipeline_metrics`.

### SQL Warehouse (Analytics Engine)

The Databricks SQL warehouse executes queries against lakehouse tables. In this app, it handles:

- **Silver table queries**: Recent workflow executions (last 50), hourly distribution, regional distribution, top providers, tenant overview, confirmation methods — these need the full granular silver data.
- **Pipeline metrics**: Counts across bronze/silver/gold tables for the Data Flow visualization.
- **Fallback**: If Lakebase is unavailable, all gold-table queries automatically fall back to the warehouse via `run_serving_query()`.

### Lakebase (Postgres Serving Layer + Invoice Management)

**What Lakebase is**: Databricks' fully-managed PostgreSQL database (PG 17). It runs inside the Databricks platform with autoscaling (0.5-32 CU), branching, and Unity Catalog integration.

**Why it's needed**: The lakehouse (Delta tables + SQL warehouse) is optimized for analytical workloads — scanning millions of rows, aggregations, batch processing. But it has cold-start latency (several seconds if the warehouse is idle). A dashboard needs sub-second reads. And invoice management needs ACID transactions (single-row inserts and updates) which Postgres handles natively.

**Two roles in this project**:

1. **Serving layer** (read path): The 6 gold aggregate tables are synced into Lakebase via `lakebase/sync_gold_to_lakebase.py`. When the dashboard loads, it reads from Postgres instead of the warehouse — millisecond response times instead of seconds. The app's `run_serving_query()` function tries Lakebase first and falls back to the warehouse if unavailable.

2. **Invoice management** (write path): Invoices need ACID transactions — create a record, insert line items, update status from draft to sent to paid. The `invoices` and `invoice_line_items` tables live natively in Lakebase. When a user saves an invoice, the app generates a PDF (saved to UC Volume) and creates a structured record in Lakebase for tracking and lifecycle management.

**Data flow**:
- **Gold → Lakebase**: The sync script pulls data from gold tables via the SQL Statement API and upserts into Lakebase. Run whenever the pipeline refreshes.
- **App → Lakebase**: FastAPI reads aggregates and manages invoices directly in Postgres.
- **App → Volume**: PDF invoices saved as files via `WorkspaceClient.files.upload()`.

**Authentication**: The app's service principal connects to Lakebase using native Postgres login (`pg_native_login` enabled on the project) with dedicated credentials configured as environment variables.

**Infrastructure**:
- Project: `temporal-lakebase` (autoscaling tier, PG 17)
- Endpoint: `ep-odd-bread-d25xag2n` (8-16 CU, auto-scales to zero)
- Database: `temporal` with 8 tables (6 serving mirrors + 2 invoice tables)

### Genie Space (AI/BI)

Databricks AI/BI Genie enables natural language questions over the data. The app's "Ask AI" tab proxies requests to the Genie API, which generates SQL against the gold tables and returns results. Users can ask questions like "What is the total revenue by tenant?" or "Which facility has the highest MRI volume?" and get answers with data tables and follow-up suggestions.

### FastAPI Backend (API Layer)

`app.py` is the orchestration layer that routes requests to the right data source:

| Endpoint | Data Source | Purpose |
|----------|-------------|---------|
| `/api/summary` | Lakebase (fallback: warehouse) | KPI metrics |
| `/api/daily-trend` | Lakebase (fallback: warehouse) | Time-series chart |
| `/api/workflows-by-type` | Lakebase (fallback: warehouse) | Workflow type pie chart |
| `/api/appointments-by-type` | Lakebase (fallback: warehouse) | Appointment breakdown |
| `/api/facilities` | Lakebase (fallback: warehouse) | Facility utilization |
| `/api/providers` | Lakebase (fallback: warehouse) | Provider workload |
| `/api/failures` | Lakebase (fallback: warehouse) | Failure analysis |
| `/api/tenants` | Lakebase (fallback: warehouse) | Tenant list for invoicing |
| `/api/invoice` | Lakebase (fallback: warehouse) | Calculate invoice |
| `/api/regional-distribution` | SQL Warehouse (silver) | Regional metrics |
| `/api/hourly-distribution` | SQL Warehouse (silver) | Hourly pattern chart |
| `/api/top-providers` | SQL Warehouse (silver) | Top 5 providers |
| `/api/tenant-overview` | SQL Warehouse (silver) | Tenant status breakdown |
| `/api/confirmation-methods` | SQL Warehouse (silver) | Confirmation method chart |
| `/api/recent-workflows` | SQL Warehouse (silver) | Last 50 executions |
| `/api/pipeline-metrics` | SQL Warehouse (bronze+silver+gold) | Data flow counts |
| `/api/invoice/save` | Volume (PDF) + Lakebase (record) | Save invoice |
| `/api/invoices` | Lakebase | List saved invoices |
| `/api/invoices/{id}` | Lakebase | Update invoice status |
| `/api/genie/ask` | Genie API | Natural language queries |

### React Dashboard (Frontend)

Built with React 19, TypeScript, Vite, and Recharts. Uses lucide-react for icons. Seven tabs:

- **Overview**: KPI cards (total workflows, success rate, avg duration, failures) + 7 charts (daily volume, workflow types, appointments by type, regional distribution, confirmation methods, hourly distribution, top providers, tenant overview)
- **Data Flow**: Visual pipeline diagram with medallion-colored tiers (source → bronze → silver → gold) showing record counts and transformation descriptions
- **Invoicing**: Generate invoices by tenant/date range, save as PDF to Volume, view invoice history from Lakebase with status management (draft/sent/paid/cancelled)
- **Facilities**: Facility utilization table
- **Recent**: Last 50 workflow executions with status badges
- **Failures**: Failure analysis by workflow type and reason
- **Ask AI**: Chat interface to Genie Space for natural language queries

## Prerequisites

- Databricks CLI v0.285.0+ (`databricks --version`)
- Node.js 18+ and npm
- Python 3.10+
- `psql` client (`brew install postgresql@16`)
- Databricks profile `demo-workspace` configured

## Workspace & App Details

| Resource | Value |
|----------|-------|
| Workspace | https://demo-workspace.cloud.databricks.com |
| App Name | pockethealth-temporal-demo |
| App URL | https://pockethealth-temporal-demo-7474647873824811.aws.databricksapps.com |
| SQL Warehouse | a82088b3bfe8752c |
| Catalog/Schema | demo_catalog.temporal |
| Lakebase Project | temporal-lakebase |
| Lakebase Endpoint | ep-odd-bread-d25xag2n.database.us-east-1.cloud.databricks.com |
| Lakebase Database | temporal |
| Genie Space ID | 01f1258ac8e91665ac1f796131a7a19b |

## Deployment Steps

### 1. Lakebase Setup (one-time)

```bash
PROFILE=demo-workspace

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
python3 lakebase/sync_gold_to_lakebase.py --profile demo-workspace
```

Run this whenever the Lakeflow pipeline refreshes to keep Lakebase in sync with the latest gold aggregates.

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
cd app && databricks sync . /Workspace/Users/user@databricks.com/pockethealth-temporal-demo \
  --profile demo-workspace

# Upload built frontend
databricks workspace import-dir frontend/dist \
  /Workspace/Users/user@databricks.com/pockethealth-temporal-demo/frontend/dist \
  --overwrite --profile demo-workspace

# Deploy the app
databricks apps deploy pockethealth-temporal-demo \
  --source-code-path /Workspace/Users/user@databricks.com/pockethealth-temporal-demo \
  --profile demo-workspace
```

### 5. Verify

```bash
# Check app status
databricks apps get pockethealth-temporal-demo --profile demo-workspace

# Test an API endpoint (requires auth)
TOKEN=$(databricks auth token -p demo-workspace -o json | jq -r '.access_token')
curl -s "https://pockethealth-temporal-demo-7474647873824811.aws.databricksapps.com/api/summary" \
  -H "Authorization: Bearer $TOKEN"
```

## Lakebase Tables

### Serving Layer (synced from gold)

| Table | Rows | Purpose |
|-------|------|---------|
| `daily_workflow_summary` | ~254 | Daily workflow counts by type and status |
| `appointment_type_metrics` | ~60 | Appointment type breakdown with success rates |
| `facility_utilization` | ~12 | Facility-level utilization metrics |
| `provider_workload` | ~15 | Provider workload and patient metrics |
| `failure_analysis` | ~38 | Failure breakdown by type and reason |
| `billing_summary` | ~2,309 | Billing data by tenant, facility, type, and date |

### Invoice Management (Lakebase-native)

| Table | Purpose |
|-------|---------|
| `invoices` | Invoice records with lifecycle status (draft/sent/paid/cancelled), PDF volume path |
| `invoice_line_items` | Line items per invoice (appointment type, count, unit price, subtotal) |

## Project Structure

```
temporal_workflow_demo/
  app/
    app.py                  # FastAPI backend (Lakebase + SQL Warehouse + Genie)
    app.yaml                # Databricks App config (env vars, command)
    requirements.txt        # Python deps (fastapi, psycopg2, fpdf2, databricks-sdk)
    frontend/
      src/
        App.tsx             # Main dashboard with KPIs, charts, tabs
        App.css             # Styling (CSS variables, medallion colors)
        theme.ts            # Chart colors and status colors
        types.ts            # TypeScript interfaces for all data types
        components/
          DataFlowTab.tsx   # Pipeline visualization with colored tiers
          InvoiceTab.tsx    # Invoice generation, PDF save, history
          GenieTab.tsx      # AI chat interface
  data_generation/
    generate_temporal_data.py  # Synthetic Temporal workflow data generator
    output/                    # 30 days of generated JSON files
  lakebase/
    sync_gold_to_lakebase.py   # Gold table -> Lakebase sync script
  pipeline/
    temporal_workflow_pipeline.sql  # Lakeflow declarative pipeline (bronze/silver/gold)
```

## The Demo Story

This project demonstrates the complete Databricks data platform:

**"Lakehouse for analytics, Lakebase for serving."**

Data flows from Temporal through the medallion pipeline into the lakehouse for batch analytics, gets synced to Lakebase for low-latency dashboard reads and transactional invoice management, and is made queryable via natural language through Genie. The React dashboard ties it all together into a single-pane-of-glass operational view for PocketHealth's healthcare scheduling operations.
