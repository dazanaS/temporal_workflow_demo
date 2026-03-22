"""
PocketHealth Temporal Workflow Analytics - Databricks App
FastAPI backend serving React dashboard with SQL Warehouse queries.
"""

import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from databricks.sdk import WorkspaceClient
from databricks import sql as dbsql

# --- Configuration ---
CATALOG = "dazana_classic_ws_catalog"
SCHEMA = "temporal"
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

# --- Appointment Pricing (server-side rate card) ---
APPOINTMENT_PRICING = {
    "MRI": 150.00,
    "CT Scan": 125.00,
    "Ultrasound": 85.00,
    "X-Ray": 45.00,
    "Blood Work": 35.00,
    "Primary Care Visit": 75.00,
    "Specialist Consultation": 120.00,
    "Physical Therapy": 90.00,
    "Dermatology": 110.00,
    "Cardiology": 135.00,
}


def get_connection():
    """Get Databricks SQL connection using appropriate auth."""
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "a82088b3bfe8752c")
    http_path = f"/sql/1.0/warehouses/{warehouse_id}"

    if IS_DATABRICKS_APP:
        w = WorkspaceClient()
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        token = w.config.authenticate()
        if isinstance(token, dict):
            token = token.get("Authorization", "").replace("Bearer ", "")
        return dbsql.connect(
            server_hostname=host.replace("https://", ""),
            http_path=http_path,
            access_token=token,
        )
    else:
        profile = os.environ.get("DATABRICKS_PROFILE", "Dazana-classic-ws-pat")
        w = WorkspaceClient(profile=profile)
        host = w.config.host.replace("https://", "")
        auth = w.config.authenticate()
        if isinstance(auth, dict):
            token = auth.get("Authorization", "").replace("Bearer ", "")
        else:
            token = auth
        return dbsql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token,
        )


def run_query(query: str) -> list[dict]:
    """Execute SQL and return list of dicts."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


# --- FastAPI App ---
app = FastAPI(title="PocketHealth Temporal Workflow Analytics")


@app.get("/api/summary")
def get_summary():
    """Overall KPI metrics."""
    query = f"""
    SELECT
        SUM(workflow_count) AS total_workflows,
        SUM(CASE WHEN status = 'Completed' THEN workflow_count ELSE 0 END) AS successful_workflows,
        SUM(CASE WHEN status = 'Failed' THEN workflow_count ELSE 0 END) AS failed_workflows,
        SUM(CASE WHEN status = 'TimedOut' THEN workflow_count ELSE 0 END) AS timed_out_workflows,
        ROUND(SUM(CASE WHEN status = 'Completed' THEN workflow_count ELSE 0 END) * 100.0 / SUM(workflow_count), 1) AS success_rate,
        ROUND(AVG(avg_duration_seconds), 1) AS avg_duration_seconds
    FROM {CATALOG}.{SCHEMA}.daily_workflow_summary
    """
    results = run_query(query)
    if results:
        row = results[0]
        # Convert Decimal types to float for JSON serialization
        return {k: float(v) if v is not None else 0 for k, v in row.items()}
    return {}


@app.get("/api/daily-trend")
def get_daily_trend():
    """Daily workflow counts for time-series chart."""
    query = f"""
    SELECT
        workflow_date,
        SUM(workflow_count) AS total,
        SUM(CASE WHEN status = 'Completed' THEN workflow_count ELSE 0 END) AS completed,
        SUM(CASE WHEN status != 'Completed' THEN workflow_count ELSE 0 END) AS failed
    FROM {CATALOG}.{SCHEMA}.daily_workflow_summary
    GROUP BY workflow_date
    ORDER BY workflow_date
    """
    results = run_query(query)
    return [
        {
            "date": str(r["workflow_date"]),
            "total": int(r["total"]),
            "completed": int(r["completed"]),
            "failed": int(r["failed"]),
        }
        for r in results
    ]


@app.get("/api/workflows-by-type")
def get_workflows_by_type():
    """Workflow type breakdown."""
    query = f"""
    SELECT
        workflow_type,
        SUM(workflow_count) AS count,
        ROUND(SUM(CASE WHEN status = 'Completed' THEN workflow_count ELSE 0 END) * 100.0 / SUM(workflow_count), 1) AS success_rate
    FROM {CATALOG}.{SCHEMA}.daily_workflow_summary
    GROUP BY workflow_type
    ORDER BY count DESC
    """
    results = run_query(query)
    return [
        {
            "workflow_type": r["workflow_type"].replace("Workflow", ""),
            "count": int(r["count"]),
            "success_rate": float(r["success_rate"]),
        }
        for r in results
    ]


@app.get("/api/appointments-by-type")
def get_appointments_by_type():
    """Appointment type breakdown."""
    query = f"""
    SELECT
        appointment_type,
        SUM(total_count) AS total,
        SUM(success_count) AS successful,
        ROUND(SUM(success_count) * 100.0 / SUM(total_count), 1) AS success_rate
    FROM {CATALOG}.{SCHEMA}.appointment_type_metrics
    GROUP BY appointment_type
    ORDER BY total DESC
    """
    results = run_query(query)
    return [
        {
            "appointment_type": r["appointment_type"],
            "total": int(r["total"]),
            "successful": int(r["successful"]),
            "success_rate": float(r["success_rate"]),
        }
        for r in results
    ]


@app.get("/api/facilities")
def get_facilities():
    """Facility utilization metrics."""
    query = f"""
    SELECT * FROM {CATALOG}.{SCHEMA}.facility_utilization
    ORDER BY total_appointments DESC
    """
    results = run_query(query)
    return [
        {
            "facility_name": r["facility_name"],
            "region": r["patient_region"],
            "total_appointments": int(r["total_appointments"]),
            "successful": int(r["successful_appointments"]),
            "unique_providers": int(r["unique_providers"]),
            "unique_patients": int(r["unique_patients"]),
        }
        for r in results
    ]


@app.get("/api/providers")
def get_providers():
    """Provider workload metrics."""
    query = f"""
    SELECT * FROM {CATALOG}.{SCHEMA}.provider_workload
    ORDER BY total_appointments DESC
    """
    results = run_query(query)
    return [
        {
            "provider_name": r["provider_name"],
            "specialty": r["provider_specialty"],
            "total_appointments": int(r["total_appointments"]),
            "successful": int(r["successful_appointments"]),
            "unique_patients": int(r["unique_patients"]),
            "avg_duration_seconds": float(r["avg_workflow_duration_seconds"]),
        }
        for r in results
    ]


@app.get("/api/failures")
def get_failures():
    """Failure analysis breakdown."""
    query = f"""
    SELECT * FROM {CATALOG}.{SCHEMA}.failure_analysis
    ORDER BY failure_count DESC
    """
    results = run_query(query)
    return [
        {
            "workflow_type": r["workflow_type"].replace("Workflow", ""),
            "failure_reason": r["failure_reason"],
            "count": int(r["failure_count"]),
        }
        for r in results
    ]


@app.get("/api/recent-workflows")
def get_recent_workflows():
    """Most recent workflow executions."""
    query = f"""
    SELECT
        workflow_id, workflow_type, status, patient_first_name, patient_last_name,
        appointment_type, facility_name, provider_name, start_time,
        execution_duration_seconds, failure_reason
    FROM {CATALOG}.{SCHEMA}.workflows_silver
    ORDER BY start_time DESC
    LIMIT 50
    """
    results = run_query(query)
    return [
        {
            "workflow_id": r["workflow_id"],
            "workflow_type": r["workflow_type"].replace("Workflow", ""),
            "status": r["status"],
            "patient": f"{r['patient_first_name']} {r['patient_last_name']}",
            "appointment_type": r["appointment_type"],
            "facility": r["facility_name"],
            "provider": r["provider_name"],
            "start_time": str(r["start_time"]),
            "duration_seconds": int(r["execution_duration_seconds"]) if r["execution_duration_seconds"] else 0,
            "failure_reason": r["failure_reason"],
        }
        for r in results
    ]


# --- New Endpoints ---

@app.get("/api/pipeline-metrics")
def get_pipeline_metrics():
    """Record counts at each medallion layer for data flow visualization."""
    # Bronze count
    bronze_q = f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.workflows_bronze"
    bronze_res = run_query(bronze_q)
    bronze_count = int(bronze_res[0]["cnt"]) if bronze_res else 0

    # Silver count
    silver_q = f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.workflows_silver"
    silver_res = run_query(silver_q)
    silver_count = int(silver_res[0]["cnt"]) if silver_res else 0

    # Gold table counts
    gold_tables = [
        "daily_workflow_summary",
        "appointment_type_metrics",
        "facility_utilization",
        "provider_workload",
        "failure_analysis",
        "billing_summary",
    ]
    gold_counts = []
    for table_name in gold_tables:
        try:
            q = f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.{table_name}"
            res = run_query(q)
            gold_counts.append({"name": table_name, "count": int(res[0]["cnt"]) if res else 0})
        except Exception:
            gold_counts.append({"name": table_name, "count": 0})

    return {
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "gold_tables": gold_counts,
        "rows_dropped": max(0, bronze_count - silver_count),
    }


@app.get("/api/tenants")
def get_tenants():
    """List all distinct tenants for invoice dropdown."""
    query = f"""
    SELECT DISTINCT tenant_id, tenant_name
    FROM {CATALOG}.{SCHEMA}.billing_summary
    WHERE tenant_id IS NOT NULL
    ORDER BY tenant_name
    """
    results = run_query(query)
    return [
        {"tenant_id": r["tenant_id"], "tenant_name": r["tenant_name"]}
        for r in results
    ]


@app.get("/api/invoice")
def get_invoice(
    tenant_id: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
):
    """Calculate invoice with line items for a tenant and date range."""
    query = f"""
    SELECT
        appointment_type,
        SUM(billable_count) AS billable_count
    FROM {CATALOG}.{SCHEMA}.billing_summary
    WHERE tenant_id = '{tenant_id}'
      AND service_date >= '{start_date}'
      AND service_date <= '{end_date}'
    GROUP BY appointment_type
    ORDER BY appointment_type
    """
    results = run_query(query)

    # Look up tenant name
    tenant_name = tenant_id
    tenant_q = f"""
    SELECT DISTINCT tenant_name FROM {CATALOG}.{SCHEMA}.billing_summary
    WHERE tenant_id = '{tenant_id}' LIMIT 1
    """
    tenant_res = run_query(tenant_q)
    if tenant_res:
        tenant_name = tenant_res[0]["tenant_name"]

    line_items = []
    total = 0.0
    for r in results:
        appt_type = r["appointment_type"]
        count = int(r["billable_count"])
        unit_price = APPOINTMENT_PRICING.get(appt_type, 0.0)
        subtotal = count * unit_price
        total += subtotal
        line_items.append({
            "appointment_type": appt_type,
            "count": count,
            "unit_price": unit_price,
            "subtotal": round(subtotal, 2),
        })

    return {
        "tenant_id": tenant_id,
        "tenant_name": tenant_name,
        "start_date": start_date,
        "end_date": end_date,
        "line_items": line_items,
        "total": round(total, 2),
    }


@app.get("/api/genie-url")
def get_genie_url():
    """Return Genie Space URL."""
    space_id = os.environ.get("GENIE_SPACE_ID", "")
    if not space_id:
        return {"url": None, "space_id": None}
    # Resolve host from SDK config if env var is missing
    host = os.environ.get("DATABRICKS_HOST", "")
    if not host:
        try:
            w = WorkspaceClient()
            host = w.config.host or ""
        except Exception:
            pass
    if host and not host.startswith("http"):
        host = f"https://{host}"
    url = f"{host}/explore/genie/spaces/{space_id}" if host else None
    return {"url": url, "space_id": space_id}


# --- Serve React Frontend ---
frontend_dir = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.exists(frontend_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(frontend_dir, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve React SPA for all non-API routes."""
        file_path = os.path.join(frontend_dir, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(frontend_dir, "index.html"))
