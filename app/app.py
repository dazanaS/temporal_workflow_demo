"""
PocketHealth Temporal Workflow Analytics - Databricks App
FastAPI backend serving React dashboard with SQL Warehouse queries.
"""

import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from databricks.sdk import WorkspaceClient
from databricks import sql as dbsql

# --- Configuration ---
CATALOG = "dazana_classic_ws_catalog"
SCHEMA = "temporal"
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))


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
