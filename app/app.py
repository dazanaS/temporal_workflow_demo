"""
PocketHealth Temporal Workflow Analytics - Databricks App
FastAPI backend with Lakebase (Postgres) for serving + SQL Warehouse for analytics.
"""

import os
import json
import time
import subprocess
import httpx
import psycopg2
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient
from databricks import sql as dbsql

# --- Configuration ---
CATALOG = "demo_catalog"
SCHEMA = "temporal"
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

# Lakebase Configuration
LAKEBASE_HOST = os.environ.get(
    "LAKEBASE_HOST",
    "ep-odd-bread-d25xag2n.database.us-east-1.cloud.databricks.com",
)
LAKEBASE_DATABASE = os.environ.get("LAKEBASE_DATABASE", "temporal")
LAKEBASE_ENDPOINT = os.environ.get(
    "LAKEBASE_ENDPOINT",
    "projects/temporal-lakebase/branches/production/endpoints/primary",
)

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
        profile = os.environ.get("DATABRICKS_PROFILE", "demo-workspace-pat")
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
    """Execute SQL on Databricks warehouse and return list of dicts."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


# --- Lakebase Connection ---

def get_lakebase_connection():
    """Get a Postgres connection to Lakebase using native credentials or OAuth."""
    lb_user = os.environ.get("LAKEBASE_USER", "")
    lb_password = os.environ.get("LAKEBASE_PASSWORD", "")

    if lb_user and lb_password:
        # Native Postgres login (preferred for Databricks Apps)
        return psycopg2.connect(
            host=LAKEBASE_HOST,
            port=5432,
            database=LAKEBASE_DATABASE,
            user=lb_user,
            password=lb_password,
            sslmode="require",
        )

    # Fallback: OAuth via Lakebase credential API
    if IS_DATABRICKS_APP:
        w = WorkspaceClient()
        ws_host = os.environ.get("DATABRICKS_HOST", "")
        if ws_host and not ws_host.startswith("http"):
            ws_host = f"https://{ws_host}"
    else:
        profile = os.environ.get("DATABRICKS_PROFILE", "demo-workspace-pat")
        w = WorkspaceClient(profile=profile)
        ws_host = w.config.host

    auth = w.config.authenticate()
    ws_token = auth.get("Authorization", "").replace("Bearer ", "") if isinstance(auth, dict) else str(auth)

    resp = httpx.post(
        f"{ws_host}/api/2.0/postgres/credentials",
        headers={"Authorization": f"Bearer {ws_token}", "Content-Type": "application/json"},
        json={"endpoint": LAKEBASE_ENDPOINT},
        timeout=15.0,
    )
    resp.raise_for_status()
    lb_token = resp.json().get("token", "")

    try:
        me = w.current_user.me()
        user = me.user_name or ""
    except Exception:
        user = "user@databricks.com"

    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=5432,
        database=LAKEBASE_DATABASE,
        user=user,
        password=lb_token,
        sslmode="require",
    )


# Track whether Lakebase is reachable
_lakebase_available = None


def _check_lakebase():
    """Check if Lakebase is reachable. Cache the result."""
    global _lakebase_available
    if _lakebase_available is not None:
        return _lakebase_available
    try:
        conn = get_lakebase_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        conn.close()
        _lakebase_available = True
    except Exception as e:
        print(f"[WARN] Lakebase unavailable, falling back to SQL warehouse: {e}")
        _lakebase_available = False
    return _lakebase_available


def run_lakebase_query(query: str, params: tuple | None = None) -> list[dict]:
    """Execute SQL on Lakebase and return list of dicts."""
    conn = get_lakebase_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def run_serving_query(lb_query: str, wh_query: str, lb_params: tuple | None = None) -> list[dict]:
    """Try Lakebase first, fall back to SQL warehouse if unavailable."""
    if _check_lakebase():
        try:
            return run_lakebase_query(lb_query, lb_params)
        except Exception:
            pass
    return run_query(wh_query)


def run_lakebase_execute(query: str, params: tuple | None = None) -> None:
    """Execute a write operation on Lakebase."""
    conn = get_lakebase_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
    finally:
        conn.close()


def run_lakebase_execute_returning(query: str, params: tuple | None = None) -> dict | None:
    """Execute a write on Lakebase and return the first row."""
    conn = get_lakebase_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            return dict(zip(columns, row)) if row else None
        return None
    finally:
        conn.close()


# --- FastAPI App ---
app = FastAPI(title="PocketHealth Temporal Workflow Analytics")


@app.get("/api/summary")
def get_summary():
    """Overall KPI metrics — Lakebase with warehouse fallback."""
    lb_q = """SELECT SUM(workflow_count) AS total_workflows,
        SUM(CASE WHEN status = 'Completed' THEN workflow_count ELSE 0 END) AS successful_workflows,
        SUM(CASE WHEN status = 'Failed' THEN workflow_count ELSE 0 END) AS failed_workflows,
        SUM(CASE WHEN status = 'TimedOut' THEN workflow_count ELSE 0 END) AS timed_out_workflows,
        ROUND(SUM(CASE WHEN status = 'Completed' THEN workflow_count ELSE 0 END) * 100.0 / SUM(workflow_count), 1) AS success_rate,
        ROUND(AVG(avg_duration_seconds), 1) AS avg_duration_seconds
    FROM daily_workflow_summary"""
    wh_q = lb_q.replace("FROM daily_workflow_summary", f"FROM {CATALOG}.{SCHEMA}.daily_workflow_summary")
    results = run_serving_query(lb_q, wh_q)
    if results:
        row = results[0]
        return {k: float(v) if v is not None else 0 for k, v in row.items()}
    return {}


@app.get("/api/daily-trend")
def get_daily_trend():
    """Daily workflow counts — Lakebase with warehouse fallback."""
    lb_q = """SELECT workflow_date, SUM(workflow_count) AS total,
        SUM(CASE WHEN status = 'Completed' THEN workflow_count ELSE 0 END) AS completed,
        SUM(CASE WHEN status != 'Completed' THEN workflow_count ELSE 0 END) AS failed
    FROM daily_workflow_summary GROUP BY workflow_date ORDER BY workflow_date"""
    wh_q = lb_q.replace("FROM daily_workflow_summary", f"FROM {CATALOG}.{SCHEMA}.daily_workflow_summary")
    results = run_serving_query(lb_q, wh_q)
    return [
        {"date": str(r["workflow_date"]), "total": int(r["total"]),
         "completed": int(r["completed"]), "failed": int(r["failed"])}
        for r in results
    ]


@app.get("/api/workflows-by-type")
def get_workflows_by_type():
    """Workflow type breakdown — Lakebase with warehouse fallback."""
    lb_q = """SELECT workflow_type, SUM(workflow_count) AS count,
        ROUND(SUM(CASE WHEN status = 'Completed' THEN workflow_count ELSE 0 END) * 100.0 / SUM(workflow_count), 1) AS success_rate
    FROM daily_workflow_summary GROUP BY workflow_type ORDER BY count DESC"""
    wh_q = lb_q.replace("FROM daily_workflow_summary", f"FROM {CATALOG}.{SCHEMA}.daily_workflow_summary")
    results = run_serving_query(lb_q, wh_q)
    return [
        {"workflow_type": r["workflow_type"].replace("Workflow", ""),
         "count": int(r["count"]), "success_rate": float(r["success_rate"])}
        for r in results
    ]


@app.get("/api/appointments-by-type")
def get_appointments_by_type():
    """Appointment type breakdown — Lakebase with warehouse fallback."""
    lb_q = """SELECT appointment_type, SUM(total_count) AS total, SUM(success_count) AS successful,
        ROUND(SUM(success_count) * 100.0 / SUM(total_count), 1) AS success_rate
    FROM appointment_type_metrics GROUP BY appointment_type ORDER BY total DESC"""
    wh_q = lb_q.replace("FROM appointment_type_metrics", f"FROM {CATALOG}.{SCHEMA}.appointment_type_metrics")
    results = run_serving_query(lb_q, wh_q)
    return [
        {"appointment_type": r["appointment_type"], "total": int(r["total"]),
         "successful": int(r["successful"]), "success_rate": float(r["success_rate"])}
        for r in results
    ]


@app.get("/api/facilities")
def get_facilities():
    """Facility utilization — Lakebase with warehouse fallback."""
    results = run_serving_query(
        "SELECT * FROM facility_utilization ORDER BY total_appointments DESC",
        f"SELECT * FROM {CATALOG}.{SCHEMA}.facility_utilization ORDER BY total_appointments DESC",
    )
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
    """Provider workload — Lakebase with warehouse fallback."""
    results = run_serving_query(
        "SELECT * FROM provider_workload ORDER BY total_appointments DESC",
        f"SELECT * FROM {CATALOG}.{SCHEMA}.provider_workload ORDER BY total_appointments DESC",
    )
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
    """Failure analysis — Lakebase with warehouse fallback."""
    results = run_serving_query(
        "SELECT * FROM failure_analysis ORDER BY failure_count DESC",
        f"SELECT * FROM {CATALOG}.{SCHEMA}.failure_analysis ORDER BY failure_count DESC",
    )
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


# --- Additional Metric Endpoints ---

@app.get("/api/regional-distribution")
def get_regional_distribution():
    """Appointment counts by patient region."""
    query = f"""
    SELECT
        patient_region AS region,
        COUNT(*) AS count,
        ROUND(COUNT(CASE WHEN status = 'Completed' THEN 1 END) * 100.0 / COUNT(*), 1) AS success_rate
    FROM {CATALOG}.{SCHEMA}.workflows_silver
    WHERE patient_region IS NOT NULL
    GROUP BY patient_region
    ORDER BY count DESC
    """
    results = run_query(query)
    return [
        {"region": r["region"], "count": int(r["count"]), "success_rate": float(r["success_rate"])}
        for r in results
    ]


@app.get("/api/confirmation-methods")
def get_confirmation_methods():
    """Confirmation method breakdown for completed workflows."""
    query = f"""
    SELECT
        confirmation_method AS method,
        COUNT(*) AS count
    FROM {CATALOG}.{SCHEMA}.workflows_silver
    WHERE confirmation_method IS NOT NULL
    GROUP BY confirmation_method
    ORDER BY count DESC
    """
    results = run_query(query)
    return [{"method": r["method"], "count": int(r["count"])} for r in results]


@app.get("/api/hourly-distribution")
def get_hourly_distribution():
    """Workflow volume by hour of day."""
    query = f"""
    SELECT
        HOUR(start_time) AS hour,
        COUNT(*) AS count
    FROM {CATALOG}.{SCHEMA}.workflows_silver
    WHERE start_time IS NOT NULL
    GROUP BY HOUR(start_time)
    ORDER BY hour
    """
    results = run_query(query)
    return [
        {"hour": int(r["hour"]), "label": f"{int(r['hour']):02d}:00", "count": int(r["count"])}
        for r in results
    ]


@app.get("/api/top-providers")
def get_top_providers():
    """Top 5 providers by workflow volume."""
    query = f"""
    SELECT
        provider_name,
        COUNT(*) AS total,
        ROUND(COUNT(CASE WHEN status = 'Completed' THEN 1 END) * 100.0 / COUNT(*), 1) AS success_rate
    FROM {CATALOG}.{SCHEMA}.workflows_silver
    WHERE provider_name IS NOT NULL
    GROUP BY provider_name
    ORDER BY total DESC
    LIMIT 5
    """
    results = run_query(query)
    return [
        {"provider_name": r["provider_name"], "total": int(r["total"]), "success_rate": float(r["success_rate"])}
        for r in results
    ]


@app.get("/api/tenant-overview")
def get_tenant_overview():
    """Tenant workflow breakdown by status."""
    query = f"""
    SELECT
        tenant_name,
        COUNT(CASE WHEN status = 'Completed' THEN 1 END) AS completed,
        COUNT(CASE WHEN status = 'Failed' THEN 1 END) AS failed,
        COUNT(CASE WHEN status = 'TimedOut' THEN 1 END) AS timed_out
    FROM {CATALOG}.{SCHEMA}.workflows_silver
    WHERE tenant_name IS NOT NULL
    GROUP BY tenant_name
    ORDER BY (completed + failed + timed_out) DESC
    """
    results = run_query(query)
    return [
        {
            "tenant_name": r["tenant_name"],
            "completed": int(r["completed"]),
            "failed": int(r["failed"]),
            "timed_out": int(r["timed_out"]),
        }
        for r in results
    ]


# --- Pipeline & Data Flow Endpoints ---

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
    """List all distinct tenants — Lakebase with warehouse fallback."""
    results = run_serving_query(
        "SELECT DISTINCT tenant_id, tenant_name FROM billing_summary WHERE tenant_id IS NOT NULL ORDER BY tenant_name",
        f"SELECT DISTINCT tenant_id, tenant_name FROM {CATALOG}.{SCHEMA}.billing_summary WHERE tenant_id IS NOT NULL ORDER BY tenant_name",
    )
    return [{"tenant_id": r["tenant_id"], "tenant_name": r["tenant_name"]} for r in results]


@app.get("/api/invoice")
def get_invoice(
    tenant_id: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
):
    """Calculate invoice — Lakebase with warehouse fallback."""
    if _check_lakebase():
        try:
            results = run_lakebase_query("""
                SELECT appointment_type, SUM(billable_count) AS billable_count
                FROM billing_summary
                WHERE tenant_id = %s AND service_date >= %s AND service_date <= %s
                GROUP BY appointment_type ORDER BY appointment_type
            """, (tenant_id, start_date, end_date))
        except Exception:
            results = run_query(f"""
                SELECT appointment_type, SUM(billable_count) AS billable_count
                FROM {CATALOG}.{SCHEMA}.billing_summary
                WHERE tenant_id = '{tenant_id}' AND service_date >= '{start_date}' AND service_date <= '{end_date}'
                GROUP BY appointment_type ORDER BY appointment_type
            """)
    else:
        results = run_query(f"""
            SELECT appointment_type, SUM(billable_count) AS billable_count
            FROM {CATALOG}.{SCHEMA}.billing_summary
            WHERE tenant_id = '{tenant_id}' AND service_date >= '{start_date}' AND service_date <= '{end_date}'
            GROUP BY appointment_type ORDER BY appointment_type
        """)

    # Look up tenant name
    tenant_name = tenant_id
    tenant_res = run_serving_query(
        "SELECT DISTINCT tenant_name FROM billing_summary WHERE tenant_id = '" + tenant_id + "' LIMIT 1",
        f"SELECT DISTINCT tenant_name FROM {CATALOG}.{SCHEMA}.billing_summary WHERE tenant_id = '{tenant_id}' LIMIT 1",
    )
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


# --- Invoice CRUD (Lakebase with graceful fallback) ---

@app.get("/api/invoices")
def list_invoices(tenant_id: str = Query(None)):
    """List all invoices. Returns empty list if Lakebase is unavailable."""
    try:
        if tenant_id:
            results = run_lakebase_query(
                "SELECT * FROM invoices WHERE tenant_id = %s ORDER BY created_at DESC",
                (tenant_id,),
            )
        else:
            results = run_lakebase_query("SELECT * FROM invoices ORDER BY created_at DESC")
        return [
            {
                "id": r["id"],
                "invoice_number": r["invoice_number"],
                "tenant_id": r["tenant_id"],
                "tenant_name": r["tenant_name"],
                "start_date": str(r["start_date"]),
                "end_date": str(r["end_date"]),
                "total": float(r["total"]),
                "status": r["status"],
                "created_at": str(r["created_at"]),
                "pdf_volume_path": r["pdf_volume_path"],
                "notes": r["notes"],
            }
            for r in results
        ]
    except Exception as e:
        print(f"[WARN] Lakebase unavailable for invoice list: {e}")
        return []


class InvoiceStatusUpdate(BaseModel):
    status: str
    notes: str | None = None


@app.patch("/api/invoices/{invoice_id}")
def update_invoice_status(invoice_id: int, req: InvoiceStatusUpdate):
    """Update invoice status (draft, sent, paid, cancelled)."""
    valid_statuses = {"draft", "sent", "paid", "cancelled"}
    if req.status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    try:
        run_lakebase_execute(
            "UPDATE invoices SET status = %s, notes = COALESCE(%s, notes), updated_at = NOW() WHERE id = %s",
            (req.status, req.notes, invoice_id),
        )
        return {"status": "updated", "invoice_id": invoice_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lakebase unavailable: {str(e)}")


class InvoiceSaveRequest(BaseModel):
    tenant_id: str
    tenant_name: str
    start_date: str
    end_date: str
    line_items: list[dict]
    total: float


@app.post("/api/invoice/save")
def save_invoice(req: InvoiceSaveRequest):
    """Generate a PDF invoice and save it to a UC Volume."""
    from datetime import datetime as dt
    from io import BytesIO
    from fpdf import FPDF

    volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/invoices"
    timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
    filename = f"invoice_{req.tenant_id}_{req.start_date}_{req.end_date}_{timestamp}.pdf"

    def fmt_currency(n: float) -> str:
        return f"${n:,.2f}"

    # --- Build PDF ---
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Header bar
    pdf.set_fill_color(0, 102, 255)
    pdf.rect(10, 10, 190, 1.5, "F")

    # Brand
    pdf.set_font("Helvetica", "B", 22)
    pdf.set_text_color(0, 102, 255)
    pdf.set_y(18)
    pdf.cell(0, 10, "PocketHealth", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(100, 116, 139)
    pdf.cell(0, 5, "Temporal Workflow Platform  |  Healthcare Scheduling Services", new_x="LMARGIN", new_y="NEXT")

    # Invoice title (right aligned)
    pdf.set_y(18)
    pdf.set_font("Helvetica", "B", 28)
    pdf.set_text_color(15, 23, 42)
    pdf.cell(0, 10, "INVOICE", align="R", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(8)
    # Divider
    pdf.set_draw_color(226, 232, 240)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(6)

    # Bill To and Invoice Details side by side
    y_top = pdf.get_y()

    # Left: Bill To
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_text_color(100, 116, 139)
    pdf.cell(95, 5, "BILL TO", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "B", 12)
    pdf.set_text_color(15, 23, 42)
    pdf.cell(95, 6, req.tenant_name, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(100, 116, 139)
    pdf.cell(95, 5, f"Tenant ID: {req.tenant_id}", new_x="LMARGIN", new_y="NEXT")

    # Right: Invoice Details
    pdf.set_y(y_top)
    pdf.set_x(120)
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_text_color(100, 116, 139)
    pdf.cell(70, 5, "INVOICE DETAILS", align="R", new_x="LMARGIN", new_y="NEXT")
    pdf.set_x(120)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(70, 5, f"Date: {dt.now().strftime('%Y-%m-%d')}", align="R", new_x="LMARGIN", new_y="NEXT")
    pdf.set_x(120)
    pdf.cell(70, 5, f"Period: {req.start_date} to {req.end_date}", align="R", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(12)

    # Table Header
    pdf.set_fill_color(248, 250, 252)
    pdf.set_draw_color(226, 232, 240)
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_text_color(100, 116, 139)

    col_widths = [75, 35, 35, 45]
    headers = ["Service / Appointment Type", "Billable Count", "Unit Price", "Subtotal"]
    for i, h in enumerate(headers):
        align = "R" if i > 0 else "L"
        pdf.cell(col_widths[i], 10, h, border="B", fill=True, align=align)
    pdf.ln()

    # Table Rows
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(51, 65, 85)
    for item in req.line_items:
        appt_type = item.get("appointment_type", "")
        count = item.get("count", 0)
        unit_price = item.get("unit_price", 0)
        subtotal = item.get("subtotal", 0)

        pdf.cell(col_widths[0], 9, appt_type, border="B")
        pdf.cell(col_widths[1], 9, str(count), border="B", align="R")
        pdf.cell(col_widths[2], 9, fmt_currency(unit_price), border="B", align="R")
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(col_widths[3], 9, fmt_currency(subtotal), border="B", align="R")
        pdf.set_font("Helvetica", "", 10)
        pdf.ln()

    pdf.ln(4)

    # Total
    pdf.set_draw_color(15, 23, 42)
    pdf.line(120, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(15, 23, 42)
    pdf.set_x(120)
    pdf.cell(35, 10, "Total Due", align="R")
    pdf.cell(45, 10, fmt_currency(req.total), align="R")
    pdf.ln(14)

    # Footer
    pdf.set_draw_color(226, 232, 240)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(4)
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(148, 163, 184)
    pdf.cell(0, 5, f"Generated on {dt.now().strftime('%Y-%m-%d %H:%M:%S')}  |  PocketHealth Temporal Workflow Platform", align="C")

    # Bottom bar
    pdf.set_fill_color(0, 102, 255)
    pdf.rect(10, 285, 190, 1.5, "F")

    # --- Upload PDF to Volume ---
    try:
        if IS_DATABRICKS_APP:
            w = WorkspaceClient()
        else:
            profile = os.environ.get("DATABRICKS_PROFILE", "demo-workspace-pat")
            w = WorkspaceClient(profile=profile)

        file_path = f"{volume_path}/{filename}"
        pdf_bytes = pdf.output()
        w.files.upload(file_path, BytesIO(pdf_bytes), overwrite=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save PDF: {str(e)}")

    # --- Create invoice record in Lakebase (best-effort) ---
    invoice_number = f"INV-{req.tenant_id}-{timestamp}"
    inv = None
    try:
        inv = run_lakebase_execute_returning("""
            INSERT INTO invoices (invoice_number, tenant_id, tenant_name, start_date, end_date, total, status, pdf_volume_path)
            VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s)
            RETURNING id, invoice_number
        """, (invoice_number, req.tenant_id, req.tenant_name, req.start_date, req.end_date, req.total, file_path))

        if inv:
            conn = get_lakebase_connection()
            try:
                cur = conn.cursor()
                for item in req.line_items:
                    cur.execute("""
                        INSERT INTO invoice_line_items (invoice_id, appointment_type, count, unit_price, subtotal)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (inv["id"], item["appointment_type"], item["count"], item["unit_price"], item["subtotal"]))
                conn.commit()
            finally:
                conn.close()
    except Exception as e:
        print(f"[WARN] Lakebase invoice record skipped: {e}")

    return {
        "status": "success",
        "path": file_path,
        "filename": filename,
        "invoice_id": inv["id"] if inv else None,
        "invoice_number": inv.get("invoice_number") if inv else invoice_number,
    }


@app.get("/api/genie-url")
def get_genie_url():
    """Return Genie Space URL."""
    space_id = os.environ.get("GENIE_SPACE_ID", "")
    if not space_id:
        return {"url": None, "space_id": None}
    host = _get_workspace_host()
    url = f"{host}/explore/genie/spaces/{space_id}" if host else None
    return {"url": url, "space_id": space_id}


# --- Genie API Proxy ---

def _get_workspace_host() -> str:
    """Resolve the Databricks workspace host URL."""
    host = os.environ.get("DATABRICKS_HOST", "")
    if not host:
        try:
            w = WorkspaceClient()
            host = w.config.host or ""
        except Exception:
            pass
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host.rstrip("/")


def _get_genie_headers() -> dict:
    """Get auth headers for Genie API calls."""
    if IS_DATABRICKS_APP:
        w = WorkspaceClient()
        token = w.config.authenticate()
        if isinstance(token, dict):
            token = token.get("Authorization", "").replace("Bearer ", "")
    else:
        profile = os.environ.get("DATABRICKS_PROFILE", "demo-workspace-pat")
        w = WorkspaceClient(profile=profile)
        auth = w.config.authenticate()
        if isinstance(auth, dict):
            token = auth.get("Authorization", "").replace("Bearer ", "")
        else:
            token = auth
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


class GenieAskRequest(BaseModel):
    question: str
    conversation_id: str | None = None


class GenieFollowUpRequest(BaseModel):
    question: str
    conversation_id: str


@app.post("/api/genie/ask")
def genie_ask(req: GenieAskRequest):
    """Start a new Genie conversation or send a follow-up, poll until complete, return the answer."""
    space_id = os.environ.get("GENIE_SPACE_ID", "")
    if not space_id:
        raise HTTPException(status_code=400, detail="GENIE_SPACE_ID not configured")

    host = _get_workspace_host()
    if not host:
        raise HTTPException(status_code=500, detail="Cannot resolve workspace host")

    headers = _get_genie_headers()
    base_url = f"{host}/api/2.0/genie/spaces/{space_id}"

    # Start conversation or send follow-up
    if req.conversation_id:
        url = f"{base_url}/conversations/{req.conversation_id}/messages"
        payload = {"content": req.question}
    else:
        url = f"{base_url}/start-conversation"
        payload = {"content": req.question}

    resp = httpx.post(url, headers=headers, json=payload, timeout=30.0)
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    data = resp.json()
    conversation_id = data.get("conversation_id", "")
    message_id = data.get("message_id", "")

    # Poll for completion
    poll_url = f"{base_url}/conversations/{conversation_id}/messages/{message_id}"
    max_polls = 30
    for _ in range(max_polls):
        time.sleep(2)
        poll_resp = httpx.get(poll_url, headers=headers, timeout=30.0)
        if poll_resp.status_code != 200:
            continue
        msg = poll_resp.json()
        status = msg.get("status", "")
        if status in ("COMPLETED", "FAILED"):
            break

    if status != "COMPLETED":
        return {
            "conversation_id": conversation_id,
            "status": status,
            "text": "The query timed out or failed. Please try again.",
            "sql": None,
            "data": None,
            "columns": None,
            "suggested_questions": [],
        }

    # Extract answer parts from attachments
    attachments = msg.get("attachments", [])
    text_answer = ""
    sql_query = ""
    query_description = ""
    suggested_questions = []
    attachment_id = None

    for att in attachments:
        if "text" in att:
            text_answer = att["text"].get("content", "")
        if "query" in att:
            sql_query = att["query"].get("query", "")
            query_description = att["query"].get("description", "")
            attachment_id = att.get("attachment_id", "")
        if "suggested_questions" in att:
            suggested_questions = att["suggested_questions"].get("questions", [])

    # Fetch query result data if there was a SQL query
    columns = None
    result_data = None
    if attachment_id:
        result_url = f"{poll_url}/query-result/{attachment_id}"
        result_resp = httpx.get(result_url, headers=headers, timeout=30.0)
        if result_resp.status_code == 200:
            sr = result_resp.json().get("statement_response", {})
            manifest = sr.get("manifest", {})
            columns = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
            result_data = sr.get("result", {}).get("data_array", [])

    return {
        "conversation_id": conversation_id,
        "status": "COMPLETED",
        "text": text_answer,
        "description": query_description,
        "sql": sql_query,
        "columns": columns,
        "data": result_data,
        "suggested_questions": suggested_questions,
    }


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
