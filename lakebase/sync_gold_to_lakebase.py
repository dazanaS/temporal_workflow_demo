"""
Sync Gold layer tables from Databricks SQL Warehouse into Lakebase (Postgres).
Uses the SQL Statement Execution API (no databricks-sql-connector needed).

Usage:
    python3 sync_gold_to_lakebase.py [--profile PROFILE]
"""

import argparse
import json
import subprocess
import time
import urllib.request
import urllib.error
import psycopg2

# --- Configuration ---
CATALOG = "dazana_classic_ws_catalog"
SCHEMA = "temporal"
WAREHOUSE_ID = "a82088b3bfe8752c"
LAKEBASE_PROJECT = "temporal-lakebase"
LAKEBASE_BRANCH = "production"
LAKEBASE_ENDPOINT = "primary"
LAKEBASE_DATABASE = "temporal"


def get_databricks_auth(profile: str) -> tuple[str, str]:
    """Get Databricks workspace host and token from CLI profile."""
    result = subprocess.run(
        ["databricks", "auth", "token", "-p", profile, "-o", "json"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to get auth token: {result.stderr}")
    data = json.loads(result.stdout)
    token = data.get("access_token") or data.get("token", "")

    result2 = subprocess.run(
        ["databricks", "auth", "profiles", "-o", "json"],
        capture_output=True, text=True,
    )
    profiles_data = json.loads(result2.stdout)
    profile_list = profiles_data.get("profiles", profiles_data) if isinstance(profiles_data, dict) else profiles_data
    host = ""
    for p in profile_list:
        if isinstance(p, dict) and p.get("name") == profile:
            host = p["host"]
            break
    return host.rstrip("/"), token


def run_sql_api(host: str, token: str, query: str) -> list[list]:
    """Execute SQL via Statement Execution API using curl (bypasses SSL issues)."""
    url = f"{host}/api/2.0/sql/statements/"
    payload = json.dumps({
        "warehouse_id": WAREHOUSE_ID,
        "statement": query,
        "wait_timeout": "50s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    })

    result = subprocess.run(
        ["curl", "-s", "-X", "POST", url,
         "-H", f"Authorization: Bearer {token}",
         "-H", "Content-Type: application/json",
         "-d", payload],
        capture_output=True, text=True,
    )
    if not result.stdout.strip():
        raise RuntimeError(f"Empty response from SQL API. stderr: {result.stderr}")
    data = json.loads(result.stdout)

    if "error_code" in data:
        raise RuntimeError(f"API error: {data.get('error_code')}: {data.get('message', '')}")
    if not data.get("status"):
        raise RuntimeError(f"No status in response. Full response: {json.dumps(data)[:500]}")

    status = data.get("status", {}).get("state", "")
    if status == "SUCCEEDED":
        return data.get("result", {}).get("data_array", [])
    elif status in ("PENDING", "RUNNING"):
        stmt_id = data["statement_id"]
        for _ in range(30):
            time.sleep(2)
            poll_url = f"{host}/api/2.0/sql/statements/{stmt_id}"
            poll_result = subprocess.run(
                ["curl", "-s", poll_url, "-H", f"Authorization: Bearer {token}"],
                capture_output=True, text=True,
            )
            poll_data = json.loads(poll_result.stdout)
            if poll_data["status"]["state"] == "SUCCEEDED":
                return poll_data.get("result", {}).get("data_array", [])
            elif poll_data["status"]["state"] == "FAILED":
                raise RuntimeError(f"Query failed: {poll_data['status']}")
    else:
        raise RuntimeError(f"Query failed: {data.get('status', {})}")
    return []


def get_lakebase_connection(profile: str):
    """Connect to Lakebase Postgres."""
    endpoint_path = f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}/endpoints/{LAKEBASE_ENDPOINT}"
    branch_path = f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}"

    result = subprocess.run(
        ["databricks", "postgres", "list-endpoints", branch_path, "-p", profile, "-o", "json"],
        capture_output=True, text=True,
    )
    endpoints = json.loads(result.stdout)
    host = endpoints[0]["status"]["hosts"]["host"]

    result = subprocess.run(
        ["databricks", "postgres", "generate-database-credential", endpoint_path, "-p", profile, "-o", "json"],
        capture_output=True, text=True,
    )
    token = json.loads(result.stdout)["token"]

    result = subprocess.run(
        ["databricks", "current-user", "me", "-p", profile, "-o", "json"],
        capture_output=True, text=True,
    )
    email = json.loads(result.stdout)["userName"]

    return psycopg2.connect(
        host=host, port=5432, database=LAKEBASE_DATABASE,
        user=email, password=token, sslmode="require",
    )


SYNC_CONFIG = [
    {
        "name": "daily_workflow_summary",
        "query": f"SELECT workflow_date, workflow_type, status, workflow_count, avg_duration_seconds, min_duration_seconds, max_duration_seconds FROM {CATALOG}.{SCHEMA}.daily_workflow_summary",
        "upsert": """INSERT INTO daily_workflow_summary (workflow_date, workflow_type, status, workflow_count,
                     avg_duration_seconds, min_duration_seconds, max_duration_seconds, synced_at)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                     ON CONFLICT (workflow_date, workflow_type, status)
                     DO UPDATE SET workflow_count = EXCLUDED.workflow_count,
                                   avg_duration_seconds = EXCLUDED.avg_duration_seconds,
                                   synced_at = NOW()""",
    },
    {
        "name": "appointment_type_metrics",
        "query": f"SELECT appointment_type, workflow_type, total_count, success_count, failed_count, timed_out_count, success_rate_pct, avg_appointment_duration_min FROM {CATALOG}.{SCHEMA}.appointment_type_metrics",
        "upsert": """INSERT INTO appointment_type_metrics (appointment_type, workflow_type, total_count,
                     success_count, failed_count, timed_out_count, success_rate_pct, avg_appointment_duration_min, synced_at)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                     ON CONFLICT (appointment_type, workflow_type)
                     DO UPDATE SET total_count = EXCLUDED.total_count, success_count = EXCLUDED.success_count,
                                   failed_count = EXCLUDED.failed_count, synced_at = NOW()""",
    },
    {
        "name": "facility_utilization",
        "query": f"SELECT facility_id, facility_name, facility_address, patient_region, tenant_id, tenant_name, total_appointments, successful_appointments, unique_providers, unique_patients, active_days FROM {CATALOG}.{SCHEMA}.facility_utilization",
        "upsert": """INSERT INTO facility_utilization (facility_id, facility_name, facility_address, patient_region,
                     tenant_id, tenant_name, total_appointments, successful_appointments, unique_providers,
                     unique_patients, active_days, synced_at)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                     ON CONFLICT (facility_id, tenant_id)
                     DO UPDATE SET total_appointments = EXCLUDED.total_appointments,
                                   successful_appointments = EXCLUDED.successful_appointments, synced_at = NOW()""",
    },
    {
        "name": "provider_workload",
        "query": f"SELECT provider_id, provider_name, provider_specialty, total_appointments, successful_appointments, unique_patients, avg_workflow_duration_seconds FROM {CATALOG}.{SCHEMA}.provider_workload",
        "upsert": """INSERT INTO provider_workload (provider_id, provider_name, provider_specialty,
                     total_appointments, successful_appointments, unique_patients, avg_workflow_duration_seconds, synced_at)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                     ON CONFLICT (provider_id)
                     DO UPDATE SET total_appointments = EXCLUDED.total_appointments,
                                   successful_appointments = EXCLUDED.successful_appointments, synced_at = NOW()""",
    },
    {
        "name": "failure_analysis",
        "query": f"SELECT workflow_type, failure_reason, failure_count, first_occurrence, last_occurrence FROM {CATALOG}.{SCHEMA}.failure_analysis",
        "upsert": """INSERT INTO failure_analysis (workflow_type, failure_reason, failure_count,
                     first_occurrence, last_occurrence, synced_at)
                     VALUES (%s, %s, %s, %s, %s, NOW())
                     ON CONFLICT (workflow_type, failure_reason)
                     DO UPDATE SET failure_count = EXCLUDED.failure_count,
                                   last_occurrence = EXCLUDED.last_occurrence, synced_at = NOW()""",
    },
    {
        "name": "billing_summary",
        "query": f"SELECT tenant_id, tenant_name, facility_id, facility_name, appointment_type, service_date, total_count, billable_count, non_billable_count FROM {CATALOG}.{SCHEMA}.billing_summary",
        "upsert": """INSERT INTO billing_summary (tenant_id, tenant_name, facility_id, facility_name,
                     appointment_type, service_date, total_count, billable_count, non_billable_count, synced_at)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                     ON CONFLICT (tenant_id, facility_id, appointment_type, service_date)
                     DO UPDATE SET billable_count = EXCLUDED.billable_count,
                                   non_billable_count = EXCLUDED.non_billable_count, synced_at = NOW()""",
    },
]


def main():
    parser = argparse.ArgumentParser(description="Sync gold tables to Lakebase")
    parser.add_argument("--profile", default="Dazana-classic-ws", help="Databricks CLI profile")
    args = parser.parse_args()

    print("Getting Databricks auth...")
    host, token = get_databricks_auth(args.profile)

    print("Connecting to Lakebase...")
    lb_conn = get_lakebase_connection(args.profile)

    print("\nStarting sync...\n")

    for config in SYNC_CONFIG:
        print(f"  Syncing {config['name']}...")
        rows = run_sql_api(host, token, config["query"])
        if not rows:
            print(f"    No data")
            continue

        cur = lb_conn.cursor()
        for row in rows:
            # Convert "null" strings to None
            clean_row = [None if v is None or v == "null" else v for v in row]
            cur.execute(config["upsert"], clean_row)
        lb_conn.commit()
        print(f"    {len(rows)} rows synced")

    print("\nSync complete!")

    # Verify counts
    cur = lb_conn.cursor()
    print("\nLakebase table counts:")
    for config in SYNC_CONFIG:
        cur.execute(f"SELECT COUNT(*) FROM {config['name']}")
        count = cur.fetchone()[0]
        print(f"  {config['name']}: {count}")

    lb_conn.close()


if __name__ == "__main__":
    main()
