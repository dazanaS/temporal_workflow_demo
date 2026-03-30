-- =============================================================================
-- PocketHealth Temporal Workflow Ingestion Pipeline
-- Lakeflow Declarative Pipeline (formerly DLT)
-- Catalog: demo_catalog | Schema: temporal
-- =============================================================================

-- =============================================================================
-- BRONZE: Raw ingestion from JSON files via Auto Loader
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE workflows_bronze
COMMENT 'Raw Temporal workflow exports ingested from JSON files'
AS SELECT
  *,
  _metadata.file_name AS source_file,
  _metadata.file_modification_time AS file_ingested_at
FROM STREAM read_files(
  '/Volumes/demo_catalog/temporal/workflow_exports/',
  format => 'json',
  multiLine => 'true'
);

-- =============================================================================
-- SILVER: Parsed, flattened, and quality-checked workflow records
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE workflows_silver (
  CONSTRAINT valid_workflow_id EXPECT (workflow_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (status IN ('Completed', 'Failed', 'TimedOut')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_workflow_type EXPECT (workflow_type IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Parsed and validated Temporal workflow records with flattened appointment details'
AS SELECT
  -- Workflow identifiers
  execution.workflowId AS workflow_id,
  execution.runId AS run_id,
  type.name AS workflow_type,
  status,
  taskQueue AS task_queue,
  historyLength AS history_length,
  stateTransitionCount AS state_transition_count,

  -- Timestamps
  CAST(startTime AS TIMESTAMP) AS start_time,
  CAST(closeTime AS TIMESTAMP) AS close_time,
  TIMESTAMPDIFF(SECOND, CAST(startTime AS TIMESTAMP), CAST(closeTime AS TIMESTAMP)) AS execution_duration_seconds,

  -- Tenant details
  result.tenant.tenantId AS tenant_id,
  result.tenant.name AS tenant_name,

  -- Patient details
  result.patient.patientId AS patient_id,
  result.patient.firstName AS patient_first_name,
  result.patient.lastName AS patient_last_name,
  result.patient.dateOfBirth AS patient_date_of_birth,

  -- Provider details
  result.provider.providerId AS provider_id,
  result.provider.name AS provider_name,
  result.provider.specialty AS provider_specialty,

  -- Appointment details
  result.appointmentId AS appointment_id,
  result.appointment.type AS appointment_type,
  CAST(result.appointment.scheduledDate AS DATE) AS appointment_date,
  result.appointment.scheduledTime AS appointment_time,
  result.appointment.durationMinutes AS duration_minutes,

  -- Facility details
  result.appointment.facility.facilityId AS facility_id,
  result.appointment.facility.name AS facility_name,
  result.appointment.facility.address AS facility_address,

  -- Scheduling metadata
  result.scheduling.confirmationMethod AS confirmation_method,
  result.scheduling.reminderSent AS reminder_sent,

  -- Search attributes
  searchAttributes.PatientRegion AS patient_region,

  -- Failure info
  result.failureReason AS failure_reason,

  -- Lineage
  source_file,
  file_ingested_at

FROM STREAM(LIVE.workflows_bronze);

-- =============================================================================
-- GOLD: Daily Workflow Summary
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW daily_workflow_summary
COMMENT 'Aggregated daily workflow metrics by type and status'
AS SELECT
  DATE(start_time) AS workflow_date,
  workflow_type,
  status,
  COUNT(*) AS workflow_count,
  ROUND(AVG(execution_duration_seconds), 1) AS avg_duration_seconds,
  MIN(execution_duration_seconds) AS min_duration_seconds,
  MAX(execution_duration_seconds) AS max_duration_seconds
FROM LIVE.workflows_silver
GROUP BY DATE(start_time), workflow_type, status;

-- =============================================================================
-- GOLD: Appointment Type Metrics
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW appointment_type_metrics
COMMENT 'Appointment type breakdown with success rates'
AS SELECT
  appointment_type,
  workflow_type,
  COUNT(*) AS total_count,
  COUNT(CASE WHEN status = 'Completed' THEN 1 END) AS success_count,
  COUNT(CASE WHEN status = 'Failed' THEN 1 END) AS failed_count,
  COUNT(CASE WHEN status = 'TimedOut' THEN 1 END) AS timed_out_count,
  ROUND(COUNT(CASE WHEN status = 'Completed' THEN 1 END) * 100.0 / COUNT(*), 2) AS success_rate_pct,
  ROUND(AVG(duration_minutes), 0) AS avg_appointment_duration_min
FROM LIVE.workflows_silver
GROUP BY appointment_type, workflow_type;

-- =============================================================================
-- GOLD: Facility Utilization
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW facility_utilization
COMMENT 'Facility-level appointment and utilization metrics'
AS SELECT
  facility_id,
  facility_name,
  facility_address,
  patient_region,
  tenant_id,
  tenant_name,
  COUNT(*) AS total_appointments,
  COUNT(CASE WHEN status = 'Completed' THEN 1 END) AS successful_appointments,
  COUNT(DISTINCT provider_id) AS unique_providers,
  COUNT(DISTINCT patient_id) AS unique_patients,
  COUNT(DISTINCT appointment_date) AS active_days
FROM LIVE.workflows_silver
GROUP BY facility_id, facility_name, facility_address, patient_region, tenant_id, tenant_name;

-- =============================================================================
-- GOLD: Provider Workload
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW provider_workload
COMMENT 'Provider-level workload and patient metrics'
AS SELECT
  provider_id,
  provider_name,
  provider_specialty,
  COUNT(*) AS total_appointments,
  COUNT(CASE WHEN status = 'Completed' THEN 1 END) AS successful_appointments,
  COUNT(DISTINCT patient_id) AS unique_patients,
  ROUND(AVG(execution_duration_seconds), 1) AS avg_workflow_duration_seconds
FROM LIVE.workflows_silver
GROUP BY provider_id, provider_name, provider_specialty;

-- =============================================================================
-- GOLD: Failure Analysis
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW failure_analysis
COMMENT 'Breakdown of workflow failures by type and reason'
AS SELECT
  workflow_type,
  failure_reason,
  COUNT(*) AS failure_count,
  MIN(start_time) AS first_occurrence,
  MAX(start_time) AS last_occurrence
FROM LIVE.workflows_silver
WHERE status IN ('Failed', 'TimedOut')
GROUP BY workflow_type, failure_reason;

-- =============================================================================
-- GOLD: Billing Summary (by tenant, facility, appointment type, date)
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW billing_summary
COMMENT 'Billing-ready summary: billable appointment counts by tenant, facility, type, and date'
AS SELECT
  tenant_id,
  tenant_name,
  facility_id,
  facility_name,
  appointment_type,
  DATE(start_time) AS service_date,
  COUNT(*) AS total_count,
  COUNT(CASE WHEN status = 'Completed' THEN 1 END) AS billable_count,
  COUNT(CASE WHEN status != 'Completed' THEN 1 END) AS non_billable_count
FROM LIVE.workflows_silver
GROUP BY tenant_id, tenant_name, facility_id, facility_name, appointment_type, DATE(start_time);

-- =============================================================================
-- GOLD: Pipeline Metrics (record counts across layers)
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW pipeline_metrics
COMMENT 'Record counts at each medallion layer for data flow visualization'
AS SELECT
  'silver' AS layer,
  COUNT(*) AS record_count,
  COUNT(CASE WHEN status = 'Completed' THEN 1 END) AS valid_count,
  COUNT(CASE WHEN status != 'Completed' THEN 1 END) AS error_count
FROM LIVE.workflows_silver;
