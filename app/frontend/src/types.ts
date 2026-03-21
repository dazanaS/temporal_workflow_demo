export interface Summary {
  total_workflows: number;
  successful_workflows: number;
  failed_workflows: number;
  timed_out_workflows: number;
  success_rate: number;
  avg_duration_seconds: number;
}

export interface DailyTrend {
  date: string;
  total: number;
  completed: number;
  failed: number;
}

export interface WorkflowType {
  workflow_type: string;
  count: number;
  success_rate: number;
}

export interface AppointmentType {
  appointment_type: string;
  total: number;
  successful: number;
  success_rate: number;
}

export interface Facility {
  facility_name: string;
  region: string;
  total_appointments: number;
  successful: number;
  unique_providers: number;
  unique_patients: number;
}

export interface RecentWorkflow {
  workflow_id: string;
  workflow_type: string;
  status: string;
  patient: string;
  appointment_type: string;
  facility: string;
  provider: string;
  start_time: string;
  duration_seconds: number;
  failure_reason: string | null;
}

export interface Failure {
  workflow_type: string;
  failure_reason: string;
  count: number;
}

export interface Tenant {
  tenant_id: string;
  tenant_name: string;
}

export interface InvoiceLineItem {
  appointment_type: string;
  count: number;
  unit_price: number;
  subtotal: number;
}

export interface Invoice {
  tenant_id: string;
  tenant_name: string;
  start_date: string;
  end_date: string;
  line_items: InvoiceLineItem[];
  total: number;
}

export interface PipelineMetrics {
  bronze_count: number;
  silver_count: number;
  gold_tables: { name: string; count: number }[];
  rows_dropped: number;
}
