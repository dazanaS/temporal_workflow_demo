import { useEffect, useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, BarChart, Bar, Legend,
} from "recharts";
import "./App.css";

// --- Types ---
interface Summary {
  total_workflows: number;
  successful_workflows: number;
  failed_workflows: number;
  timed_out_workflows: number;
  success_rate: number;
  avg_duration_seconds: number;
}

interface DailyTrend { date: string; total: number; completed: number; failed: number; }
interface WorkflowType { workflow_type: string; count: number; success_rate: number; }
interface AppointmentType { appointment_type: string; total: number; successful: number; success_rate: number; }
interface Facility { facility_name: string; region: string; total_appointments: number; successful: number; unique_providers: number; unique_patients: number; }
interface RecentWorkflow { workflow_id: string; workflow_type: string; status: string; patient: string; appointment_type: string; facility: string; provider: string; start_time: string; duration_seconds: number; failure_reason: string | null; }
interface Failure { workflow_type: string; failure_reason: string; count: number; }

const COLORS = ["#2563eb", "#7c3aed", "#059669", "#d97706", "#dc2626", "#0891b2"];
const STATUS_COLORS: Record<string, string> = { Completed: "#059669", Failed: "#dc2626", TimedOut: "#d97706" };

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds % 60);
  return `${m}m ${s}s`;
}

function KpiCard({ icon, label, value, sub, color }: {
  icon: string; label: string; value: string | number; sub?: string; color: string;
}) {
  return (
    <div className="kpi-card">
      <div className="kpi-icon" style={{ backgroundColor: `${color}15`, color }}>{icon}</div>
      <div className="kpi-content">
        <div className="kpi-value">{value}</div>
        <div className="kpi-label">{label}</div>
        {sub && <div className="kpi-sub">{sub}</div>}
      </div>
    </div>
  );
}

export default function App() {
  const [summary, setSummary] = useState<Summary | null>(null);
  const [dailyTrend, setDailyTrend] = useState<DailyTrend[]>([]);
  const [workflowTypes, setWorkflowTypes] = useState<WorkflowType[]>([]);
  const [appointmentTypes, setAppointmentTypes] = useState<AppointmentType[]>([]);
  const [facilities, setFacilities] = useState<Facility[]>([]);
  const [recentWorkflows, setRecentWorkflows] = useState<RecentWorkflow[]>([]);
  const [failures, setFailures] = useState<Failure[]>([]);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<"overview" | "facilities" | "recent" | "failures">("overview");

  useEffect(() => {
    async function fetchAll() {
      try {
        const [sumRes, trendRes, wtRes, atRes, facRes, recRes, failRes] = await Promise.all([
          fetch("/api/summary").then(r => r.json()),
          fetch("/api/daily-trend").then(r => r.json()),
          fetch("/api/workflows-by-type").then(r => r.json()),
          fetch("/api/appointments-by-type").then(r => r.json()),
          fetch("/api/facilities").then(r => r.json()),
          fetch("/api/recent-workflows").then(r => r.json()),
          fetch("/api/failures").then(r => r.json()),
        ]);
        setSummary(sumRes);
        setDailyTrend(trendRes);
        setWorkflowTypes(wtRes);
        setAppointmentTypes(atRes);
        setFacilities(facRes);
        setRecentWorkflows(recRes);
        setFailures(failRes);
      } catch (err) {
        console.error("Failed to fetch data:", err);
      } finally {
        setLoading(false);
      }
    }
    fetchAll();
  }, []);

  if (loading) {
    return (
      <div className="loading">
        <div className="spinner" />
        <p>Loading workflow analytics...</p>
      </div>
    );
  }

  return (
    <div className="app">
      <header className="header">
        <div className="header-left">
          <div className="logo">PH</div>
          <div>
            <h1>PocketHealth</h1>
            <span className="header-sub">Temporal Workflow Analytics</span>
          </div>
        </div>
        <div className="header-badge">
          <span className="live-dot" />
          Live Dashboard
        </div>
      </header>

      {summary && (
        <div className="kpi-grid">
          <KpiCard icon="&#9881;" label="Total Workflows" value={summary.total_workflows.toLocaleString()} sub="Last 30 days" color="#2563eb" />
          <KpiCard icon="&#10003;" label="Success Rate" value={`${summary.success_rate}%`} sub={`${summary.successful_workflows.toLocaleString()} completed`} color="#059669" />
          <KpiCard icon="&#9202;" label="Avg Duration" value={formatDuration(summary.avg_duration_seconds)} sub="Per workflow execution" color="#7c3aed" />
          <KpiCard icon="&#10007;" label="Failed / Timed Out" value={summary.failed_workflows + summary.timed_out_workflows} sub={`${summary.failed_workflows} failed, ${summary.timed_out_workflows} timed out`} color="#dc2626" />
        </div>
      )}

      <div className="tabs">
        {(["overview", "facilities", "recent", "failures"] as const).map(tab => (
          <button key={tab} className={`tab ${activeTab === tab ? "active" : ""}`} onClick={() => setActiveTab(tab)}>
            {tab.charAt(0).toUpperCase() + tab.slice(1)}
          </button>
        ))}
      </div>

      {activeTab === "overview" && (
        <>
          <div className="card full-width">
            <h2>Daily Workflow Volume</h2>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={dailyTrend}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="date" tickFormatter={d => new Date(d).toLocaleDateString("en-CA", { month: "short", day: "numeric" })} fontSize={12} />
                <YAxis fontSize={12} />
                <Tooltip labelFormatter={d => new Date(d as string).toLocaleDateString("en-CA", { weekday: "long", month: "long", day: "numeric" })} />
                <Line type="monotone" dataKey="completed" stroke="#059669" strokeWidth={2} name="Completed" dot={false} />
                <Line type="monotone" dataKey="failed" stroke="#dc2626" strokeWidth={2} name="Failed/TimedOut" dot={false} />
                <Legend />
              </LineChart>
            </ResponsiveContainer>
          </div>

          <div className="chart-grid">
            <div className="card">
              <h2>Workflow Types</h2>
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie data={workflowTypes} cx="50%" cy="50%" innerRadius={60} outerRadius={100} dataKey="count" nameKey="workflow_type" label={({ name, value }: any) => `${name}: ${value}`} labelLine={false}>
                    {workflowTypes.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                  </Pie>
                  <Tooltip formatter={(value: any) => Number(value).toLocaleString()} />
                </PieChart>
              </ResponsiveContainer>
            </div>

            <div className="card">
              <h2>Appointments by Type</h2>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={appointmentTypes} layout="vertical" margin={{ left: 120 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis type="number" fontSize={12} />
                  <YAxis type="category" dataKey="appointment_type" fontSize={12} width={115} />
                  <Tooltip />
                  <Bar dataKey="total" fill="#2563eb" radius={[0, 4, 4, 0]} name="Total" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </>
      )}

      {activeTab === "facilities" && (
        <div className="card full-width">
          <h2>Facility Utilization</h2>
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Facility</th>
                  <th>Region</th>
                  <th>Appointments</th>
                  <th>Successful</th>
                  <th>Providers</th>
                  <th>Patients</th>
                </tr>
              </thead>
              <tbody>
                {facilities.map((f, i) => (
                  <tr key={i}>
                    <td className="bold">{f.facility_name}</td>
                    <td>{f.region}</td>
                    <td>{f.total_appointments}</td>
                    <td>{f.successful}</td>
                    <td>{f.unique_providers}</td>
                    <td>{f.unique_patients}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {activeTab === "recent" && (
        <div className="card full-width">
          <h2>Recent Workflow Executions</h2>
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Patient</th>
                  <th>Appointment</th>
                  <th>Facility</th>
                  <th>Provider</th>
                  <th>Duration</th>
                  <th>Time</th>
                </tr>
              </thead>
              <tbody>
                {recentWorkflows.map((w, i) => (
                  <tr key={i}>
                    <td><span className="type-badge">{w.workflow_type}</span></td>
                    <td>
                      <span className="status-badge" style={{ backgroundColor: `${STATUS_COLORS[w.status] || "#6b7280"}15`, color: STATUS_COLORS[w.status] || "#6b7280" }}>
                        {w.status}
                      </span>
                    </td>
                    <td>{w.patient}</td>
                    <td>{w.appointment_type}</td>
                    <td className="truncate">{w.facility}</td>
                    <td>{w.provider}</td>
                    <td>{formatDuration(w.duration_seconds)}</td>
                    <td className="time-cell">{new Date(w.start_time).toLocaleString("en-CA", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {activeTab === "failures" && (
        <div className="card full-width">
          <h2>Failure Analysis</h2>
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Workflow Type</th>
                  <th>Failure Reason</th>
                  <th>Count</th>
                </tr>
              </thead>
              <tbody>
                {failures.map((f, i) => (
                  <tr key={i}>
                    <td><span className="type-badge">{f.workflow_type}</span></td>
                    <td>{f.failure_reason}</td>
                    <td className="bold">{f.count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
