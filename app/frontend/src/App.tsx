import { useEffect, useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, BarChart, Bar, Legend, AreaChart, Area,
} from "recharts";
import {
  Settings, CheckCircle, Clock, XCircle, MapPin, Phone, Users, Building2,
} from "lucide-react";
import "./App.css";
import type {
  Summary, DailyTrend, WorkflowType, AppointmentType,
  Facility, RecentWorkflow, Failure,
  RegionalDistribution, ConfirmationMethod, HourlyDistribution,
  TopProvider, TenantOverview,
} from "./types";
import { CHART_COLORS, STATUS_COLORS } from "./theme";
import DataFlowTab from "./components/DataFlowTab";
import InvoiceTab from "./components/InvoiceTab";
import GenieTab from "./components/GenieTab";

type TabId = "overview" | "dataflow" | "invoicing" | "facilities" | "recent" | "failures" | "genie";

const TAB_LABELS: Record<TabId, string> = {
  overview: "Overview",
  dataflow: "Data Flow",
  invoicing: "Invoicing",
  facilities: "Facilities",
  recent: "Recent",
  failures: "Failures",
  genie: "Ask AI",
};

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds % 60);
  return `${m}m ${s}s`;
}

function KpiCard({ icon, label, value, sub, color }: {
  icon: React.ReactNode; label: string; value: string | number; sub?: string; color: string;
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
  const [regionalDist, setRegionalDist] = useState<RegionalDistribution[]>([]);
  const [confirmationMethods, setConfirmationMethods] = useState<ConfirmationMethod[]>([]);
  const [hourlyDist, setHourlyDist] = useState<HourlyDistribution[]>([]);
  const [topProviders, setTopProviders] = useState<TopProvider[]>([]);
  const [tenantOverview, setTenantOverview] = useState<TenantOverview[]>([]);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<TabId>("overview");

  useEffect(() => {
    async function fetchAll() {
      try {
        const [sumRes, trendRes, wtRes, atRes, facRes, recRes, failRes,
               regRes, confRes, hourRes, provRes, tenRes] = await Promise.all([
          fetch("/api/summary").then(r => r.json()),
          fetch("/api/daily-trend").then(r => r.json()),
          fetch("/api/workflows-by-type").then(r => r.json()),
          fetch("/api/appointments-by-type").then(r => r.json()),
          fetch("/api/facilities").then(r => r.json()),
          fetch("/api/recent-workflows").then(r => r.json()),
          fetch("/api/failures").then(r => r.json()),
          fetch("/api/regional-distribution").then(r => r.json()).catch(() => []),
          fetch("/api/confirmation-methods").then(r => r.json()).catch(() => []),
          fetch("/api/hourly-distribution").then(r => r.json()).catch(() => []),
          fetch("/api/top-providers").then(r => r.json()).catch(() => []),
          fetch("/api/tenant-overview").then(r => r.json()).catch(() => []),
        ]);
        setSummary(sumRes);
        setDailyTrend(trendRes);
        setWorkflowTypes(wtRes);
        setAppointmentTypes(atRes);
        setFacilities(facRes);
        setRecentWorkflows(recRes);
        setFailures(failRes);
        setRegionalDist(regRes);
        setConfirmationMethods(confRes);
        setHourlyDist(hourRes);
        setTopProviders(provRes);
        setTenantOverview(tenRes);
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
          <KpiCard icon={<Settings size={22} />} label="Total Workflows" value={summary.total_workflows.toLocaleString()} sub="Last 30 days" color={CHART_COLORS[0]} />
          <KpiCard icon={<CheckCircle size={22} />} label="Success Rate" value={`${summary.success_rate}%`} sub={`${summary.successful_workflows.toLocaleString()} completed`} color={CHART_COLORS[2]} />
          <KpiCard icon={<Clock size={22} />} label="Avg Duration" value={formatDuration(summary.avg_duration_seconds)} sub="Per workflow execution" color={CHART_COLORS[1]} />
          <KpiCard icon={<XCircle size={22} />} label="Failed / Timed Out" value={summary.failed_workflows + summary.timed_out_workflows} sub={`${summary.failed_workflows} failed, ${summary.timed_out_workflows} timed out`} color={CHART_COLORS[4]} />
        </div>
      )}

      <div className="tabs">
        {(Object.keys(TAB_LABELS) as TabId[]).map(tab => (
          <button key={tab} className={`tab ${activeTab === tab ? "active" : ""}`} onClick={() => setActiveTab(tab)}>
            {TAB_LABELS[tab]}
          </button>
        ))}
      </div>

      {activeTab === "overview" && (
        <>
          {/* Daily Workflow Volume */}
          <div className="card full-width">
            <h2>Daily Workflow Volume</h2>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={dailyTrend}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey="date" tickFormatter={d => new Date(d).toLocaleDateString("en-CA", { month: "short", day: "numeric" })} fontSize={12} />
                <YAxis fontSize={12} />
                <Tooltip labelFormatter={d => new Date(d as string).toLocaleDateString("en-CA", { weekday: "long", month: "long", day: "numeric" })} />
                <Line type="monotone" dataKey="completed" stroke={STATUS_COLORS.Completed} strokeWidth={2} name="Completed" dot={false} />
                <Line type="monotone" dataKey="failed" stroke={STATUS_COLORS.Failed} strokeWidth={2} name="Failed/TimedOut" dot={false} />
                <Legend />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* Row 1: Workflow Types + Appointments by Type */}
          <div className="chart-grid">
            <div className="card">
              <h2>Workflow Types</h2>
              <div className="pie-with-legend">
                <div className="pie-legend">
                  {workflowTypes.map((wt, i) => (
                    <div key={i} className="pie-legend-item">
                      <div className="pie-legend-swatch" style={{ backgroundColor: CHART_COLORS[i % CHART_COLORS.length] }} />
                      <span className="pie-legend-name">{wt.workflow_type}</span>
                      <span className="pie-legend-value">{wt.count.toLocaleString()}</span>
                    </div>
                  ))}
                </div>
                <div style={{ flex: 1 }}>
                  <ResponsiveContainer width="100%" height={300}>
                    <PieChart>
                      <Pie data={workflowTypes} cx="50%" cy="50%" innerRadius={60} outerRadius={100} dataKey="count" nameKey="workflow_type">
                        {workflowTypes.map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
                      </Pie>
                      <Tooltip formatter={(value: any) => Number(value).toLocaleString()} />
                    </PieChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>

            <div className="card">
              <h2>Appointments by Type</h2>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={appointmentTypes} layout="vertical" margin={{ left: 120 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis type="number" fontSize={12} />
                  <YAxis type="category" dataKey="appointment_type" fontSize={12} width={115} />
                  <Tooltip />
                  <Bar dataKey="total" fill={CHART_COLORS[0]} radius={[0, 4, 4, 0]} name="Total" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Row 2: Regional Distribution + Confirmation Methods */}
          {(regionalDist.length > 0 || confirmationMethods.length > 0) && (
            <div className="chart-grid">
              {regionalDist.length > 0 && (
                <div className="card">
                  <h2><MapPin size={16} style={{ display: "inline", verticalAlign: "middle", marginRight: 6 }} />Regional Distribution</h2>
                  <ResponsiveContainer width="100%" height={250}>
                    <BarChart data={regionalDist} layout="vertical" margin={{ left: 100 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                      <XAxis type="number" fontSize={12} />
                      <YAxis type="category" dataKey="region" fontSize={12} width={95} />
                      <Tooltip formatter={(value: any) => Number(value).toLocaleString()} />
                      <Bar dataKey="count" fill={CHART_COLORS[1]} radius={[0, 4, 4, 0]} name="Appointments" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              )}

              {confirmationMethods.length > 0 && (
                <div className="card">
                  <h2><Phone size={16} style={{ display: "inline", verticalAlign: "middle", marginRight: 6 }} />Confirmation Methods</h2>
                  <div className="pie-with-legend">
                    <div className="pie-legend">
                      {confirmationMethods.map((cm, i) => (
                        <div key={i} className="pie-legend-item">
                          <div className="pie-legend-swatch" style={{ backgroundColor: CHART_COLORS[i % CHART_COLORS.length] }} />
                          <span className="pie-legend-name">{cm.method}</span>
                          <span className="pie-legend-value">{cm.count.toLocaleString()}</span>
                        </div>
                      ))}
                    </div>
                    <div style={{ flex: 1 }}>
                      <ResponsiveContainer width="100%" height={250}>
                        <PieChart>
                          <Pie data={confirmationMethods} cx="50%" cy="50%" innerRadius={55} outerRadius={90} dataKey="count" nameKey="method">
                            {confirmationMethods.map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
                          </Pie>
                          <Tooltip formatter={(value: any) => Number(value).toLocaleString()} />
                        </PieChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Row 3: Hourly Distribution (full width) */}
          {hourlyDist.length > 0 && (
            <div className="card full-width">
              <h2><Clock size={16} style={{ display: "inline", verticalAlign: "middle", marginRight: 6 }} />Hourly Workflow Distribution</h2>
              <ResponsiveContainer width="100%" height={250}>
                <AreaChart data={hourlyDist}>
                  <defs>
                    <linearGradient id="hourlyGradient" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor={CHART_COLORS[0]} stopOpacity={0.3} />
                      <stop offset="95%" stopColor={CHART_COLORS[0]} stopOpacity={0.02} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey="label" fontSize={12} />
                  <YAxis fontSize={12} />
                  <Tooltip />
                  <Area type="monotone" dataKey="count" stroke={CHART_COLORS[0]} strokeWidth={2} fill="url(#hourlyGradient)" name="Workflows" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Row 4: Top Providers + Tenant Overview */}
          {(topProviders.length > 0 || tenantOverview.length > 0) && (
            <div className="chart-grid">
              {topProviders.length > 0 && (
                <div className="card">
                  <h2><Users size={16} style={{ display: "inline", verticalAlign: "middle", marginRight: 6 }} />Top Providers</h2>
                  <ResponsiveContainer width="100%" height={280}>
                    <BarChart data={topProviders} layout="vertical" margin={{ left: 130 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                      <XAxis type="number" fontSize={12} />
                      <YAxis type="category" dataKey="provider_name" fontSize={12} width={125} />
                      <Tooltip formatter={(value: any) => Number(value).toLocaleString()} />
                      <Bar dataKey="total" fill={CHART_COLORS[0]} radius={[0, 4, 4, 0]} name="Total Workflows" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              )}

              {tenantOverview.length > 0 && (
                <div className="card">
                  <h2><Building2 size={16} style={{ display: "inline", verticalAlign: "middle", marginRight: 6 }} />Tenant Overview</h2>
                  <ResponsiveContainer width="100%" height={280}>
                    <BarChart data={tenantOverview} margin={{ left: 10 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                      <XAxis dataKey="tenant_name" fontSize={11} angle={-15} textAnchor="end" height={60} />
                      <YAxis fontSize={12} />
                      <Tooltip />
                      <Legend />
                      <Bar dataKey="completed" stackId="a" fill={STATUS_COLORS.Completed} name="Completed" />
                      <Bar dataKey="failed" stackId="a" fill={STATUS_COLORS.Failed} name="Failed" />
                      <Bar dataKey="timed_out" stackId="a" fill={STATUS_COLORS.TimedOut} name="Timed Out" radius={[4, 4, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              )}
            </div>
          )}
        </>
      )}

      {activeTab === "dataflow" && <DataFlowTab />}
      {activeTab === "invoicing" && <InvoiceTab />}

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

      {activeTab === "genie" && <GenieTab />}
    </div>
  );
}
