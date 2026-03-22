import { useEffect, useState } from "react";

interface GenieInfo {
  url: string | null;
  space_id: string | null;
}

const SAMPLE_QUESTIONS = [
  "What is the total revenue by tenant for the last 30 days?",
  "Which facility has the highest volume of MRI appointments?",
  "What are the top failure reasons for referral workflows?",
  "Show daily appointment trends by appointment type",
  "Compare billing across tenants for March 2026",
];

export default function GenieTab() {
  const [genie, setGenie] = useState<GenieInfo | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/genie-url")
      .then(r => r.json())
      .then(data => {
        setGenie(data);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  if (loading) return <div className="card full-width"><p>Loading Genie Space...</p></div>;

  if (!genie?.url && !genie?.space_id) {
    return (
      <div className="card full-width">
        <div className="genie-fallback">
          <p style={{ fontSize: 32, marginBottom: 8 }}>&#129302;</p>
          <p><strong>Genie Space not configured</strong></p>
          <p style={{ fontSize: 13, color: "var(--text-light)", marginTop: 4 }}>
            Set the GENIE_SPACE_ID environment variable in app.yaml to enable the AI assistant.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="card full-width">
        <h2>Ask AI - Genie Space</h2>
        <p style={{ fontSize: 14, color: "var(--text-light)", marginBottom: 16 }}>
          Use natural language to query your PocketHealth workflow data — revenue, facility volumes, failure patterns, and more.
          Genie Space connects directly to your silver and gold tables in Unity Catalog.
        </p>
        <div style={{ textAlign: "center", padding: "24px 0" }}>
          {genie.url ? (
            <a href={genie.url} target="_blank" rel="noopener noreferrer">
              <button className="btn-primary" style={{ fontSize: 16, padding: "12px 32px" }}>
                Open Genie Space
              </button>
            </a>
          ) : (
            <p style={{ color: "var(--text-light)" }}>
              Genie Space ID: <code style={{ fontSize: 13, padding: "2px 6px", background: "var(--bg-alt)", borderRadius: 4 }}>{genie.space_id}</code>
              <br />
              <span style={{ fontSize: 13 }}>Open it from your Databricks workspace under Genie.</span>
            </p>
          )}
        </div>
      </div>

      <div className="card full-width">
        <h2>Sample Questions</h2>
        <p style={{ fontSize: 13, color: "var(--text-light)", marginBottom: 16 }}>
          Try asking these in the Genie Space:
        </p>
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {SAMPLE_QUESTIONS.map((q, i) => (
            <div
              key={i}
              style={{
                padding: "10px 14px",
                background: "var(--bg)",
                borderRadius: "var(--radius)",
                fontSize: 14,
                color: "var(--text)",
                border: "1px solid var(--border)",
              }}
            >
              {q}
            </div>
          ))}
        </div>
      </div>

      <div className="card full-width">
        <h2>Connected Tables</h2>
        <div className="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>Layer</th>
                <th>Table</th>
                <th>Description</th>
              </tr>
            </thead>
            <tbody>
              <tr><td><span className="type-badge">Silver</span></td><td className="bold">workflows_silver</td><td>Parsed workflow records with patient, provider, facility, and tenant details</td></tr>
              <tr><td><span className="type-badge" style={{ background: "#34a86c15", color: "#34a86c" }}>Gold</span></td><td className="bold">daily_workflow_summary</td><td>Daily aggregated workflow metrics by type and status</td></tr>
              <tr><td><span className="type-badge" style={{ background: "#34a86c15", color: "#34a86c" }}>Gold</span></td><td className="bold">appointment_type_metrics</td><td>Appointment type breakdown with success rates</td></tr>
              <tr><td><span className="type-badge" style={{ background: "#34a86c15", color: "#34a86c" }}>Gold</span></td><td className="bold">facility_utilization</td><td>Facility-level appointment and utilization metrics</td></tr>
              <tr><td><span className="type-badge" style={{ background: "#34a86c15", color: "#34a86c" }}>Gold</span></td><td className="bold">provider_workload</td><td>Provider-level workload and patient metrics</td></tr>
              <tr><td><span className="type-badge" style={{ background: "#34a86c15", color: "#34a86c" }}>Gold</span></td><td className="bold">failure_analysis</td><td>Failure breakdown by workflow type and reason</td></tr>
              <tr><td><span className="type-badge" style={{ background: "#34a86c15", color: "#34a86c" }}>Gold</span></td><td className="bold">billing_summary</td><td>Billable appointment counts by tenant, facility, type, and date</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
