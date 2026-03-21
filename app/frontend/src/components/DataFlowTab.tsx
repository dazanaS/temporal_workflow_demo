import { useEffect, useState } from "react";
import type { PipelineMetrics } from "../types";

function PipelineNode({ icon, label, count, sub }: {
  icon: string; label: string; count: number | string; sub?: string;
}) {
  return (
    <div className="pipeline-node">
      <div className="pipeline-node-box">
        <div className="pipeline-node-icon">{icon}</div>
        <div className="pipeline-node-label">{label}</div>
        <div className="pipeline-node-count">{typeof count === "number" ? count.toLocaleString() : count}</div>
        {sub && <div className="pipeline-node-sub">{sub}</div>}
      </div>
    </div>
  );
}

function Connector({ animated = true }: { animated?: boolean }) {
  return (
    <div className="pipeline-connector">
      <div className={`pipeline-arrow ${animated ? "animated" : ""}`} />
    </div>
  );
}

export default function DataFlowTab() {
  const [metrics, setMetrics] = useState<PipelineMetrics | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/pipeline-metrics")
      .then(r => r.json())
      .then(data => { setMetrics(data); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  if (loading) return <div className="card full-width"><p>Loading pipeline metrics...</p></div>;
  if (!metrics) return <div className="card full-width"><p>Unable to load pipeline metrics.</p></div>;

  const goldTotal = metrics.gold_tables.reduce((s, t) => s + t.count, 0);

  return (
    <div className="data-flow">
      <div className="card full-width">
        <h2>Data Pipeline Flow</h2>
        <div className="pipeline-diagram">
          <PipelineNode icon="&#9200;" label="Temporal" count={metrics.bronze_count} sub="JSON exports" />
          <Connector />
          <PipelineNode icon="&#128230;" label="UC Volume" count={metrics.bronze_count} sub="Raw files" />
          <Connector />
          <PipelineNode icon="&#129513;" label="Bronze" count={metrics.bronze_count} sub="Auto Loader" />
          <Connector />
          <PipelineNode icon="&#129505;" label="Silver" count={metrics.silver_count} sub={`${metrics.rows_dropped} dropped`} />
          <Connector />
          <PipelineNode icon="&#129504;" label="Gold" count={goldTotal} sub={`${metrics.gold_tables.length} views`} />
        </div>
      </div>

      <div className="card full-width">
        <h2>Data Quality Metrics</h2>
        <div className="quality-metrics">
          <div className="quality-metric">
            <div className="quality-metric-value">{metrics.bronze_count.toLocaleString()}</div>
            <div className="quality-metric-label">Records Ingested (Bronze)</div>
          </div>
          <div className="quality-metric">
            <div className="quality-metric-value">{metrics.silver_count.toLocaleString()}</div>
            <div className="quality-metric-label">Valid Records (Silver)</div>
          </div>
          <div className="quality-metric">
            <div className="quality-metric-value">{metrics.rows_dropped.toLocaleString()}</div>
            <div className="quality-metric-label">Rows Dropped (Quality Constraints)</div>
          </div>
        </div>
      </div>

      <div className="card full-width">
        <h2>Gold Layer Tables</h2>
        <div className="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>Table / View</th>
                <th>Record Count</th>
              </tr>
            </thead>
            <tbody>
              {metrics.gold_tables.map((t, i) => (
                <tr key={i}>
                  <td className="bold">{t.name}</td>
                  <td>{t.count.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
