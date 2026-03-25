import { useEffect, useState } from "react";
import { Clock, HardDrive, Database, Layers, Trophy, ChevronDown, ArrowRight } from "lucide-react";
import type { PipelineMetrics } from "../types";

function TierNode({ tier, icon, label, count, sub }: {
  tier: "source" | "bronze" | "silver" | "gold";
  icon: React.ReactNode;
  label: string;
  count: number | string;
  sub?: string;
}) {
  return (
    <div className="pipeline-tier-node">
      <div className={`pipeline-tier-node-icon ${tier}`}>{icon}</div>
      <div className="pipeline-tier-node-info">
        <div className="pipeline-tier-node-label">{label}</div>
        <div className="pipeline-tier-node-count">{typeof count === "number" ? count.toLocaleString() : count}</div>
        {sub && <div className="pipeline-tier-node-sub">{sub}</div>}
      </div>
    </div>
  );
}

function VerticalConnector({ label }: { label: string }) {
  return (
    <div className="pipeline-vertical-connector">
      <div style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
        <div className="pipeline-vertical-line" />
        <ChevronDown size={16} style={{ color: "var(--text-light)", margin: "-4px 0" }} />
      </div>
      <span className="pipeline-vertical-label">{label}</span>
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
        <div className="pipeline-redesign">
          {/* Source Tier */}
          <div className="pipeline-tier pipeline-tier--source">
            <div className="pipeline-tier-label">Source</div>
            <TierNode tier="source" icon={<Clock size={20} />} label="Temporal" count={metrics.bronze_count} sub="JSON exports" />
            <div className="pipeline-tier-arrow"><ArrowRight size={20} /></div>
            <TierNode tier="source" icon={<HardDrive size={20} />} label="UC Volume" count={metrics.bronze_count} sub="Raw files" />
          </div>

          <VerticalConnector label="Auto Loader ingestion into streaming table" />

          {/* Bronze Tier */}
          <div className="pipeline-tier pipeline-tier--bronze">
            <div className="pipeline-tier-label">Bronze</div>
            <TierNode tier="bronze" icon={<Database size={20} />} label="workflows_bronze" count={metrics.bronze_count} sub="Raw records with metadata" />
          </div>

          <VerticalConnector label="Parse, flatten, validate (3 quality constraints)" />

          {/* Silver Tier */}
          <div className="pipeline-tier pipeline-tier--silver">
            <div className="pipeline-tier-label">Silver</div>
            <TierNode tier="silver" icon={<Layers size={20} />} label="workflows_silver" count={metrics.silver_count} sub={`${metrics.rows_dropped} rows dropped`} />
          </div>

          <VerticalConnector label="Aggregate into materialized views" />

          {/* Gold Tier */}
          <div className="pipeline-tier pipeline-tier--gold">
            <div className="pipeline-tier-label">Gold ({goldTotal.toLocaleString()} total rows)</div>
            <div className="pipeline-gold-grid">
              {metrics.gold_tables.map((t, i) => (
                <div key={i} className="pipeline-gold-mini">
                  <div className="pipeline-gold-mini-icon"><Trophy size={14} /></div>
                  <div className="pipeline-gold-mini-info">
                    <div className="pipeline-gold-mini-label">{t.name.replace(/_/g, " ")}</div>
                    <div className="pipeline-gold-mini-count">{t.count.toLocaleString()}</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
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
