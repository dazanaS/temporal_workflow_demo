import { useEffect, useState } from "react";

export default function GenieTab() {
  const [genieUrl, setGenieUrl] = useState<string | null>(null);
  const [iframeError, setIframeError] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/genie-url")
      .then(r => r.json())
      .then(data => {
        setGenieUrl(data.url || null);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  if (loading) return <div className="card full-width"><p>Loading Genie Space...</p></div>;

  if (!genieUrl) {
    return (
      <div className="card full-width">
        <div className="genie-fallback">
          <p>Genie Space URL is not configured.</p>
          <p style={{ fontSize: 13, color: "var(--text-light)" }}>
            Set the GENIE_SPACE_ID environment variable in app.yaml to enable the AI assistant.
          </p>
        </div>
      </div>
    );
  }

  if (iframeError) {
    return (
      <div className="card full-width">
        <div className="genie-fallback">
          <p>Unable to embed Genie Space (blocked by browser security policy).</p>
          <a href={genieUrl} target="_blank" rel="noopener noreferrer">
            <button className="btn-primary">Open Genie Space in New Tab</button>
          </a>
        </div>
      </div>
    );
  }

  return (
    <div className="card full-width genie-container">
      <h2>Ask AI - Genie Space</h2>
      <p style={{ fontSize: 13, color: "var(--text-light)", marginBottom: 8 }}>
        Ask natural language questions about your workflow data, revenue, facility volumes, and more.
      </p>
      <iframe
        className="genie-iframe"
        src={genieUrl}
        title="Genie Space"
        onError={() => setIframeError(true)}
        sandbox="allow-same-origin allow-scripts allow-forms allow-popups"
      />
    </div>
  );
}
