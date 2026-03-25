import { useEffect, useState } from "react";
import { FileText, Save, History } from "lucide-react";
import type { Tenant, Invoice, SavedInvoice } from "../types";

const STATUS_STYLES: Record<string, { bg: string; color: string }> = {
  draft: { bg: "rgba(100,116,139,0.1)", color: "#64748b" },
  sent: { bg: "rgba(0,102,255,0.1)", color: "#0066ff" },
  paid: { bg: "rgba(52,168,108,0.1)", color: "#34a86c" },
  cancelled: { bg: "rgba(239,83,80,0.1)", color: "#ef5350" },
};

export default function InvoiceTab() {
  const [tenants, setTenants] = useState<Tenant[]>([]);
  const [selectedTenant, setSelectedTenant] = useState("");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [invoice, setInvoice] = useState<Invoice | null>(null);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveStatus, setSaveStatus] = useState<string | null>(null);
  const [savedInvoices, setSavedInvoices] = useState<SavedInvoice[]>([]);
  const [showHistory, setShowHistory] = useState(false);

  useEffect(() => {
    fetch("/api/tenants")
      .then(r => r.json())
      .then(data => {
        setTenants(data);
        const end = new Date();
        const start = new Date();
        start.setDate(start.getDate() - 30);
        setEndDate(end.toISOString().split("T")[0]);
        setStartDate(start.toISOString().split("T")[0]);
      })
      .catch(console.error);
    loadInvoices();
  }, []);

  const loadInvoices = () => {
    fetch("/api/invoices")
      .then(r => r.json())
      .then(setSavedInvoices)
      .catch(() => setSavedInvoices([]));
  };

  const generateInvoice = async () => {
    if (!selectedTenant || !startDate || !endDate) return;
    setLoading(true);
    setSaveStatus(null);
    try {
      const params = new URLSearchParams({ tenant_id: selectedTenant, start_date: startDate, end_date: endDate });
      const res = await fetch(`/api/invoice?${params}`);
      const data = await res.json();
      setInvoice(data);
    } catch (err) {
      console.error("Failed to generate invoice:", err);
    } finally {
      setLoading(false);
    }
  };

  const saveToVolume = async () => {
    if (!invoice) return;
    setSaving(true);
    setSaveStatus(null);
    try {
      const res = await fetch("/api/invoice/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(invoice),
      });
      const data = await res.json();
      if (res.ok) {
        setSaveStatus(`Saved: ${data.invoice_number || data.filename}`);
        loadInvoices();
      } else {
        setSaveStatus(`Error: ${data.detail}`);
      }
    } catch {
      setSaveStatus("Failed to save invoice.");
    } finally {
      setSaving(false);
    }
  };

  const updateInvoiceStatus = async (id: number, status: string) => {
    try {
      await fetch(`/api/invoices/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status }),
      });
      loadInvoices();
    } catch {
      console.error("Failed to update invoice status");
    }
  };

  const handlePrint = () => window.print();

  const formatCurrency = (n: number) => `$${n.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ",")}`;

  return (
    <div>
      <div className="card full-width">
        <h2>Generate Invoice</h2>
        <div className="invoice-controls">
          <div className="invoice-field">
            <label>Tenant Organization</label>
            <select value={selectedTenant} onChange={e => setSelectedTenant(e.target.value)}>
              <option value="">Select tenant...</option>
              {tenants.map(t => (
                <option key={t.tenant_id} value={t.tenant_id}>{t.tenant_name}</option>
              ))}
            </select>
          </div>
          <div className="invoice-field">
            <label>Start Date</label>
            <input type="date" value={startDate} onChange={e => setStartDate(e.target.value)} />
          </div>
          <div className="invoice-field">
            <label>End Date</label>
            <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)} />
          </div>
          <button className="btn-primary" onClick={generateInvoice} disabled={!selectedTenant || loading}>
            {loading ? "Generating..." : "Generate Invoice"}
          </button>
          {invoice && (
            <>
              <button className="btn-secondary" onClick={handlePrint}>Print / Save PDF</button>
              <button className="btn-primary" onClick={saveToVolume} disabled={saving} style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                <Save size={14} />
                {saving ? "Saving..." : "Save PDF to Volume"}
              </button>
            </>
          )}
          {saveStatus && (
            <span style={{ fontSize: 13, fontWeight: 500, color: saveStatus.startsWith("Error") || saveStatus.startsWith("Failed") ? "var(--danger)" : "var(--success)" }}>
              {saveStatus}
            </span>
          )}
        </div>
      </div>

      {invoice && invoice.line_items.length > 0 ? (
        <div className="card full-width">
          <div className="invoice-preview">
            <div className="invoice-header">
              <div className="invoice-brand">
                <h3>PocketHealth</h3>
                <p>Temporal Workflow Platform</p>
                <p>Healthcare Scheduling Services</p>
              </div>
              <div className="invoice-meta">
                <h4>Invoice</h4>
                <p>Date: {new Date().toLocaleDateString("en-CA")}</p>
                <p>Period: {invoice.start_date} to {invoice.end_date}</p>
              </div>
            </div>

            <div className="invoice-details">
              <div className="invoice-details-col">
                <h5>Bill To</h5>
                <p style={{ fontWeight: 600 }}>{invoice.tenant_name}</p>
                <p>Tenant ID: {invoice.tenant_id}</p>
              </div>
            </div>

            <table className="invoice-table">
              <thead>
                <tr>
                  <th>Service / Appointment Type</th>
                  <th>Billable Count</th>
                  <th>Unit Price</th>
                  <th>Subtotal</th>
                </tr>
              </thead>
              <tbody>
                {invoice.line_items.map((item, i) => (
                  <tr key={i}>
                    <td>{item.appointment_type}</td>
                    <td>{item.count}</td>
                    <td>{formatCurrency(item.unit_price)}</td>
                    <td style={{ fontWeight: 600 }}>{formatCurrency(item.subtotal)}</td>
                  </tr>
                ))}
              </tbody>
            </table>

            <div className="invoice-totals">
              <div className="invoice-total-row grand">
                <span className="invoice-total-label">Total Due</span>
                <span className="invoice-total-value">{formatCurrency(invoice.total)}</span>
              </div>
            </div>
          </div>
        </div>
      ) : invoice ? (
        <div className="card full-width">
          <div className="invoice-empty">
            <p><FileText size={40} /></p>
            <p>No billable appointments found for this tenant and date range.</p>
          </div>
        </div>
      ) : !showHistory ? (
        <div className="card full-width">
          <div className="invoice-empty">
            <p><FileText size={40} /></p>
            <p>Select a tenant and date range, then click "Generate Invoice" to preview.</p>
          </div>
        </div>
      ) : null}

      {/* Invoice History */}
      <div className="card full-width">
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <h2 style={{ margin: 0, display: "flex", alignItems: "center", gap: 8 }}>
            <History size={18} /> Saved Invoices
          </h2>
          <button className="btn-secondary" onClick={() => { setShowHistory(!showHistory); loadInvoices(); }} style={{ fontSize: 12, padding: "4px 12px" }}>
            {showHistory ? "Hide" : "Show"} History
          </button>
        </div>

        {showHistory && (
          savedInvoices.length > 0 ? (
            <div className="table-wrapper">
              <table>
                <thead>
                  <tr>
                    <th>Invoice #</th>
                    <th>Tenant</th>
                    <th>Period</th>
                    <th>Total</th>
                    <th>Status</th>
                    <th>Created</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {savedInvoices.map(inv => {
                    const st = STATUS_STYLES[inv.status] || STATUS_STYLES.draft;
                    return (
                      <tr key={inv.id}>
                        <td className="bold">{inv.invoice_number}</td>
                        <td>{inv.tenant_name}</td>
                        <td style={{ fontSize: 12, color: "var(--text-light)" }}>{inv.start_date} to {inv.end_date}</td>
                        <td className="bold">{formatCurrency(inv.total)}</td>
                        <td>
                          <span className="status-badge" style={{ backgroundColor: st.bg, color: st.color }}>
                            {inv.status}
                          </span>
                        </td>
                        <td className="time-cell">{new Date(inv.created_at).toLocaleDateString("en-CA")}</td>
                        <td>
                          <select
                            value={inv.status}
                            onChange={e => updateInvoiceStatus(inv.id, e.target.value)}
                            style={{
                              fontSize: 11, padding: "3px 6px", border: "1px solid var(--border)",
                              borderRadius: 4, background: "var(--card-bg)", fontFamily: "inherit",
                            }}
                          >
                            <option value="draft">Draft</option>
                            <option value="sent">Sent</option>
                            <option value="paid">Paid</option>
                            <option value="cancelled">Cancelled</option>
                          </select>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <p style={{ color: "var(--text-light)", textAlign: "center", padding: 24 }}>
              No invoices saved yet. Generate and save an invoice to see it here.
            </p>
          )
        )}
      </div>
    </div>
  );
}
