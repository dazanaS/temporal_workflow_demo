import { useEffect, useState } from "react";
import type { Tenant, Invoice } from "../types";

export default function InvoiceTab() {
  const [tenants, setTenants] = useState<Tenant[]>([]);
  const [selectedTenant, setSelectedTenant] = useState("");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [invoice, setInvoice] = useState<Invoice | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetch("/api/tenants")
      .then(r => r.json())
      .then(data => {
        setTenants(data);
        // Default date range: last 30 days
        const end = new Date();
        const start = new Date();
        start.setDate(start.getDate() - 30);
        setEndDate(end.toISOString().split("T")[0]);
        setStartDate(start.toISOString().split("T")[0]);
      })
      .catch(console.error);
  }, []);

  const generateInvoice = async () => {
    if (!selectedTenant || !startDate || !endDate) return;
    setLoading(true);
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
            <button className="btn-secondary" onClick={handlePrint}>Print / Save PDF</button>
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
            <p>&#128196;</p>
            <p>No billable appointments found for this tenant and date range.</p>
          </div>
        </div>
      ) : (
        <div className="card full-width">
          <div className="invoice-empty">
            <p>&#128196;</p>
            <p>Select a tenant and date range, then click "Generate Invoice" to preview.</p>
          </div>
        </div>
      )}
    </div>
  );
}
