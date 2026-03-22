import { useState, useRef, useEffect } from "react";

interface GenieMessage {
  role: "user" | "assistant";
  question?: string;
  text?: string;
  description?: string;
  sql?: string | null;
  columns?: string[] | null;
  data?: string[][] | null;
  suggested_questions?: string[];
}

const SAMPLE_QUESTIONS = [
  "What is the total revenue by tenant for the last 30 days?",
  "Which facility has the highest volume of MRI appointments?",
  "What are the top failure reasons for referral workflows?",
  "Show daily appointment trends by appointment type",
  "Compare billing across tenants for March 2026",
];

export default function GenieTab() {
  const [messages, setMessages] = useState<GenieMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [showSql, setShowSql] = useState<Record<number, boolean>>({});
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  const askQuestion = async (question: string) => {
    if (!question.trim() || loading) return;

    const userMsg: GenieMessage = { role: "user", question };
    setMessages(prev => [...prev, userMsg]);
    setInput("");
    setLoading(true);

    try {
      const resp = await fetch("/api/genie/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question, conversation_id: conversationId }),
      });
      const data = await resp.json();

      if (!resp.ok) {
        setMessages(prev => [...prev, {
          role: "assistant",
          text: `Error: ${data.detail || "Something went wrong"}`,
        }]);
        return;
      }

      setConversationId(data.conversation_id);
      setMessages(prev => [...prev, {
        role: "assistant",
        text: data.text || "",
        description: data.description || "",
        sql: data.sql,
        columns: data.columns,
        data: data.data,
        suggested_questions: data.suggested_questions || [],
      }]);
    } catch {
      setMessages(prev => [...prev, {
        role: "assistant",
        text: "Failed to reach the Genie API. Please try again.",
      }]);
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    askQuestion(input);
  };

  const toggleSql = (idx: number) => {
    setShowSql(prev => ({ ...prev, [idx]: !prev[idx] }));
  };

  const newConversation = () => {
    setMessages([]);
    setConversationId(null);
    setShowSql({});
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* Chat area */}
      <div className="card full-width" style={{ padding: 0, overflow: "hidden" }}>
        <div style={{
          display: "flex", justifyContent: "space-between", alignItems: "center",
          padding: "16px 24px", borderBottom: "1px solid var(--border)",
        }}>
          <h2 style={{ margin: 0 }}>Ask AI — Genie Space</h2>
          {messages.length > 0 && (
            <button className="btn-secondary" onClick={newConversation} style={{ fontSize: 12, padding: "4px 12px" }}>
              New Conversation
            </button>
          )}
        </div>

        <div style={{
          minHeight: 400, maxHeight: 600, overflowY: "auto", padding: 24,
          display: "flex", flexDirection: "column", gap: 16,
        }}>
          {messages.length === 0 && !loading && (
            <div style={{ textAlign: "center", padding: "40px 0", color: "var(--text-light)" }}>
              <div style={{ fontSize: 40, marginBottom: 12 }}>&#129302;</div>
              <p style={{ fontSize: 16, fontWeight: 600, color: "var(--text-heading)", marginBottom: 4 }}>
                Ask a question about your workflow data
              </p>
              <p style={{ fontSize: 13, marginBottom: 24 }}>
                Genie connects to your silver and gold tables to answer natural language questions.
              </p>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 8, justifyContent: "center" }}>
                {SAMPLE_QUESTIONS.map((q, i) => (
                  <button
                    key={i}
                    onClick={() => askQuestion(q)}
                    style={{
                      padding: "8px 14px", fontSize: 13, border: "1px solid var(--border)",
                      borderRadius: 20, background: "var(--bg)", color: "var(--text)",
                      cursor: "pointer", fontFamily: "inherit", transition: "all 0.15s",
                    }}
                    onMouseEnter={e => { e.currentTarget.style.borderColor = "var(--primary)"; e.currentTarget.style.color = "var(--primary)"; }}
                    onMouseLeave={e => { e.currentTarget.style.borderColor = "var(--border)"; e.currentTarget.style.color = "var(--text)"; }}
                  >
                    {q}
                  </button>
                ))}
              </div>
            </div>
          )}

          {messages.map((msg, idx) => (
            <div key={idx} style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {msg.role === "user" && (
                <div style={{ display: "flex", justifyContent: "flex-end" }}>
                  <div style={{
                    background: "var(--primary)", color: "white", padding: "10px 16px",
                    borderRadius: "16px 16px 4px 16px", maxWidth: "70%", fontSize: 14,
                  }}>
                    {msg.question}
                  </div>
                </div>
              )}
              {msg.role === "assistant" && (
                <div style={{ display: "flex", justifyContent: "flex-start" }}>
                  <div style={{
                    background: "var(--bg)", border: "1px solid var(--border)",
                    padding: 16, borderRadius: "16px 16px 16px 4px", maxWidth: "85%",
                    fontSize: 14, lineHeight: 1.6,
                  }}>
                    {msg.text && <div dangerouslySetInnerHTML={{ __html: formatMarkdown(msg.text) }} />}

                    {msg.columns && msg.data && msg.data.length > 0 && (
                      <div style={{ marginTop: 12, overflowX: "auto" }}>
                        <table style={{ fontSize: 12, borderCollapse: "collapse", width: "100%" }}>
                          <thead>
                            <tr>
                              {msg.columns.map((col, ci) => (
                                <th key={ci} style={{
                                  textAlign: "left", padding: "8px 10px", fontWeight: 600,
                                  color: "var(--text-light)", fontSize: 11, textTransform: "uppercase",
                                  letterSpacing: "0.05em", borderBottom: "2px solid var(--border)",
                                  whiteSpace: "nowrap",
                                }}>{col}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {msg.data.slice(0, 20).map((row, ri) => (
                              <tr key={ri}>
                                {row.map((cell, ci) => (
                                  <td key={ci} style={{
                                    padding: "6px 10px", borderBottom: "1px solid var(--border)",
                                    whiteSpace: "nowrap",
                                  }}>{cell}</td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                        {msg.data.length > 20 && (
                          <p style={{ fontSize: 11, color: "var(--text-light)", marginTop: 4 }}>
                            Showing 20 of {msg.data.length} rows
                          </p>
                        )}
                      </div>
                    )}

                    {msg.sql && (
                      <div style={{ marginTop: 8 }}>
                        <button
                          onClick={() => toggleSql(idx)}
                          style={{
                            fontSize: 11, color: "var(--text-light)", background: "none",
                            border: "none", cursor: "pointer", padding: 0, fontFamily: "inherit",
                            textDecoration: "underline",
                          }}
                        >
                          {showSql[idx] ? "Hide SQL" : "Show SQL"}
                        </button>
                        {showSql[idx] && (
                          <pre style={{
                            marginTop: 6, padding: 10, background: "var(--bg-alt)",
                            borderRadius: 6, fontSize: 11, overflowX: "auto",
                            color: "var(--text)", fontFamily: "monospace", lineHeight: 1.5,
                          }}>
                            {msg.sql}
                          </pre>
                        )}
                      </div>
                    )}

                    {msg.suggested_questions && msg.suggested_questions.length > 0 && (
                      <div style={{ marginTop: 12, display: "flex", flexWrap: "wrap", gap: 6 }}>
                        {msg.suggested_questions.map((q, qi) => (
                          <button
                            key={qi}
                            onClick={() => askQuestion(q)}
                            disabled={loading}
                            style={{
                              fontSize: 11, padding: "4px 10px", border: "1px solid var(--border)",
                              borderRadius: 12, background: "white", color: "var(--primary)",
                              cursor: loading ? "not-allowed" : "pointer", fontFamily: "inherit",
                            }}
                          >
                            {q}
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          ))}

          {loading && (
            <div style={{ display: "flex", justifyContent: "flex-start" }}>
              <div style={{
                background: "var(--bg)", border: "1px solid var(--border)",
                padding: "12px 20px", borderRadius: "16px 16px 16px 4px",
                fontSize: 14, color: "var(--text-light)", display: "flex", alignItems: "center", gap: 8,
              }}>
                <div className="spinner" style={{ width: 16, height: 16, borderWidth: 2 }} />
                Analyzing your data...
              </div>
            </div>
          )}

          <div ref={messagesEndRef} />
        </div>

        {/* Input bar */}
        <form
          onSubmit={handleSubmit}
          style={{
            display: "flex", gap: 8, padding: "12px 24px",
            borderTop: "1px solid var(--border)", background: "white",
          }}
        >
          <input
            type="text"
            value={input}
            onChange={e => setInput(e.target.value)}
            placeholder="Ask a question about your workflow data..."
            disabled={loading}
            style={{
              flex: 1, padding: "10px 14px", fontSize: 14, border: "1px solid var(--border)",
              borderRadius: 7, outline: "none", fontFamily: "inherit",
              transition: "border-color 0.2s",
            }}
            onFocus={e => e.currentTarget.style.borderColor = "var(--primary)"}
            onBlur={e => e.currentTarget.style.borderColor = "var(--border)"}
          />
          <button
            type="submit"
            className="btn-primary"
            disabled={!input.trim() || loading}
          >
            Ask
          </button>
        </form>
      </div>
    </div>
  );
}

/** Minimal markdown: bold and code */
function formatMarkdown(text: string): string {
  return text
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>")
    .replace(/`([^`]+)`/g, '<code style="font-size:12px;padding:1px 4px;background:var(--bg-alt);border-radius:3px">$1</code>')
    .replace(/\n/g, "<br/>");
}
