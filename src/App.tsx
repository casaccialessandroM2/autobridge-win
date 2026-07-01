import { useState, useEffect, useRef, useCallback } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import type { AppConfig, InterfaceInfo, LogEntry, ConnectionStatus } from "./types";

const DEFAULT_CONFIG: AppConfig = {
  relay_url:     "wss://autobridge-relay-production.up.railway.app",
  session_id:    "",
  local_bind_ip: "",
  vin:           "",
  tcp_port:      6801,
  udp_port:      6811,
};

// ── Palette / stili ──────────────────────────────────────────────────────────
const C = {
  blue: "#5BB8E4", navy: "#1B3F94", red: "#DC0A1E",
  green: "#00e87a", warn: "#ffaa00", danger: "#ff4060",
  text: "#e6edf3", muted: "#8899aa", faint: "#566375",
  panel: "rgba(15,21,32,0.92)", panel2: "rgba(19,28,43,0.92)",
  line: "#1e2535", line2: "#30363d",
};

const S: Record<string, React.CSSProperties> = {
  root: {
    height: "100vh", boxSizing: "border-box", position: "relative",
    color: C.text, fontFamily: "'Inter','Segoe UI',system-ui,sans-serif",
    display: "flex", flexDirection: "column", overflow: "hidden",
    background: `linear-gradient(-45deg,#000 30%,${C.blue} 30%,${C.blue} 46%,${C.navy} 46%,${C.navy} 57%,${C.red} 57%,${C.red} 66%,#000 66%)`,
  },
  overlay: { position: "absolute", inset: 0, background: "rgba(0,0,0,0.62)", zIndex: 0 },
  header: {
    position: "relative", zIndex: 1, display: "flex", alignItems: "center",
    justifyContent: "space-between", padding: "14px 20px",
    borderBottom: `1px solid ${C.line}`,
  },
  logoRow: { display: "flex", alignItems: "center", gap: "10px" },
  logoMark: {
    width: 32, height: 32, borderRadius: 9, display: "flex", alignItems: "center",
    justifyContent: "center", fontSize: 15, fontWeight: 800, color: "#fff",
    background: `linear-gradient(135deg,${C.blue},${C.navy})`,
  },
  logoText: { fontSize: 19, fontWeight: 700, letterSpacing: "-0.5px" },
  badge: {
    fontSize: 9, fontWeight: 700, letterSpacing: "0.8px", color: C.blue,
    border: `1px solid ${C.blue}44`, borderRadius: 5, padding: "2px 7px",
    background: `${C.blue}12`, textTransform: "uppercase",
  },
  body: {
    position: "relative", zIndex: 1, flex: 1, display: "flex", gap: 16,
    padding: 16, overflow: "hidden",
  },
  sidebar: {
    width: 380, display: "flex", flexDirection: "column", gap: 12, overflowY: "auto",
    paddingRight: 4,
  },
  card: {
    background: `linear-gradient(135deg,${C.panel},${C.panel2})`,
    border: `1px solid ${C.line}`, borderRadius: 14, padding: 16,
    display: "flex", flexDirection: "column", gap: 10,
  },
  cardLabel: {
    fontSize: 10, fontWeight: 700, letterSpacing: "1px", color: C.faint,
    textTransform: "uppercase",
  },
  fieldLabel: { fontSize: 11, color: C.muted, fontWeight: 600, marginBottom: 5 },
  input: {
    width: "100%", boxSizing: "border-box", background: "#0b0f17",
    border: `1px solid ${C.line2}`, borderRadius: 10, color: C.text,
    fontSize: 13, padding: "10px 12px", outline: "none",
  },
  ghostBtn: {
    width: "100%", height: 40, borderRadius: 11, cursor: "pointer",
    background: "none", border: `1px solid ${C.line2}`, color: C.blue,
    fontSize: 13, fontWeight: 600, display: "flex", alignItems: "center",
    justifyContent: "center", gap: 8,
  },
  logPanel: {
    flex: 1, minWidth: 0, display: "flex", flexDirection: "column",
    background: "rgba(8,12,20,0.85)", border: `1px solid ${C.line}`,
    borderRadius: 14, overflow: "hidden",
  },
};

const btnStyle = (bg: string, fg: string): React.CSSProperties => ({
  width: "100%", height: 44, borderRadius: 12, border: "none", cursor: "pointer",
  background: bg, color: fg, fontSize: 14, fontWeight: 700, letterSpacing: "0.3px",
  display: "flex", alignItems: "center", justifyContent: "center", gap: 8,
});

function statusInfo(s: ConnectionStatus) {
  if (s === "Connected") return { color: C.green, label: "Tecnico connesso" };
  if (s === "Connecting") return { color: C.warn, label: "Connessione…" };
  return { color: C.danger, label: "Non connesso" };
}
function logColor(l: string) {
  if (l === "ERROR") return C.danger;
  if (l === "WARN") return C.warn;
  if (l === "DOIP") return C.blue;
  if (l === "INFO") return C.green;
  return C.faint;
}

export default function App() {
  const [interfaces, setInterfaces] = useState<InterfaceInfo[]>([]);
  const [config, setConfig] = useState<AppConfig>(DEFAULT_CONFIG);
  const [status, setStatus] = useState<ConnectionStatus>("Disconnected");
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [isBusy, setIsBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [vin, setVin] = useState<string>("");
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [isTestingRelay, setIsTestingRelay] = useState(false);
  const [relayResult, setRelayResult] = useState<{ success: boolean; latency_ms: number; error: string | null } | null>(null);
  const logEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    invoke<InterfaceInfo[]>("get_interfaces").then(ifaces => {
      setInterfaces(ifaces);
      if (ifaces[0]?.ip_addresses[0]) setConfig(p => ({ ...p, local_bind_ip: ifaces[0].ip_addresses[0] }));
    }).catch(console.error);
    invoke<LogEntry[]>("get_logs").then(setLogs).catch(console.error);
    invoke<string>("get_status").then(s => setStatus(s as ConnectionStatus)).catch(console.error);

    const unLog = listen<LogEntry>("log_entry", e => setLogs(p => [...p.slice(-999), e.payload]));
    const unStatus = listen<string>("connection_status", e => { setStatus(e.payload as ConnectionStatus); setIsBusy(false); });
    const unVin = listen<string>("vin_detected", e => { if (e.payload) setVin(e.payload); });
    return () => { unLog.then(f => f()); unStatus.then(f => f()); unVin.then(f => f()); };
  }, []);

  useEffect(() => { logEndRef.current?.scrollIntoView({ behavior: "smooth" }); }, [logs]);

  const handleConnect = async () => {
    setError(null); setIsBusy(true);
    try { await invoke("update_config", { config }); await invoke("connect"); }
    catch (err) { setError(String(err)); setIsBusy(false); }
  };
  const handleDisconnect = async () => {
    setIsBusy(true);
    try { await invoke("disconnect"); } catch (err) { setError(String(err)); setIsBusy(false); }
  };
  const handleTestRelay = async () => {
    setIsTestingRelay(true); setRelayResult(null);
    try {
      await invoke("update_config", { config });
      const r = await invoke<{ success: boolean; latency_ms: number; error: string | null }>("test_relay");
      setRelayResult(r);
    } catch (e) { setRelayResult({ success: false, latency_ms: 0, error: String(e) }); }
    finally { setIsTestingRelay(false); }
  };
  const clearLogs = useCallback(() => setLogs([]), []);

  const isConnected = status === "Connected";
  const isConnecting = status === "Connecting";
  const locked = isConnected || isConnecting;
  const sessionValid = config.session_id.trim().length === 6;
  const canConnect = !locked && sessionValid && config.local_bind_ip.trim().length > 0;
  const st = statusInfo(status);
  const vinChars = (vin || "— — — — — — — — — — — — — — — — —").split("");

  return (
    <div style={S.root}>
      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
        @keyframes fadein { from { opacity:0; transform:translateY(4px);} to {opacity:1;transform:none;} }
        * { scrollbar-width: thin; scrollbar-color: ${C.line2} transparent; }
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-thumb { background: ${C.line2}; border-radius: 8px; }
        input::placeholder { color: ${C.faint}; }
        select, input, button { font-family: inherit; }
      `}</style>
      <div style={S.overlay} />

      {/* Header */}
      <header style={S.header}>
        <div style={S.logoRow}>
          <div style={S.logoMark}>‹/›</div>
          <span style={S.logoText}>AutoBridge <span style={{ color: C.blue }}>Win</span></span>
          <span style={S.badge}>v0.1.3</span>
          <span style={{ ...S.badge, color: C.muted, borderColor: `${C.muted}33`, background: `${C.muted}10` }}>ISTA · Tunnel</span>
        </div>
        <div style={{
          display: "flex", alignItems: "center", gap: 8, padding: "6px 12px",
          borderRadius: 20, background: `${st.color}12`, border: `1px solid ${st.color}44`,
        }}>
          <span style={{ width: 8, height: 8, borderRadius: "50%", background: st.color, boxShadow: `0 0 8px ${st.color}` }} />
          <span style={{ fontSize: 12, fontWeight: 700, color: st.color }}>{st.label}</span>
        </div>
      </header>

      <div style={S.body}>
        {/* ── Colonna sinistra: controlli ── */}
        <aside style={S.sidebar}>

          {/* Veicolo / VIN automatico */}
          <div style={{ ...S.card, alignItems: "center", background: `linear-gradient(135deg,${C.blue}14,${C.navy}14)`, border: `1px solid ${C.blue}33` }}>
            <span style={{ ...S.cardLabel, color: "#8db8d8" }}>Veicolo — VIN rilevato dal Mac</span>
            <div style={{ display: "flex", gap: 3, flexWrap: "wrap", justifyContent: "center", padding: "4px 0" }}>
              {vinChars.map((ch, i) => (
                <span key={i} style={{
                  fontFamily: "'SF Mono',Menlo,monospace", fontSize: 17, fontWeight: 800,
                  color: vin ? C.blue : C.faint, minWidth: 11, textAlign: "center",
                  textShadow: vin ? `0 0 10px ${C.blue}66` : "none",
                }}>{ch}</span>
              ))}
            </div>
            <span style={{ fontSize: 11, color: vin ? C.green : C.faint, fontWeight: 600 }}>
              {vin ? "✓ Telaio ricevuto automaticamente" : "In attesa del telaio dal Mac…"}
            </span>
          </div>

          {/* Codice sessione */}
          <div style={S.card}>
            <span style={S.cardLabel}>Codice sessione (mostrato da AutoBridge Mac)</span>
            <input
              style={{
                ...S.input, textAlign: "center", fontSize: 26, fontWeight: 800,
                letterSpacing: "10px", fontFamily: "'SF Mono',Menlo,monospace",
                textTransform: "uppercase", paddingLeft: 10,
                borderColor: sessionValid ? C.green : C.line2,
                color: sessionValid ? C.green : C.text,
              }}
              placeholder="XXXXXX"
              maxLength={6}
              value={config.session_id}
              onChange={e => setConfig(p => ({ ...p, session_id: e.target.value.toUpperCase().replace(/[^A-Z0-9]/g, "") }))}
              disabled={locked}
            />
          </div>

          {/* Adattatore + porte */}
          <div style={S.card}>
            <span style={S.cardLabel}>Adattatore di rete</span>
            <select
              style={{ ...S.input, cursor: locked ? "not-allowed" : "pointer" }}
              value={config.local_bind_ip}
              onChange={e => setConfig(p => ({ ...p, local_bind_ip: e.target.value }))}
              disabled={locked}
            >
              {interfaces.length === 0 && <option value="">Nessuna interfaccia trovata</option>}
              {interfaces.flatMap(iface => iface.ip_addresses.map(ip => (
                <option key={`${iface.name}-${ip}`} value={ip}>{iface.name} — {ip}</option>
              )))}
            </select>
            <div style={{ display: "flex", gap: 10 }}>
              <div style={{ flex: 1 }}>
                <div style={S.fieldLabel}>Porta TCP</div>
                <input style={S.input} type="number" value={config.tcp_port} disabled={locked}
                  onChange={e => setConfig(p => ({ ...p, tcp_port: Number(e.target.value) }))} />
              </div>
              <div style={{ flex: 1 }}>
                <div style={S.fieldLabel}>Porta UDP</div>
                <input style={S.input} type="number" value={config.udp_port} disabled={locked}
                  onChange={e => setConfig(p => ({ ...p, udp_port: Number(e.target.value) }))} />
              </div>
            </div>
            <span style={{ fontSize: 10, color: C.faint }}>Default BMW ENET: TCP 6801 · UDP 6811</span>
          </div>

          {/* Avanzate (relay + test) */}
          <button style={{ ...S.ghostBtn, color: C.muted, height: 34 }} onClick={() => setShowAdvanced(s => !s)}>
            {showAdvanced ? "▲ Nascondi avanzate" : "▼ Impostazioni avanzate"}
          </button>
          {showAdvanced && (
            <div style={{ ...S.card, animation: "fadein 0.2s" }}>
              <span style={S.cardLabel}>Relay server</span>
              <input style={S.input} value={config.relay_url} disabled={locked}
                onChange={e => setConfig(p => ({ ...p, relay_url: e.target.value }))} />
              <button style={S.ghostBtn} onClick={handleTestRelay} disabled={isTestingRelay || locked}>
                {isTestingRelay
                  ? <><span style={spinner} /> Test in corso…</>
                  : "◎ Test Relay"}
              </button>
              {relayResult && (
                <div style={{
                  fontSize: 12, borderRadius: 10, padding: "10px 12px",
                  background: relayResult.success ? "#0d1f17" : "#1a0d10",
                  border: `1px solid ${relayResult.success ? "#1f4a31" : "#4a1f24"}`,
                  display: "flex", flexDirection: "column", gap: 5,
                }}>
                  <span style={{ fontWeight: 700, color: relayResult.success ? C.green : C.danger }}>
                    {relayResult.success ? `✓ Relay raggiungibile — ${relayResult.latency_ms} ms` : "✗ Relay non raggiungibile"}
                  </span>
                  {relayResult.error && <span style={{ color: C.danger, fontFamily: "monospace", fontSize: 11 }}>{relayResult.error}</span>}
                </div>
              )}
            </div>
          )}

          {error && (
            <div style={{ fontSize: 12, color: C.danger, background: "#1a0d10", border: `1px solid #4a1f24`, borderRadius: 10, padding: "10px 12px" }}>
              {error}
            </div>
          )}

          <div style={{ flex: 1, minHeight: 8 }} />

          {/* Connetti */}
          <button
            style={{
              ...btnStyle(
                isConnected ? "#C0272D" : isConnecting ? "#1a1030" : `linear-gradient(135deg,${C.blue},${C.navy} 60%,${C.red})`,
                "#fff"
              ),
              height: 50, opacity: (!locked && !canConnect) || isBusy ? 0.5 : 1,
              cursor: (!locked && !canConnect) || isBusy ? "not-allowed" : "pointer",
            }}
            onClick={isConnected ? handleDisconnect : handleConnect}
            disabled={isBusy || isConnecting || (!locked && !canConnect)}
          >
            {isConnecting ? <><span style={spinner} /> Connessione…</> : isConnected ? "Disconnetti" : "Connetti"}
          </button>
        </aside>

        {/* ── Colonna destra: log ── */}
        <main style={S.logPanel}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 16px", borderBottom: `1px solid ${C.line}` }}>
            <span style={S.cardLabel}>Registro attività</span>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <span style={{ fontSize: 11, color: C.faint }}>{logs.length} voci</span>
              <button style={{ ...S.ghostBtn, width: "auto", height: 28, padding: "0 12px", fontSize: 11 }} onClick={clearLogs}>Pulisci</button>
            </div>
          </div>
          <div style={{ flex: 1, overflowY: "auto", padding: "8px 12px", fontFamily: "'SF Mono',Menlo,monospace", fontSize: 11.5, lineHeight: 1.7 }}>
            {logs.length === 0 ? (
              <div style={{ height: "100%", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 10, color: C.faint }}>
                <span style={{ fontSize: 28 }}>◎</span>
                <span style={{ fontSize: 13 }}>Inserisci il codice sessione e premi Connetti</span>
              </div>
            ) : logs.map((e, i) => (
              <div key={i} style={{ display: "flex", gap: 8, padding: "1px 0" }}>
                <span style={{ color: C.faint, flexShrink: 0 }}>{e.timestamp}</span>
                <span style={{ color: logColor(e.level), fontWeight: 700, flexShrink: 0, width: 42 }}>{e.level}</span>
                <span style={{ color: "#c8d4e0", wordBreak: "break-word" }}>{e.message}</span>
              </div>
            ))}
            <div ref={logEndRef} />
          </div>
        </main>
      </div>
    </div>
  );
}

const spinner: React.CSSProperties = {
  display: "inline-block", width: 14, height: 14, border: `2px solid ${C.line2}`,
  borderTop: `2px solid ${C.text}`, borderRadius: "50%", animation: "spin 0.7s linear infinite",
};
