import { useState, useEffect, useRef, useCallback } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import type { AppConfig, LogEntry, ConnectionStatus } from "./types";

const DEFAULT_CONFIG: AppConfig = {
  mac_ip:          "",
  mac_ws_port:     8765,
  local_doip_port: 13400,
  session_label:   "",
};

export default function App() {
  const [config, setConfig]   = useState<AppConfig>(DEFAULT_CONFIG);
  const [status, setStatus]   = useState<ConnectionStatus>("Disconnected");
  const [logs, setLogs]       = useState<LogEntry[]>([]);
  const [isBusy, setIsBusy]   = useState(false);
  const [error, setError]     = useState<string | null>(null);
  const logEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    invoke<LogEntry[]>("get_logs").then(setLogs).catch(console.error);
    invoke<string>("get_status").then(s => setStatus(s as ConnectionStatus)).catch(console.error);

    const unLog = listen<LogEntry>("log_entry", e => {
      setLogs(prev => [...prev.slice(-999), e.payload]);
    });
    const unStatus = listen<string>("connection_status", e => {
      setStatus(e.payload as ConnectionStatus);
      setIsBusy(false);
    });
    return () => { unLog.then(f => f()); unStatus.then(f => f()); };
  }, []);

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [logs]);

  const handleConnect = async () => {
    setError(null);
    setIsBusy(true);
    try {
      await invoke("update_config", { config });
      await invoke("connect");
    } catch (err) {
      setError(String(err));
      setIsBusy(false);
    }
  };

  const handleDisconnect = async () => {
    setIsBusy(true);
    try { await invoke("disconnect"); }
    catch (err) { setError(String(err)); setIsBusy(false); }
  };

  const clearLogs = useCallback(() => setLogs([]), []);

  const isConnected  = status === "Connected";
  const isConnecting = status === "Connecting";
  const locked       = isConnected || isConnecting;
  const canConnect   = !locked && config.mac_ip.trim().length > 0;

  return (
    <div className="app">

      {/* ── Header ── */}
      <header className="header">
        <div className="header-left">
          <span className="logo-icon">⟨/⟩</span>
          <span className="logo-text">AutoBridge <span className="accent">Win</span></span>
          <span className="badge">v0.1.0</span>
          <span className="badge badge-protocol">DoIP Proxy</span>
        </div>
        <div className={`status-pill status-${status.toLowerCase()}`}>
          <span className="status-dot" />
          {status}
        </div>
      </header>

      {/* ── Layout ── */}
      <div className="layout">

        {/* ── Sidebar ── */}
        <aside className="sidebar">
          <div className="section-label">AutoBridge Mac</div>

          <div className="field">
            <label>IP del Mac</label>
            <input
              type="text"
              placeholder="192.168.1.50"
              value={config.mac_ip}
              onChange={e => setConfig(p => ({ ...p, mac_ip: e.target.value }))}
              disabled={locked}
            />
          </div>

          <div className="field">
            <label>Porta WebSocket Mac</label>
            <input
              type="number"
              value={config.mac_ws_port}
              onChange={e => setConfig(p => ({ ...p, mac_ws_port: parseInt(e.target.value) || 8765 }))}
              disabled={locked}
              min={1024} max={65535}
            />
          </div>

          <div className="divider" />
          <div className="section-label">Software Diagnosi</div>

          <div className="field">
            <label>Porta DoIP locale</label>
            <input
              type="number"
              value={config.local_doip_port}
              onChange={e => setConfig(p => ({ ...p, local_doip_port: parseInt(e.target.value) || 13400 }))}
              disabled={locked}
              min={1024} max={65535}
            />
            <span className="field-hint">
              Il software di diagnosi si connette a 127.0.0.1:{config.local_doip_port}
            </span>
          </div>

          <div className="field">
            <label>Etichetta sessione</label>
            <input
              type="text"
              placeholder="opzionale"
              value={config.session_label}
              onChange={e => setConfig(p => ({ ...p, session_label: e.target.value }))}
              disabled={locked}
            />
          </div>

          {error && <div className="error-box">{error}</div>}

          {isConnected && (
            <div className="info-box">
              <div className="info-row">
                <span className="info-label">Mac WS</span>
                <span className="accent">ws://{config.mac_ip}:{config.mac_ws_port}</span>
              </div>
              <div className="info-row">
                <span className="info-label">DoIP locale</span>
                <span className="accent">127.0.0.1:{config.local_doip_port}</span>
              </div>
            </div>
          )}

          <div className="spacer" />

          <button
            className={`btn-connect ${isConnected ? "btn-disconnect" : ""}`}
            onClick={isConnected ? handleDisconnect : handleConnect}
            disabled={isBusy || isConnecting || (!locked && !canConnect)}
          >
            {isConnecting
              ? <><span className="spinner" /> Connessione…</>
              : isConnected ? "Disconnetti" : "Connetti"}
          </button>
        </aside>

        {/* ── Log panel ── */}
        <main className="log-panel">
          <div className="log-toolbar">
            <span className="section-label" style={{ margin: 0 }}>Log</span>
            <div className="log-toolbar-right">
              <span className="log-count">{logs.length} voci</span>
              <button className="btn-clear" onClick={clearLogs}>Pulisci</button>
            </div>
          </div>
          <div className="log-body">
            {logs.length === 0
              ? (
                <div className="log-empty">
                  <span className="log-empty-icon">◎</span>
                  <span>Nessun log — connettiti per iniziare</span>
                </div>
              )
              : logs.map((e, i) => (
                <div key={i} className={`log-row log-${e.level.toLowerCase()}`}>
                  <span className="log-ts">{e.timestamp}</span>
                  <span className={`log-lvl lvl-${e.level.toLowerCase()}`}>{e.level}</span>
                  <span className="log-msg">{e.message}</span>
                </div>
              ))
            }
            <div ref={logEndRef} />
          </div>
        </main>
      </div>

      {/* ── Footer ── */}
      <footer className="footer">
        <span>DoIP Proxy — ISO 13400-2</span>
        <span className="footer-sep">·</span>
        {isConnected
          ? <><span className="accent">ws://{config.mac_ip}:{config.mac_ws_port} ▲</span><span className="footer-sep">·</span></>
          : null
        }
        <span>AutoBridge Win 0.1.0</span>
      </footer>
    </div>
  );
}
