import { useState, useEffect, useRef, useCallback } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import type { AppConfig, InterfaceInfo, LogEntry, ConnectionStatus } from "./types";

const DEFAULT_CONFIG: AppConfig = {
  relay_url:      "ws://localhost:8080",
  session_id:     "",
  local_bind_ip:  "",
  vin:            "",
};

export default function App() {
  const [interfaces, setInterfaces] = useState<InterfaceInfo[]>([]);
  const [config, setConfig]         = useState<AppConfig>(DEFAULT_CONFIG);
  const [status, setStatus]         = useState<ConnectionStatus>("Disconnected");
  const [logs, setLogs]             = useState<LogEntry[]>([]);
  const [isBusy, setIsBusy]         = useState(false);
  const [error, setError]           = useState<string | null>(null);
  const logEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    invoke<InterfaceInfo[]>("get_interfaces")
      .then(ifaces => {
        setInterfaces(ifaces);
        if (ifaces.length > 0 && ifaces[0].ip_addresses[0]) {
          setConfig(p => ({ ...p, local_bind_ip: ifaces[0].ip_addresses[0] }));
        }
      })
      .catch(console.error);

    invoke<LogEntry[]>("get_logs").then(setLogs).catch(console.error);
    invoke<string>("get_status").then(s => setStatus(s as ConnectionStatus)).catch(console.error);

    const unLog    = listen<LogEntry>("log_entry", e =>
      setLogs(prev => [...prev.slice(-999), e.payload]));
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
  const canConnect   = !locked
    && config.session_id.trim().length === 6
    && config.local_bind_ip.trim().length > 0;

  const sessionValid = config.session_id.trim().length === 6;

  return (
    <div className="app">

      <header className="header">
        <div className="header-left">
          <span className="logo-icon">⟨/⟩</span>
          <span className="logo-text">AutoBridge <span className="accent">Win</span></span>
          <span className="badge">v0.1.0</span>
          <span className="badge badge-protocol">ISTA · DoIP</span>
        </div>
        <div className={`status-pill status-${status.toLowerCase()}`}>
          <span className="status-dot" />
          {status}
        </div>
      </header>

      <div className="layout">
        <aside className="sidebar">

          {/* ── Codice sessione ── */}
          <div className="section-label">Codice Sessione Mac</div>

          <div className="field">
            <label>Codice mostrato da AutoBridge Mac</label>
            <input
              type="text"
              placeholder="ABC123"
              maxLength={6}
              value={config.session_id}
              onChange={e => setConfig(p => ({ ...p, session_id: e.target.value.toUpperCase().replace(/[^A-Z0-9]/g, "") }))}
              disabled={locked}
              className={`session-input ${sessionValid ? "valid" : ""}`}
            />
            {config.session_id.length > 0 && !sessionValid && (
              <span className="field-hint warn">Il codice deve essere 6 caratteri</span>
            )}
          </div>

          <div className="field">
            <label>Relay Server URL</label>
            <input
              type="text"
              placeholder="ws://localhost:8080"
              value={config.relay_url}
              onChange={e => setConfig(p => ({ ...p, relay_url: e.target.value }))}
              disabled={locked}
            />
            <span className="field-hint">
              Stesso server usato da AutoBridge Mac
            </span>
          </div>

          <div className="divider" />

          {/* ── Adattatore di rete ── */}
          <div className="section-label">Adattatore di rete</div>
          <div className="field">
            <label>Scheda Ethernet (cavo ENET BMW)</label>
            <select
              value={config.local_bind_ip}
              onChange={e => setConfig(p => ({ ...p, local_bind_ip: e.target.value }))}
              disabled={locked}
            >
              {interfaces.length === 0 && <option value="">Nessuna interfaccia trovata</option>}
              {interfaces.flatMap(iface =>
                iface.ip_addresses.map(ip => (
                  <option key={`${iface.name}-${ip}`} value={ip}>
                    {iface.name} — {ip}
                  </option>
                ))
              )}
            </select>
          </div>

          <div className="divider" />

          {/* ── Veicolo ── */}
          <div className="section-label">Veicolo</div>
          <div className="field">
            <label>VIN (opzionale — ISTA lo verifica)</label>
            <input
              type="text"
              placeholder="WBA12345678901234"
              maxLength={17}
              value={config.vin}
              onChange={e => setConfig(p => ({ ...p, vin: e.target.value.toUpperCase() }))}
              disabled={locked}
              style={{ fontFamily: "monospace", letterSpacing: "0.05em" }}
            />
          </div>

          {error && <div className="error-box">{error}</div>}

          {isConnected && (
            <div className="info-box">
              <div className="info-row">
                <span className="info-label">Sessione</span>
                <span className="accent session-badge">{config.session_id}</span>
              </div>
              <div className="info-row">
                <span className="info-label">UDP/TCP DoIP</span>
                <span className="accent">{config.local_bind_ip}:13400</span>
              </div>
              {config.vin && (
                <div className="info-row">
                  <span className="info-label">VIN</span>
                  <span className="accent" style={{ fontFamily: "monospace" }}>{config.vin}</span>
                </div>
              )}
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
                  <span>Inserisci il codice sessione e premi Connetti</span>
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

      <footer className="footer">
        <span>DoIP Proxy — ISO 13400-2</span>
        <span className="footer-sep">·</span>
        {isConnected && (
          <><span className="accent">{config.local_bind_ip}:13400 ▲</span><span className="footer-sep">·</span></>
        )}
        <span>AutoBridge Win 0.1.0</span>
      </footer>
    </div>
  );
}
