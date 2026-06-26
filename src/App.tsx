import { useState, useEffect, useRef, useCallback } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import type { AppConfig, InterfaceInfo, LogEntry, ConnectionStatus } from "./types";

const DEFAULT_CONFIG: AppConfig = {
  mac_ip:        "",
  mac_ws_port:   8765,
  local_bind_ip: "",
  vin:           "",
  session_label: "",
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
  const canConnect   = !locked && config.mac_ip.trim().length > 0 && config.local_bind_ip.trim().length > 0;

  return (
    <div className="app">

      <header className="header">
        <div className="header-left">
          <span className="logo-icon">⟨/⟩</span>
          <span className="logo-text">AutoBridge <span className="accent">Win</span></span>
          <span className="badge">v0.1.0</span>
          <span className="badge badge-protocol">ISTA / ENET</span>
        </div>
        <div className={`status-pill status-${status.toLowerCase()}`}>
          <span className="status-dot" />
          {status}
        </div>
      </header>

      <div className="layout">
        <aside className="sidebar">

          {/* ── AutoBridge Mac ── */}
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

          {/* ── Adattatore di rete ── */}
          <div className="section-label">Adattatore di rete</div>
          <div className="field">
            <label>Scheda Ethernet (ENET cable)</label>
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
            <span className="field-hint">
              Seleziona la scheda collegata al cavo ENET BMW
            </span>
          </div>

          <div className="divider" />

          {/* ── Veicolo ── */}
          <div className="section-label">Veicolo</div>
          <div className="field">
            <label>VIN (opzionale)</label>
            <input
              type="text"
              placeholder="WBA12345678901234"
              maxLength={17}
              value={config.vin}
              onChange={e => setConfig(p => ({ ...p, vin: e.target.value.toUpperCase() }))}
              disabled={locked}
              style={{ fontFamily: "monospace", letterSpacing: "0.05em" }}
            />
            <span className="field-hint">
              17 caratteri — ISTA lo verifica durante il discovery
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

      <footer className="footer">
        <span>DoIP Proxy — ISO 13400-2 — ISTA/ENET</span>
        <span className="footer-sep">·</span>
        {isConnected && (
          <><span className="accent">{config.local_bind_ip}:13400 ▲</span><span className="footer-sep">·</span></>
        )}
        <span>AutoBridge Win 0.1.0</span>
      </footer>
    </div>
  );
}
