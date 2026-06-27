//! AutoBridge Win — Tauri library root.

pub mod proxy;
pub mod state;

use std::sync::Arc;
use tauri::AppHandle;
use tokio::sync::mpsc;

use state::{AppConfig, AppState, InterfaceInfo, LogEntry, ProxyCommand};

// ── Interface enumeration ──────────────────────────────────────────────────

#[tauri::command]
fn get_interfaces() -> Result<Vec<InterfaceInfo>, String> {
    let raw = if_addrs::get_if_addrs()
        .map_err(|e| format!("Impossibile enumerare interfacce: {e}"))?;

    let mut result: Vec<InterfaceInfo> = Vec::new();

    for iface in raw {
        if iface.is_loopback() { continue; }

        let ip = match &iface.addr {
            if_addrs::IfAddr::V4(v4) => v4.ip.to_string(),
            _ => continue,
        };

        if let Some(entry) = result.iter_mut().find(|e| e.name == iface.name) {
            entry.ip_addresses.push(ip);
        } else {
            result.push(InterfaceInfo {
                name:         iface.name.clone(),
                ip_addresses: vec![ip],
            });
        }
    }

    Ok(result)
}

// ── Tauri commands ─────────────────────────────────────────────────────────

#[tauri::command]
async fn get_status(state: tauri::State<'_, Arc<AppState>>) -> Result<String, String> {
    Ok(state.status.lock().await.clone())
}

#[tauri::command]
async fn get_logs(state: tauri::State<'_, Arc<AppState>>) -> Result<Vec<state::LogEntry>, String> {
    Ok(state.logs.lock().await.clone())
}

#[tauri::command]
async fn update_config(
    config: AppConfig,
    state:  tauri::State<'_, Arc<AppState>>,
) -> Result<(), String> {
    *state.config.lock().await = config;
    Ok(())
}

#[tauri::command]
async fn connect(
    app:   AppHandle,
    state: tauri::State<'_, Arc<AppState>>,
) -> Result<(), String> {
    let current = state.status.lock().await.clone();
    if current == "Connected" || current == "Connecting" {
        return Err(format!("Già {current}"));
    }

    let config = state.config.lock().await.clone();
    if config.session_id.trim().is_empty() {
        return Err("Inserisci il codice sessione di AutoBridge Mac".to_string());
    }
    if config.local_bind_ip.trim().is_empty() {
        return Err("Seleziona un adattatore di rete".to_string());
    }

    let (cmd_tx, cmd_rx) = mpsc::channel::<ProxyCommand>(8);
    *state.cmd_tx.lock().await = Some(cmd_tx);

    state.set_status(&app, "Connecting").await;
    state.log(&app, LogEntry::info(format!(
        "Avvio proxy — Relay: {}  Sessione: {}  Adattatore: {}",
        config.relay_url, config.session_id, config.local_bind_ip
    ))).await;

    let state_clone = (*state).clone();
    let app_clone   = app.clone();

    tokio::spawn(async move {
        match proxy::run_proxy(app_clone.clone(), state_clone.clone(), cmd_rx).await {
            Ok(()) => {}
            Err(e) => {
                state_clone.log(&app_clone, LogEntry::error(format!("Proxy error: {e}"))).await;
            }
        }
        state_clone.set_status(&app_clone, "Disconnected").await;
        *state_clone.cmd_tx.lock().await = None;
    });

    Ok(())
}

#[tauri::command]
async fn disconnect(state: tauri::State<'_, Arc<AppState>>) -> Result<(), String> {
    let maybe_tx = state.cmd_tx.lock().await.clone();
    match maybe_tx {
        Some(tx) => tx.send(ProxyCommand::Disconnect).await
            .map_err(|_| "Canale già chiuso".to_string()),
        None => Err("Non connesso".to_string()),
    }
}

// ── Test relay ────────────────────────────────────────────────────────────

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RelayTestResult {
    pub success:      bool,
    pub latency_ms:   u32,
    pub relay_url:    String,
    pub error:        Option<String>,
}

#[tauri::command]
async fn test_relay(state: tauri::State<'_, Arc<AppState>>) -> Result<RelayTestResult, String> {
    use tokio::time::{timeout, Duration, Instant};
    use tokio::net::TcpStream;

    let relay_url = state.config.lock().await.relay_url.clone();

    // Estrai host:port dall'URL WebSocket
    let host_port = relay_url
        .trim_start_matches("wss://")
        .trim_start_matches("ws://")
        .split('/')
        .next()
        .unwrap_or("")
        .to_string();

    // Se non ha porta esplicita usa 443 per wss, 80 per ws
    let addr = if host_port.contains(':') {
        host_port.clone()
    } else if relay_url.starts_with("wss://") {
        format!("{host_port}:443")
    } else {
        format!("{host_port}:80")
    };

    let t0 = Instant::now();
    match timeout(Duration::from_millis(5000), TcpStream::connect(&addr)).await {
        Ok(Ok(_)) => Ok(RelayTestResult {
            success:    true,
            latency_ms: t0.elapsed().as_millis() as u32,
            relay_url,
            error:      None,
        }),
        Ok(Err(e)) => Ok(RelayTestResult {
            success:    false,
            latency_ms: 0,
            relay_url,
            error:      Some(format!("Connessione rifiutata: {e}")),
        }),
        Err(_) => Ok(RelayTestResult {
            success:    false,
            latency_ms: 0,
            relay_url,
            error:      Some("Timeout — relay non raggiungibile".to_string()),
        }),
    }
}

// ── App entry point ────────────────────────────────────────────────────────

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(AppState::new())
        .invoke_handler(tauri::generate_handler![
            get_interfaces,
            get_status,
            get_logs,
            update_config,
            connect,
            disconnect,
            test_relay,
        ])
        .run(tauri::generate_context!())
        .expect("error running AutoBridge Win");
}
