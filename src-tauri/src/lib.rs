//! AutoBridge Win — Tauri library root.

pub mod proxy;
pub mod state;

use std::sync::Arc;
use tauri::AppHandle;
use tokio::sync::mpsc;

use state::{AppConfig, AppState, LogEntry, ProxyCommand};

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
        return Err(format!("Already {current}"));
    }

    let config = state.config.lock().await.clone();
    if config.mac_ip.trim().is_empty() {
        return Err("IP del Mac AutoBridge richiesto".to_string());
    }

    let (cmd_tx, cmd_rx) = mpsc::channel::<ProxyCommand>(8);
    *state.cmd_tx.lock().await = Some(cmd_tx);

    state.set_status(&app, "Connecting").await;
    state.log(&app, LogEntry::info(format!(
        "Avvio proxy — Mac: {}:{}  DoIP locale: {}",
        config.mac_ip, config.mac_ws_port, config.local_doip_port
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

// ── App entry point ────────────────────────────────────────────────────────

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(AppState::new())
        .invoke_handler(tauri::generate_handler![
            get_status,
            get_logs,
            update_config,
            connect,
            disconnect,
        ])
        .run(tauri::generate_context!())
        .expect("error running AutoBridge Win");
}
