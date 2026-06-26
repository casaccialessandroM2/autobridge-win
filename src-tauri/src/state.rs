//! Shared application state.

use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tauri::{AppHandle, Emitter};
use serde::{Deserialize, Serialize};
use chrono::Local;

// ── Commands sent from UI → proxy task ────────────────────────────────────

#[derive(Debug, Clone)]
pub enum ProxyCommand {
    Disconnect,
}

// ── Log entry ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: String,
    pub level:     String,
    pub message:   String,
}

impl LogEntry {
    fn new(level: &str, msg: impl Into<String>) -> Self {
        Self {
            timestamp: Local::now().format("%H:%M:%S%.3f").to_string(),
            level:     level.to_string(),
            message:   msg.into(),
        }
    }
    pub fn info(m: impl Into<String>)  -> Self { Self::new("INFO",  m) }
    pub fn warn(m: impl Into<String>)  -> Self { Self::new("WARN",  m) }
    pub fn error(m: impl Into<String>) -> Self { Self::new("ERROR", m) }
    pub fn doip(m: impl Into<String>)  -> Self { Self::new("DOIP",  m) }
    pub fn debug(m: impl Into<String>) -> Self { Self::new("DEBUG", m) }
}

// ── Interface info ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterfaceInfo {
    pub name:         String,
    pub ip_addresses: Vec<String>,
}

// ── App config ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// IP del Mac che esegue AutoBridge Mac
    pub mac_ip: String,
    /// Porta WebSocket su AutoBridge Mac (default 8765)
    pub mac_ws_port: u16,
    /// Adattatore di rete locale da cui ISTA si aspetta il gateway (es. "192.168.1.10")
    pub local_bind_ip: String,
    /// VIN del veicolo (17 caratteri) — inviato nella Vehicle Identification Response
    pub vin: String,
    /// Etichetta sessione (opzionale)
    pub session_label: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            mac_ip:         String::new(),
            mac_ws_port:    8765,
            local_bind_ip:  String::new(),
            vin:            String::new(),
            session_label:  String::new(),
        }
    }
}

// ── Shared state ───────────────────────────────────────────────────────────

pub struct AppState {
    pub config: Mutex<AppConfig>,
    pub status: Mutex<String>,
    pub logs:   Mutex<Vec<LogEntry>>,
    pub cmd_tx: Mutex<Option<mpsc::Sender<ProxyCommand>>>,
}

impl AppState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            config: Mutex::new(AppConfig::default()),
            status: Mutex::new("Disconnected".to_string()),
            logs:   Mutex::new(Vec::new()),
            cmd_tx: Mutex::new(None),
        })
    }

    pub async fn log(&self, app: &AppHandle, entry: LogEntry) {
        {
            let mut logs = self.logs.lock().await;
            if logs.len() >= 1000 { logs.drain(0..100); }
            logs.push(entry.clone());
        }
        let _ = app.emit("log_entry", &entry);
    }

    pub async fn set_status(&self, app: &AppHandle, status: &str) {
        *self.status.lock().await = status.to_string();
        let _ = app.emit("connection_status", status);
    }
}
