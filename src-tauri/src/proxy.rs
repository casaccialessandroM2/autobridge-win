//! DoIP TCP ↔ WebSocket proxy core.
//!
//! Architecture
//! ────────────
//!   Software diagnosi (Windows)
//!     └─ TCP connect → local_doip_port
//!              ↕ raw DoIP frames
//!   [tcp_to_ws task]  reads from TCP, sends binary frames to WS
//!   [ws_to_tcp task]  reads binary frames from WS, writes to TCP
//!              ↕ WebSocket binary frames
//!   AutoBridge Mac  ws://mac_ip:mac_ws_port
//!
//! One diagnosis client at a time is supported.  If a second client connects
//! while one is already active, it is rejected immediately.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tauri::AppHandle;

use crate::state::{AppState, LogEntry, ProxyCommand};

// DoIP header is always 8 bytes
const HEADER_LEN: usize = 8;
const MAX_PAYLOAD: usize = 1 << 20; // 1 MiB sanity limit

// ── Public entry point ─────────────────────────────────────────────────────

pub async fn run_proxy(
    app:        AppHandle,
    state:      Arc<AppState>,
    mut cmd_rx: mpsc::Receiver<ProxyCommand>,
) -> Result<(), String> {

    let config = state.config.lock().await.clone();
    let ws_url = format!("ws://{}:{}", config.mac_ip, config.mac_ws_port);
    let local_addr = format!("0.0.0.0:{}", config.local_doip_port);

    // ── Connect to Mac AutoBridge via WebSocket ────────────────────────────
    state.log(&app, LogEntry::info(format!(
        "Connecting to AutoBridge Mac at {ws_url}"
    ))).await;

    let (ws_stream, _) = timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(&ws_url),
    )
    .await
    .map_err(|_| format!("Timeout connecting to {ws_url}"))?
    .map_err(|e| format!("WebSocket connect failed: {e}"))?;

    state.log(&app, LogEntry::info(format!("WebSocket connected to {ws_url}"))).await;
    state.set_status(&app, "Connected").await;

    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    // ── Listen for diagnosis software on local TCP port ────────────────────
    let listener = TcpListener::bind(&local_addr).await
        .map_err(|e| format!("Cannot bind TCP {local_addr}: {e}"))?;

    state.log(&app, LogEntry::info(format!(
        "Listening for diagnosis software on TCP {local_addr}"
    ))).await;

    // Channels: TCP reader → WS sender, WS reader → TCP writer
    let (tcp_to_ws_tx, mut tcp_to_ws_rx) = mpsc::channel::<Vec<u8>>(128);
    let (ws_to_tcp_tx, mut ws_to_tcp_rx) = mpsc::channel::<Vec<u8>>(128);

    // Flag set when either side closes so both tasks can exit
    let shutdown = Arc::new(AtomicBool::new(false));

    // ── WS receive task: forwards WS frames → TCP writer channel ──────────
    let ws_rx_shutdown = shutdown.clone();
    let ws_to_tcp_tx2  = ws_to_tcp_tx.clone();
    let app_ws  = app.clone();
    let st_ws   = state.clone();
    tokio::spawn(async move {
        while let Some(msg) = ws_rx.next().await {
            if ws_rx_shutdown.load(Ordering::Relaxed) { break; }
            match msg {
                Ok(Message::Binary(data)) => {
                    st_ws.log(&app_ws, LogEntry::doip(
                        format!("← Mac→Win [WS] {} bytes", data.len())
                    )).await;
                    if ws_to_tcp_tx2.send(data.to_vec()).await.is_err() { break; }
                }
                Ok(Message::Close(_)) | Err(_) => {
                    st_ws.log(&app_ws, LogEntry::warn("WebSocket closed by Mac")).await;
                    ws_rx_shutdown.store(true, Ordering::Relaxed);
                    break;
                }
                _ => {}
            }
        }
    });

    // ── WS send task: forwards tcp_to_ws_rx → WebSocket ───────────────────
    let ws_tx_shutdown = shutdown.clone();
    tokio::spawn(async move {
        while let Some(data) = tcp_to_ws_rx.recv().await {
            if ws_tx_shutdown.load(Ordering::Relaxed) { break; }
            if ws_tx.send(Message::Binary(data.into())).await.is_err() {
                ws_tx_shutdown.store(true, Ordering::Relaxed);
                break;
            }
        }
    });

    // ── Accept loop: one diagnosis client at a time ────────────────────────
    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((tcp_stream, peer)) => {
                        state.log(&app, LogEntry::info(
                            format!("Diagnosis software connected from {peer}")
                        )).await;

                        handle_diagnosis_client(
                            tcp_stream,
                            peer,
                            tcp_to_ws_tx.clone(),
                            &mut ws_to_tcp_rx,
                            &app,
                            &state,
                            &shutdown,
                        ).await;

                        state.log(&app, LogEntry::info(
                            format!("Diagnosis software disconnected: {peer}")
                        )).await;
                    }
                    Err(e) => {
                        state.log(&app, LogEntry::warn(format!("TCP accept error: {e}"))).await;
                    }
                }
            }

            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(ProxyCommand::Disconnect) | None => {
                        state.log(&app, LogEntry::info("Disconnect requested")).await;
                        shutdown.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }

        if shutdown.load(Ordering::Relaxed) { break; }
    }

    Ok(())
}

// ── Per-client handler ─────────────────────────────────────────────────────

async fn handle_diagnosis_client(
    mut tcp:       TcpStream,
    peer:          std::net::SocketAddr,
    tcp_to_ws_tx:  mpsc::Sender<Vec<u8>>,
    ws_to_tcp_rx:  &mut mpsc::Receiver<Vec<u8>>,
    app:           &AppHandle,
    state:         &Arc<AppState>,
    shutdown:      &Arc<AtomicBool>,
) {
    let (mut tcp_rx, mut tcp_tx) = tcp.split();

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        // Read a DoIP header + payload from the diagnosis software
        let mut header = [0u8; HEADER_LEN];

        tokio::select! {
            // ── TCP → WS ─────────────────────────────────────────────────
            read_result = tcp_rx.read_exact(&mut header) => {
                if let Err(e) = read_result {
                    state.log(app, LogEntry::warn(
                        format!("TCP read [{peer}]: {e}")
                    )).await;
                    break;
                }

                let payload_len = u32::from_be_bytes([
                    header[4], header[5], header[6], header[7],
                ]) as usize;

                if payload_len > MAX_PAYLOAD {
                    state.log(app, LogEntry::error(
                        format!("DoIP payload too large: {payload_len} bytes — closing")
                    )).await;
                    break;
                }

                let mut full_frame = Vec::with_capacity(HEADER_LEN + payload_len);
                full_frame.extend_from_slice(&header);

                if payload_len > 0 {
                    full_frame.resize(HEADER_LEN + payload_len, 0);
                    if let Err(e) = tcp_rx.read_exact(&mut full_frame[HEADER_LEN..]).await {
                        state.log(app, LogEntry::warn(
                            format!("TCP payload read [{peer}]: {e}")
                        )).await;
                        break;
                    }
                }

                let pt = u16::from_be_bytes([header[2], header[3]]);
                state.log(app, LogEntry::doip(
                    format!("→ Win→Mac [TCP→WS] type=0x{pt:04X} {} bytes", full_frame.len())
                )).await;

                if tcp_to_ws_tx.send(full_frame).await.is_err() {
                    state.log(app, LogEntry::error("WS channel closed")).await;
                    break;
                }
            }

            // ── WS → TCP ─────────────────────────────────────────────────
            ws_data = ws_to_tcp_rx.recv() => {
                match ws_data {
                    Some(data) => {
                        // Data from Mac is raw UDS payload — wrap in DoIP diagnostic frame
                        let doip_frame = wrap_doip_diagnostic(&data);
                        state.log(app, LogEntry::doip(
                            format!("← Mac→Win [WS→TCP] {} bytes UDS", data.len())
                        )).await;
                        if tcp_tx.write_all(&doip_frame).await.is_err() {
                            state.log(app, LogEntry::warn("TCP write error")).await;
                            break;
                        }
                    }
                    None => {
                        state.log(app, LogEntry::error("WS→TCP channel closed")).await;
                        break;
                    }
                }
            }
        }
    }
}

// ── Wrap raw UDS bytes in a minimal DoIP Diagnostic Message (0x4001) ───────
// src=0x0001 (ECU), tgt=0x0E00 (tester) — mirrors what the ECU would send
fn wrap_doip_diagnostic(uds_payload: &[u8]) -> Vec<u8> {
    const PT_DIAG_MSG: u16 = 0x4001;
    let payload_len = 4 + uds_payload.len(); // src(2) + tgt(2) + uds
    let mut frame = Vec::with_capacity(8 + payload_len);
    // Header
    frame.push(0x02); // DoIP version
    frame.push(0xFD); // inverse
    frame.extend_from_slice(&PT_DIAG_MSG.to_be_bytes());
    frame.extend_from_slice(&(payload_len as u32).to_be_bytes());
    // Payload: ECU→tester addresses
    frame.extend_from_slice(&0x0001u16.to_be_bytes()); // src = ECU
    frame.extend_from_slice(&0x0E00u16.to_be_bytes()); // tgt = tester
    frame.extend_from_slice(uds_payload);
    frame
}
