//! DoIP proxy core — TCP forwarding + UDP Vehicle Discovery.
//!
//! Implements the subset of ISO 13400-2 needed by ISTA (BMW):
//!
//!   UDP port 13400:
//!     ← Vehicle Identification Request  (0x0001)
//!     → Vehicle Identification Response (0x0004)  ← we fake this
//!
//!   TCP port 13400:
//!     ← Routing Activation Request   (0x0005)
//!     → Routing Activation Response  (0x0006)     ← forwarded to Mac WS
//!     ↕  Diagnostic messages         (0x4001…)    ← forwarded both ways

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tauri::AppHandle;

use crate::state::{AppState, LogEntry, ProxyCommand};

const DOIP_VERSION:     u8    = 0x02;
const DOIP_INV_VERSION: u8    = 0xFD;
const HEADER_LEN:       usize = 8;
const MAX_PAYLOAD:      usize = 1 << 20;

// DoIP payload types
const PT_VEHICLE_ID_REQ:  u16 = 0x0001;
const PT_VEHICLE_ID_RES:  u16 = 0x0004;
const PT_VEHICLE_ANNOUNCE: u16 = 0x0100;

// ── Public entry point ─────────────────────────────────────────────────────

pub async fn run_proxy(
    app:        AppHandle,
    state:      Arc<AppState>,
    mut cmd_rx: mpsc::Receiver<ProxyCommand>,
) -> Result<(), String> {

    let config = state.config.lock().await.clone();

    if config.local_bind_ip.trim().is_empty() {
        return Err("Seleziona un adattatore di rete".to_string());
    }

    let ws_url   = format!("ws://{}:{}", config.mac_ip, config.mac_ws_port);
    let bind_ip  = config.local_bind_ip.trim().to_string();
    let udp_addr = format!("{}:13400", bind_ip);
    let tcp_addr = format!("{}:13400", bind_ip);

    // ── Connect WebSocket to Mac ───────────────────────────────────────────
    state.log(&app, LogEntry::info(format!(
        "Connessione a AutoBridge Mac: {ws_url}"
    ))).await;

    let (ws_stream, _) = timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(&ws_url),
    )
    .await
    .map_err(|_| format!("Timeout connessione a {ws_url}"))?
    .map_err(|e| format!("WebSocket fallito: {e}"))?;

    state.log(&app, LogEntry::info("WebSocket connesso al Mac")).await;
    state.set_status(&app, "Connected").await;

    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    // ── UDP discovery listener ─────────────────────────────────────────────
    let udp_sock = Arc::new(
        UdpSocket::bind(&udp_addr).await
            .map_err(|e| format!("UDP bind {udp_addr}: {e}"))?
    );
    state.log(&app, LogEntry::info(format!(
        "UDP discovery in ascolto su {udp_addr}"
    ))).await;

    let vin_str = config.vin.trim().to_string();
    let udp_sock2 = udp_sock.clone();
    let app_udp  = app.clone();
    let st_udp   = state.clone();
    let bind_ip2 = bind_ip.clone();

    tokio::spawn(async move {
        udp_discovery_loop(udp_sock2, &vin_str, &bind_ip2, &app_udp, &st_udp).await;
    });

    // ── TCP listener for ISTA ──────────────────────────────────────────────
    let listener = TcpListener::bind(&tcp_addr).await
        .map_err(|e| format!("TCP bind {tcp_addr}: {e}"))?;

    state.log(&app, LogEntry::info(format!(
        "TCP DoIP in ascolto su {tcp_addr} — in attesa di ISTA"
    ))).await;

    // Channels between TCP↔WS
    let (tcp_to_ws_tx, mut tcp_to_ws_rx) = mpsc::channel::<Vec<u8>>(128);
    let (ws_to_tcp_tx, mut ws_to_tcp_rx) = mpsc::channel::<Vec<u8>>(128);

    let shutdown = Arc::new(AtomicBool::new(false));

    // WS receive → tcp_write channel
    let sd_ws = shutdown.clone();
    let wt2   = ws_to_tcp_tx.clone();
    let app_w = app.clone();
    let st_w  = state.clone();
    tokio::spawn(async move {
        while let Some(msg) = ws_rx.next().await {
            if sd_ws.load(Ordering::Relaxed) { break; }
            match msg {
                Ok(Message::Binary(data)) => {
                    st_w.log(&app_w, LogEntry::doip(
                        format!("← Mac→Win [WS] {} bytes", data.len())
                    )).await;
                    if wt2.send(data.to_vec()).await.is_err() { break; }
                }
                Ok(Message::Close(_)) | Err(_) => {
                    st_w.log(&app_w, LogEntry::warn("WebSocket chiuso dal Mac")).await;
                    sd_ws.store(true, Ordering::Relaxed);
                    break;
                }
                _ => {}
            }
        }
    });

    // tcp_to_ws channel → WS send
    let sd_tx = shutdown.clone();
    tokio::spawn(async move {
        while let Some(data) = tcp_to_ws_rx.recv().await {
            if sd_tx.load(Ordering::Relaxed) { break; }
            if ws_tx.send(Message::Binary(data.into())).await.is_err() {
                sd_tx.store(true, Ordering::Relaxed);
                break;
            }
        }
    });

    // ── Accept loop ────────────────────────────────────────────────────────
    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((tcp, peer)) => {
                        state.log(&app, LogEntry::info(
                            format!("ISTA connesso da {peer}")
                        )).await;
                        handle_ista_client(
                            tcp, peer,
                            tcp_to_ws_tx.clone(),
                            &mut ws_to_tcp_rx,
                            &app, &state, &shutdown,
                        ).await;
                        state.log(&app, LogEntry::info(
                            format!("ISTA disconnesso: {peer}")
                        )).await;
                    }
                    Err(e) => {
                        state.log(&app, LogEntry::warn(
                            format!("TCP accept: {e}")
                        )).await;
                    }
                }
            }

            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(ProxyCommand::Disconnect) | None => {
                        state.log(&app, LogEntry::info("Disconnessione richiesta")).await;
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

// ── UDP discovery loop ─────────────────────────────────────────────────────

async fn udp_discovery_loop(
    sock:     Arc<UdpSocket>,
    vin:      &str,
    bind_ip:  &str,
    app:      &AppHandle,
    state:    &Arc<AppState>,
) {
    let mut buf = [0u8; 256];

    // Send an initial Vehicle Announcement so ISTA finds us immediately
    if let Ok(bcast_addr) = broadcast_addr_for(bind_ip) {
        let ann = build_vehicle_id_response(vin, bind_ip);
        let _ = sock.send_to(&ann, &bcast_addr).await;
        state.log(app, LogEntry::doip(
            format!("→ Vehicle Announcement broadcast → {bcast_addr}")
        )).await;
    }

    loop {
        let (len, from) = match sock.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(_) => break,
        };

        if len < HEADER_LEN { continue; }

        let pt = u16::from_be_bytes([buf[2], buf[3]]);

        match pt {
            PT_VEHICLE_ID_REQ | 0x0002 | 0x0003 => {
                state.log(app, LogEntry::doip(
                    format!("← Vehicle Identification Request da {from}")
                )).await;

                let resp = build_vehicle_id_response(vin, bind_ip);
                let _ = sock.send_to(&resp, from).await;

                state.log(app, LogEntry::doip(
                    format!("→ Vehicle Identification Response → {from}")
                )).await;
            }
            _ => {}
        }
    }
}

// ── Build Vehicle Identification Response (DoIP 0x0004) ───────────────────
//
// Payload layout (ISO 13400-2 Table 10):
//   VIN        17 bytes  (ASCII, padded with 0x00)
//   LogAddr     2 bytes  gateway logical address
//   EID         6 bytes  entity ID (we use a fixed fake MAC)
//   GID         6 bytes  group ID  (we use zeros)
//   FurtherAction 1 byte (0x00 = no further action)
fn build_vehicle_id_response(vin: &str, _bind_ip: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(33);

    // VIN — pad/truncate to 17 bytes
    let mut vin_bytes = [0u8; 17];
    let src = vin.as_bytes();
    let copy_len = src.len().min(17);
    vin_bytes[..copy_len].copy_from_slice(&src[..copy_len]);
    payload.extend_from_slice(&vin_bytes);

    // Logical address of gateway (same as target on Mac side)
    payload.extend_from_slice(&0x0010u16.to_be_bytes());

    // EID — fake MAC-like identifier
    payload.extend_from_slice(&[0xAB, 0xB0, 0x1D, 0x6E, 0x00, 0x01]);

    // GID — zeros
    payload.extend_from_slice(&[0u8; 6]);

    // Further action required
    payload.push(0x00);

    build_doip_frame(PT_VEHICLE_ID_RES, &payload)
}

fn build_doip_frame(pt: u16, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
    frame.push(DOIP_VERSION);
    frame.push(DOIP_INV_VERSION);
    frame.extend_from_slice(&pt.to_be_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

// Derive broadcast address from bind IP (e.g. 169.254.12.5 → 169.254.255.255)
fn broadcast_addr_for(ip: &str) -> Result<String, ()> {
    let parts: Vec<&str> = ip.split('.').collect();
    if parts.len() != 4 { return Err(()); }
    Ok(format!("{}.{}.255.255:13400", parts[0], parts[1]))
}

// ── Per-client ISTA handler ────────────────────────────────────────────────

async fn handle_ista_client(
    mut tcp:      tokio::net::TcpStream,
    peer:         SocketAddr,
    tcp_to_ws_tx: mpsc::Sender<Vec<u8>>,
    ws_to_tcp_rx: &mut mpsc::Receiver<Vec<u8>>,
    app:          &AppHandle,
    state:        &Arc<AppState>,
    shutdown:     &Arc<AtomicBool>,
) {
    let (mut tcp_rx, mut tcp_tx) = tcp.split();

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        let mut header = [0u8; HEADER_LEN];

        tokio::select! {
            // TCP → WS
            r = tcp_rx.read_exact(&mut header) => {
                if r.is_err() { break; }

                let payload_len = u32::from_be_bytes([
                    header[4], header[5], header[6], header[7],
                ]) as usize;

                if payload_len > MAX_PAYLOAD { break; }

                let mut frame = Vec::with_capacity(HEADER_LEN + payload_len);
                frame.extend_from_slice(&header);
                if payload_len > 0 {
                    frame.resize(HEADER_LEN + payload_len, 0);
                    if tcp_rx.read_exact(&mut frame[HEADER_LEN..]).await.is_err() { break; }
                }

                let pt = u16::from_be_bytes([header[2], header[3]]);
                state.log(app, LogEntry::doip(
                    format!("→ ISTA→Mac  type=0x{pt:04X}  {} bytes", frame.len())
                )).await;

                if tcp_to_ws_tx.send(frame).await.is_err() { break; }
            }

            // WS → TCP
            ws_data = ws_to_tcp_rx.recv() => {
                match ws_data {
                    Some(data) => {
                        // Data from Mac is raw UDS — wrap in DoIP Diagnostic Message
                        let frame = wrap_doip_diagnostic(&data);
                        state.log(app, LogEntry::doip(
                            format!("← Mac→ISTA  {} bytes UDS", data.len())
                        )).await;
                        if tcp_tx.write_all(&frame).await.is_err() { break; }
                    }
                    None => break,
                }
            }
        }
    }

    let _ = peer;
}

fn wrap_doip_diagnostic(uds: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + uds.len());
    payload.extend_from_slice(&0x0010u16.to_be_bytes()); // src = gateway
    payload.extend_from_slice(&0x0E00u16.to_be_bytes()); // tgt = tester
    payload.extend_from_slice(uds);
    build_doip_frame(0x4001, &payload)
}
