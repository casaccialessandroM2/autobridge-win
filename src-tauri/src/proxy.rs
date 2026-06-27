//! DoIP proxy core — TCP forwarding + UDP Vehicle Discovery + Relay connection.
//!
//! Flusso completo:
//!   Win → Relay WS  join(session_id)       → Mac viene notificato
//!   ISTA → UDP 13400  Vehicle ID Request   → rispondiamo localmente
//!   ISTA → TCP 13400  Routing Activation   → rispondiamo localmente (0x0006)
//!   ISTA → TCP 13400  Diagnostic Msg       → UDS → base64 JSON → Relay → Mac
//!   Mac  → Relay      data(base64 UDS)     → unwrap → DoIP → ISTA via TCP

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{timeout, Duration, interval};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tauri::AppHandle;
use serde_json::{json, Value};

use crate::state::{AppState, LogEntry, ProxyCommand};

const DOIP_VERSION:     u8    = 0x02;
const DOIP_INV_VERSION: u8    = 0xFD;
const HEADER_LEN:       usize = 8;
const MAX_PAYLOAD:      usize = 1 << 20;

const PT_VEHICLE_ID_REQ:  u16 = 0x0001;
const PT_VEHICLE_ID_RES:  u16 = 0x0004;
const PT_ROUTING_ACT_REQ: u16 = 0x0005;
const PT_ROUTING_ACT_RES: u16 = 0x0006;
const PT_DIAG_MSG:        u16 = 0x4001;

const GATEWAY_LOGICAL_ADDR: u16 = 0x0001;
const TESTER_LOGICAL_ADDR:  u16 = 0x0E00;

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
    if config.session_id.trim().is_empty() {
        return Err("Inserisci il codice sessione mostrato da AutoBridge Mac".to_string());
    }

    let relay_url  = config.relay_url.trim().to_string();
    let session_id = config.session_id.trim().to_uppercase();
    let bind_ip    = config.local_bind_ip.trim().to_string();
    let vin        = config.vin.trim().to_string();
    let udp_addr   = format!("{}:13400", bind_ip);
    let tcp_addr   = format!("{}:13400", bind_ip);

    // Genera device_id da timestamp
    let device_id = format!("win-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis());

    // ── Connetti al relay ──────────────────────────────────────────────────
    state.log(&app, LogEntry::info(format!(
        "Connessione al relay: {relay_url}"
    ))).await;

    let (ws_stream, _) = timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(&relay_url),
    )
    .await
    .map_err(|_| format!("Timeout connessione relay {relay_url}"))?
    .map_err(|e| format!("WebSocket relay fallito: {e}"))?;

    state.log(&app, LogEntry::info("Relay connesso — invio join...")).await;

    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    // Invia join message
    let join_msg = json!({
        "type": "join",
        "platform": "windows",
        "session_id": session_id,
        "device_id": device_id,
    });
    ws_tx.send(Message::Text(join_msg.to_string().into())).await
        .map_err(|e| format!("Invio join fallito: {e}"))?;

    // Aspetta conferma session_joined
    let joined = timeout(Duration::from_secs(15), async {
        while let Some(msg) = ws_rx.next().await {
            if let Ok(Message::Text(text)) = msg {
                if let Ok(val) = serde_json::from_str::<Value>(&text) {
                    match val.get("type").and_then(|t| t.as_str()) {
                        Some("session_joined") => return Ok(()),
                        Some("error") => {
                            let reason = val.get("reason")
                                .and_then(|r| r.as_str())
                                .unwrap_or("sconosciuto");
                            return Err(format!("Relay: {reason}"));
                        }
                        _ => {}
                    }
                }
            }
        }
        Err("Relay chiuso prima di session_joined".to_string())
    }).await
    .map_err(|_| "Timeout attesa sessione relay (15s)".to_string())?;

    joined?;

    state.log(&app, LogEntry::info(format!(
        "Sessione relay attiva [{}]", session_id
    ))).await;
    state.set_status(&app, "Connected").await;

    // ── UDP discovery listener ─────────────────────────────────────────────
    let udp_sock = Arc::new(
        UdpSocket::bind(&udp_addr).await
            .map_err(|e| format!("UDP bind {udp_addr}: {e}"))?
    );
    state.log(&app, LogEntry::info(format!(
        "UDP discovery in ascolto su {udp_addr}"
    ))).await;

    let udp2  = udp_sock.clone();
    let app_u = app.clone();
    let st_u  = state.clone();
    let vin2  = vin.clone();
    let ip2   = bind_ip.clone();
    tokio::spawn(async move {
        udp_discovery_loop(udp2, &vin2, &ip2, &app_u, &st_u).await;
    });

    // ── TCP listener (ISTA) ────────────────────────────────────────────────
    let listener = TcpListener::bind(&tcp_addr).await
        .map_err(|e| format!("TCP bind {tcp_addr}: {e}"))?;

    state.log(&app, LogEntry::info(format!(
        "TCP DoIP in ascolto su {tcp_addr} — in attesa di ISTA"
    ))).await;

    // ISTA→Mac: UDS grezzi (da TCP verso relay)
    let (tcp_to_ws_tx, mut tcp_to_ws_rx) = mpsc::channel::<Vec<u8>>(128);
    // Mac→ISTA: UDS grezzi (da relay verso tutti i client TCP)
    let (ws_to_tcp_bcast, _dummy_rx) = broadcast::channel::<Vec<u8>>(128);

    let ws_to_tcp_bcast2 = ws_to_tcp_bcast.clone();
    tokio::spawn(async move {
        let mut dummy = ws_to_tcp_bcast2.subscribe();
        while dummy.recv().await.is_ok() {}
    });

    let shutdown = Arc::new(AtomicBool::new(false));

    // ── WS relay RX: Mac → Win ────────────────────────────────────────────
    let sd_ws   = shutdown.clone();
    let bcast3  = ws_to_tcp_bcast.clone();
    let app_w   = app.clone();
    let st_w    = state.clone();
    let _sid_rx = session_id.clone();
    tokio::spawn(async move {
        while let Some(msg) = ws_rx.next().await {
            if sd_ws.load(Ordering::Relaxed) { break; }
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(val) = serde_json::from_str::<Value>(&text) {
                        match val.get("type").and_then(|t| t.as_str()) {
                            Some("data") => {
                                // Decodifica base64 payload
                                if let Some(b64) = val.get("payload").and_then(|p| p.as_str()) {
                                    use base64::Engine;
                                    match base64::engine::general_purpose::STANDARD.decode(b64) {
                                        Ok(uds) => {
                                            st_w.log(&app_w, LogEntry::doip(
                                                format!("← Relay→Win {} bytes UDS", uds.len())
                                            )).await;
                                            if bcast3.send(uds).is_err() {
                                                st_w.log(&app_w, LogEntry::debug(
                                                    "Nessun client ISTA connesso per ricevere il frame"
                                                )).await;
                                            }
                                        }
                                        Err(e) => {
                                            st_w.log(&app_w, LogEntry::warn(
                                                format!("Base64 decode fallito: {e}")
                                            )).await;
                                        }
                                    }
                                }
                            }
                            Some("heartbeat_ack") => {}
                            Some("peer_disconnected") => {
                                st_w.log(&app_w, LogEntry::warn(
                                    "AutoBridge Mac disconnesso dal relay"
                                )).await;
                                sd_ws.store(true, Ordering::Relaxed);
                                break;
                            }
                            Some(other) => {
                                st_w.log(&app_w, LogEntry::debug(
                                    format!("Relay msg tipo '{other}' — ignorato")
                                )).await;
                            }
                            None => {}
                        }
                    }
                }
                Ok(Message::Close(_)) | Err(_) => {
                    st_w.log(&app_w, LogEntry::warn("Connessione relay chiusa")).await;
                    sd_ws.store(true, Ordering::Relaxed);
                    break;
                }
                _ => {}
            }
        }
    });

    // ── WS relay TX: ISTA→Mac + heartbeat ────────────────────────────────
    let sd_tx    = shutdown.clone();
    let sid_tx   = session_id.clone();
    let app_t    = app.clone();
    let st_t     = state.clone();
    tokio::spawn(async move {
        let mut hb_interval = interval(Duration::from_secs(2));
        loop {
            tokio::select! {
                data = tcp_to_ws_rx.recv() => {
                    match data {
                        Some(uds) => {
                            if sd_tx.load(Ordering::Relaxed) { break; }
                            use base64::Engine;
                            let b64 = base64::engine::general_purpose::STANDARD.encode(&uds);
                            let msg = json!({
                                "type": "data",
                                "session_id": sid_tx,
                                "payload": b64,
                            });
                            if ws_tx.send(Message::Text(msg.to_string().into())).await.is_err() {
                                sd_tx.store(true, Ordering::Relaxed);
                                break;
                            }
                        }
                        None => break,
                    }
                }
                _ = hb_interval.tick() => {
                    if sd_tx.load(Ordering::Relaxed) { break; }
                    let hb = json!({"type":"heartbeat","session_id": sid_tx});
                    if ws_tx.send(Message::Text(hb.to_string().into())).await.is_err() {
                        sd_tx.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }
        let _ = (app_t, st_t); // keep alive
    });

    // ── Accept loop TCP (ISTA) ─────────────────────────────────────────────
    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((tcp, peer)) => {
                        state.log(&app, LogEntry::info(
                            format!("ISTA connesso da {peer}")
                        )).await;

                        let ws_rx_client = ws_to_tcp_bcast.subscribe();
                        handle_ista_client(
                            tcp, peer,
                            tcp_to_ws_tx.clone(),
                            ws_rx_client,
                            &app, &state, &shutdown,
                        ).await;

                        state.log(&app, LogEntry::info(
                            format!("ISTA disconnesso: {peer}")
                        )).await;
                    }
                    Err(e) => {
                        state.log(&app, LogEntry::warn(format!("TCP accept: {e}"))).await;
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
    sock:    Arc<UdpSocket>,
    vin:     &str,
    bind_ip: &str,
    app:     &AppHandle,
    state:   &Arc<AppState>,
) {
    let mut buf = [0u8; 256];

    if let Ok(bcast) = broadcast_addr_for(bind_ip) {
        let ann = build_vehicle_id_response(vin);
        let _ = sock.send_to(&ann, &bcast).await;
        state.log(app, LogEntry::doip(
            format!("→ Vehicle Announcement → {bcast}")
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
                let resp = build_vehicle_id_response(vin);
                let _ = sock.send_to(&resp, from).await;
            }
            _ => {}
        }
    }
}

// ── Per-client ISTA handler ────────────────────────────────────────────────

async fn handle_ista_client(
    mut tcp:      tokio::net::TcpStream,
    peer:         SocketAddr,
    tcp_to_ws_tx: mpsc::Sender<Vec<u8>>,
    mut ws_bcast: broadcast::Receiver<Vec<u8>>,
    app:          &AppHandle,
    state:        &Arc<AppState>,
    shutdown:     &Arc<AtomicBool>,
) {
    let (mut tcp_rx, mut tcp_tx) = tcp.split();

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        let mut header = [0u8; HEADER_LEN];

        tokio::select! {
            r = tcp_rx.read_exact(&mut header) => {
                if r.is_err() { break; }

                if header[0] != DOIP_VERSION || header[1] != DOIP_INV_VERSION {
                    state.log(app, LogEntry::warn(
                        format!("Header DoIP non valido: {:#04X}/{:#04X} — ignoro",
                            header[0], header[1])
                    )).await;
                    continue;
                }

                let pt          = u16::from_be_bytes([header[2], header[3]]);
                let payload_len = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;

                if payload_len > MAX_PAYLOAD {
                    state.log(app, LogEntry::error(
                        format!("Payload DoIP troppo grande: {payload_len} bytes — chiudo")
                    )).await;
                    break;
                }

                let mut payload = vec![0u8; payload_len];
                if payload_len > 0 {
                    if tcp_rx.read_exact(&mut payload).await.is_err() { break; }
                }

                match pt {
                    PT_ROUTING_ACT_REQ => {
                        let tester_addr = if payload.len() >= 2 {
                            u16::from_be_bytes([payload[0], payload[1]])
                        } else {
                            TESTER_LOGICAL_ADDR
                        };
                        state.log(app, LogEntry::doip(
                            format!("← Routing Activation Request [tester=0x{tester_addr:04X}]")
                        )).await;
                        let resp = build_routing_activation_response(tester_addr);
                        if tcp_tx.write_all(&resp).await.is_err() { break; }
                        state.log(app, LogEntry::doip("→ Routing Activation Response: OK")).await;
                    }

                    PT_DIAG_MSG if payload.len() >= 4 => {
                        let src = u16::from_be_bytes([payload[0], payload[1]]);
                        let tgt = u16::from_be_bytes([payload[2], payload[3]]);
                        let uds = payload[4..].to_vec();
                        state.log(app, LogEntry::doip(
                            format!("→ ISTA→Mac [src=0x{src:04X} tgt=0x{tgt:04X}] {} bytes UDS", uds.len())
                        )).await;
                        if tcp_to_ws_tx.send(uds).await.is_err() { break; }
                    }

                    0x0007 => {
                        let resp = build_doip_frame(0x0008, &[]);
                        let _ = tcp_tx.write_all(&resp).await;
                    }

                    pt => {
                        state.log(app, LogEntry::debug(
                            format!("← Frame DoIP 0x{pt:04X} [{payload_len}B] — ignorato")
                        )).await;
                    }
                }
            }

            bcast_msg = ws_bcast.recv() => {
                match bcast_msg {
                    Ok(uds) => {
                        let frame = wrap_doip_diagnostic(&uds);
                        if tcp_tx.write_all(&frame).await.is_err() { break; }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        state.log(app, LogEntry::warn(
                            format!("ISTA [{peer}] lento: {n} frame persi")
                        )).await;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

// ── DoIP frame builders ────────────────────────────────────────────────────

fn build_doip_frame(pt: u16, payload: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(HEADER_LEN + payload.len());
    f.push(DOIP_VERSION);
    f.push(DOIP_INV_VERSION);
    f.extend_from_slice(&pt.to_be_bytes());
    f.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    f.extend_from_slice(payload);
    f
}

fn build_vehicle_id_response(vin: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(33);
    let mut vin_bytes = [0u8; 17];
    let src = vin.as_bytes();
    vin_bytes[..src.len().min(17)].copy_from_slice(&src[..src.len().min(17)]);
    payload.extend_from_slice(&vin_bytes);
    payload.extend_from_slice(&GATEWAY_LOGICAL_ADDR.to_be_bytes());
    payload.extend_from_slice(&[0xAB, 0xB0, 0x1D, 0x6E, 0x00, 0x01]);
    payload.extend_from_slice(&[0u8; 6]);
    payload.push(0x00);
    build_doip_frame(PT_VEHICLE_ID_RES, &payload)
}

fn build_routing_activation_response(tester_addr: u16) -> Vec<u8> {
    let mut payload = Vec::with_capacity(9);
    payload.extend_from_slice(&tester_addr.to_be_bytes());
    payload.extend_from_slice(&GATEWAY_LOGICAL_ADDR.to_be_bytes());
    payload.push(0x10);
    payload.extend_from_slice(&[0u8; 4]);
    build_doip_frame(PT_ROUTING_ACT_RES, &payload)
}

fn wrap_doip_diagnostic(uds: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + uds.len());
    payload.extend_from_slice(&GATEWAY_LOGICAL_ADDR.to_be_bytes());
    payload.extend_from_slice(&TESTER_LOGICAL_ADDR.to_be_bytes());
    payload.extend_from_slice(uds);
    build_doip_frame(PT_DIAG_MSG, &payload)
}

fn broadcast_addr_for(bind_ip: &str) -> Result<String, ()> {
    if let Ok(ifaces) = if_addrs::get_if_addrs() {
        for iface in ifaces {
            if let if_addrs::IfAddr::V4(v4) = &iface.addr {
                if v4.ip.to_string() == bind_ip {
                    if let Some(bcast) = v4.broadcast {
                        return Ok(format!("{bcast}:13400"));
                    }
                }
            }
        }
    }
    let parts: Vec<&str> = bind_ip.split('.').collect();
    if parts.len() == 4 {
        return Ok(format!("{}.{}.{}.255:13400", parts[0], parts[1], parts[2]));
    }
    Err(())
}
