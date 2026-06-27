//! DoIP proxy core — TCP forwarding + UDP Vehicle Discovery.
//!
//! Flusso completo:
//!   ISTA → UDP 13400  Vehicle Identification Request  → rispondiamo localmente
//!   ISTA → TCP 13400  Routing Activation Request      → rispondiamo localmente (0x0006)
//!   ISTA → TCP 13400  Diagnostic Message (0x4001)     → estrai UDS → Mac via WS
//!   Mac  → WS binary  UDS payload                     → wrappa in DoIP → ISTA via TCP

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{timeout, Duration};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tauri::AppHandle;

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

// Indirizzo logico del gateway simulato (ciò che ISTA vede come ECU gateway)
const GATEWAY_LOGICAL_ADDR: u16 = 0x0001;
// Indirizzo logico del tester ISTA (standard BMW)
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

    let ws_url   = format!("ws://{}:{}", config.mac_ip, config.mac_ws_port);
    let bind_ip  = config.local_bind_ip.trim().to_string();
    let udp_addr = format!("{}:13400", bind_ip);
    let tcp_addr = format!("{}:13400", bind_ip);
    let vin      = config.vin.trim().to_string();

    // Valida VIN (solo ASCII stampabile)
    for b in vin.as_bytes() {
        if *b < 0x20 || *b > 0x7E {
            return Err(format!("VIN contiene caratteri non validi: 0x{b:02X}"));
        }
    }

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

    let udp2   = udp_sock.clone();
    let app_u  = app.clone();
    let st_u   = state.clone();
    let vin2   = vin.clone();
    let ip2    = bind_ip.clone();
    tokio::spawn(async move {
        udp_discovery_loop(udp2, &vin2, &ip2, &app_u, &st_u).await;
    });

    // ── TCP listener ──────────────────────────────────────────────────────
    let listener = TcpListener::bind(&tcp_addr).await
        .map_err(|e| format!("TCP bind {tcp_addr}: {e}"))?;

    state.log(&app, LogEntry::info(format!(
        "TCP DoIP in ascolto su {tcp_addr} — in attesa di ISTA"
    ))).await;

    // Canale: handler TCP→WS (UDS payload grezzi verso il Mac)
    let (tcp_to_ws_tx, mut tcp_to_ws_rx) = mpsc::channel::<Vec<u8>>(128);

    // Broadcast: Mac→WS→TCP (UDS grezzi verso tutti i client ISTA connessi)
    let (ws_to_tcp_bcast, _dummy_rx) = broadcast::channel::<Vec<u8>>(128);
    // Manteniamo un receiver "dummy" per evitare che SendError quando nessun
    // client è connesso dropi silenziosamente i frame
    let ws_to_tcp_bcast2 = ws_to_tcp_bcast.clone();
    tokio::spawn(async move {
        let mut dummy = ws_to_tcp_bcast2.subscribe();
        while dummy.recv().await.is_ok() { /* mantiene vivo il sender */ }
    });

    let shutdown = Arc::new(AtomicBool::new(false));

    // WS receive → broadcast a tutti i client TCP
    let sd_ws   = shutdown.clone();
    let bcast3  = ws_to_tcp_bcast.clone();
    let app_w   = app.clone();
    let st_w    = state.clone();
    tokio::spawn(async move {
        while let Some(msg) = ws_rx.next().await {
            if sd_ws.load(Ordering::Relaxed) { break; }
            match msg {
                Ok(Message::Binary(data)) => {
                    st_w.log(&app_w, LogEntry::doip(
                        format!("← Mac→Win [WS] {} bytes UDS", data.len())
                    )).await;
                    // Invia a tutti i client ISTA connessi (broadcast)
                    if bcast3.send(data.to_vec()).is_err() {
                        // Nessun subscriber attivo — loghiamo solo in debug
                        st_w.log(&app_w, LogEntry::debug(
                            "← WS frame ricevuto ma nessun client ISTA connesso"
                        )).await;
                    }
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

    // tcp_to_ws channel → WS send (dati da ISTA verso Mac)
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

                        // Ogni client ottiene il proprio receiver broadcast
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
    sock:    Arc<UdpSocket>,
    vin:     &str,
    bind_ip: &str,
    app:     &AppHandle,
    state:   &Arc<AppState>,
) {
    let mut buf = [0u8; 256];

    // Invia Vehicle Announcement iniziale
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
                state.log(app, LogEntry::doip(
                    format!("→ Vehicle Identification Response → {from}")
                )).await;
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
            // ── TCP (ISTA) → process o forward WS ───────────────────────
            r = tcp_rx.read_exact(&mut header) => {
                if r.is_err() { break; }

                // Valida versione DoIP
                if header[0] != DOIP_VERSION || header[1] != DOIP_INV_VERSION {
                    state.log(app, LogEntry::warn(
                        format!("Header DoIP non valido: ver={:02X}/{:02X} — ignoro",
                            header[0], header[1])
                    )).await;
                    continue;
                }

                let pt          = u16::from_be_bytes([header[2], header[3]]);
                let payload_len = u32::from_be_bytes([
                    header[4], header[5], header[6], header[7],
                ]) as usize;

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
                    // ── Routing Activation Request → rispondi localmente ──
                    PT_ROUTING_ACT_REQ => {
                        let tester_addr = if payload.len() >= 2 {
                            u16::from_be_bytes([payload[0], payload[1]])
                        } else {
                            TESTER_LOGICAL_ADDR
                        };
                        state.log(app, LogEntry::doip(
                            format!("← Routing Activation Request  [tester=0x{tester_addr:04X}]")
                        )).await;

                        let resp = build_routing_activation_response(tester_addr);
                        if tcp_tx.write_all(&resp).await.is_err() { break; }

                        state.log(app, LogEntry::doip(
                            "→ Routing Activation Response: attivato (0x10)"
                        )).await;
                    }

                    // ── Diagnostic Message → estrai UDS e manda al Mac ───
                    PT_DIAG_MSG if payload.len() >= 4 => {
                        let src = u16::from_be_bytes([payload[0], payload[1]]);
                        let tgt = u16::from_be_bytes([payload[2], payload[3]]);
                        let uds = payload[4..].to_vec();

                        state.log(app, LogEntry::doip(
                            format!("→ ISTA→Mac  [src=0x{src:04X} tgt=0x{tgt:04X}] {} bytes UDS",
                                uds.len())
                        )).await;

                        // Manda solo il payload UDS al Mac (non il frame DoIP intero)
                        if tcp_to_ws_tx.send(uds).await.is_err() { break; }
                    }

                    // ── Alive Check Request → rispondi localmente ─────────
                    0x0007 => {
                        let resp = build_doip_frame(0x0008, &[]);
                        let _ = tcp_tx.write_all(&resp).await;
                    }

                    pt => {
                        state.log(app, LogEntry::debug(
                            format!("← Frame DoIP tipo 0x{pt:04X} [{payload_len} bytes] — ignoro")
                        )).await;
                    }
                }
            }

            // ── WS (Mac) → wrappa in DoIP → TCP (ISTA) ───────────────────
            bcast_msg = ws_bcast.recv() => {
                match bcast_msg {
                    Ok(uds) => {
                        state.log(app, LogEntry::doip(
                            format!("← Mac→ISTA  {} bytes UDS", uds.len())
                        )).await;
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

    let _ = peer;
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

/// Vehicle Identification Response (ISO 13400-2:2019 Table 10)
/// Payload: VIN(17) + LogAddr(2) + EID(6) + GID(6) + FurtherAction(1) = 33 bytes
fn build_vehicle_id_response(vin: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(33);

    let mut vin_bytes = [0u8; 17];
    let src = vin.as_bytes();
    vin_bytes[..src.len().min(17)].copy_from_slice(&src[..src.len().min(17)]);
    payload.extend_from_slice(&vin_bytes);                        // VIN       17 bytes
    payload.extend_from_slice(&GATEWAY_LOGICAL_ADDR.to_be_bytes()); // LogAddr    2 bytes
    payload.extend_from_slice(&[0xAB, 0xB0, 0x1D, 0x6E, 0x00, 0x01]); // EID   6 bytes
    payload.extend_from_slice(&[0u8; 6]);                         // GID        6 bytes
    payload.push(0x00);                                           // FurtherAction 1 byte

    build_doip_frame(PT_VEHICLE_ID_RES, &payload)
}

/// Routing Activation Response (ISO 13400-2:2019 Table 20)
/// Payload: TesterAddr(2) + GatewayAddr(2) + ResponseCode(1) + Reserved(4) = 9 bytes
fn build_routing_activation_response(tester_addr: u16) -> Vec<u8> {
    let mut payload = Vec::with_capacity(9);
    payload.extend_from_slice(&tester_addr.to_be_bytes());            // Tester addr
    payload.extend_from_slice(&GATEWAY_LOGICAL_ADDR.to_be_bytes());   // Gateway addr
    payload.push(0x10);                                               // 0x10 = routing activated OK
    payload.extend_from_slice(&[0u8; 4]);                             // Reserved
    build_doip_frame(PT_ROUTING_ACT_RES, &payload)
}

/// Wrappa UDS payload in un DoIP Diagnostic Message per ISTA
/// src = gateway ECU (0x0001), tgt = tester ISTA (0x0E00)
fn wrap_doip_diagnostic(uds: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + uds.len());
    payload.extend_from_slice(&GATEWAY_LOGICAL_ADDR.to_be_bytes()); // src = ECU/gateway
    payload.extend_from_slice(&TESTER_LOGICAL_ADDR.to_be_bytes());  // tgt = ISTA tester
    payload.extend_from_slice(uds);
    build_doip_frame(PT_DIAG_MSG, &payload)
}

/// Calcola indirizzo broadcast usando le interfacce di sistema (subnet corretta)
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
    // Fallback: usa /24 broadcast
    let parts: Vec<&str> = bind_ip.split('.').collect();
    if parts.len() == 4 {
        return Ok(format!("{}.{}.{}.255:13400", parts[0], parts[1], parts[2]));
    }
    Err(())
}
