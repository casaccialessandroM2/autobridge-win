//! Proxy trasparente — tunnel ISTA ↔ relay ↔ BMW (via Mac).
//!
//! Nessuna terminazione di protocollo: ISTA e la BMW negoziano HSFZ/DoIP
//! end-to-end *attraverso* il tubo. Questo lato inoltra soltanto byte grezzi.
//!
//! Flusso:
//!   Win → Relay WS  join(session_id)            → Mac notificato
//!   ISTA → UDP (porta config)  discovery         → 0x01 + datagram → relay → Mac → BMW
//!   ISTA → TCP (porta config)  stream diagnostico → 0x00 + bytes    → relay → Mac → BMW
//!   Mac  → Relay   data(base64 [canale+bytes])   → instradato a ISTA (UDP o TCP)
//!
//! Contratto di trasporto (condiviso col Mac):
//!   payload = base64( [1 byte canale] + byte grezzi )
//!     0x00 = stream TCP   |   0x01 = datagramma UDP

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::time::{timeout, Duration, interval};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tauri::{AppHandle, Emitter};
use serde_json::{json, Value};

use crate::state::{AppState, LogEntry, ProxyCommand};

const CH_TCP: u8 = 0x00;
const CH_UDP: u8 = 0x01;

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
    let tcp_addr   = format!("{}:{}", bind_ip, config.tcp_port);
    // UDP discovery: bind su 0.0.0.0 per ricevere ANCHE i broadcast di ISTA
    // (un socket legato a un IP unicast specifico non li riceve su Windows).
    let udp_addr   = format!("0.0.0.0:{}", config.udp_port);

    let device_id = format!("win-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis());

    // ── Connessione al relay ───────────────────────────────────────────────
    state.log(&app, LogEntry::info(format!("Connessione al relay: {relay_url}"))).await;

    let (ws_stream, _) = timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(&relay_url),
    )
    .await
    .map_err(|_| format!("Timeout connessione relay {relay_url}"))?
    .map_err(|e| format!("WebSocket relay fallito: {e}"))?;

    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    // Join
    let join_msg = json!({
        "type": "join", "platform": "windows",
        "session_id": session_id, "device_id": device_id,
    });
    ws_tx.send(Message::Text(join_msg.to_string().into())).await
        .map_err(|e| format!("Invio join fallito: {e}"))?;

    // Attendi session_joined
    timeout(Duration::from_secs(15), async {
        while let Some(msg) = ws_rx.next().await {
            if let Ok(Message::Text(text)) = msg {
                if let Ok(val) = serde_json::from_str::<Value>(&text) {
                    match val.get("type").and_then(|t| t.as_str()) {
                        Some("session_joined") => {
                            // VIN già noto dal Mac: mostralo subito.
                            if let Some(v) = val.get("vin").and_then(|x| x.as_str()) {
                                if !v.is_empty() { let _ = app.emit("vin_detected", v); }
                            }
                            return Ok(());
                        }
                        Some("error") => {
                            let reason = val.get("reason").and_then(|r| r.as_str()).unwrap_or("sconosciuto");
                            return Err(format!("Relay: {reason}"));
                        }
                        _ => {}
                    }
                }
            }
        }
        Err("Relay chiuso prima di session_joined".to_string())
    }).await
    .map_err(|_| "Timeout attesa sessione relay (15s)".to_string())??;

    state.log(&app, LogEntry::info(format!("Sessione relay attiva [{session_id}]"))).await;
    state.set_status(&app, "Connected").await;

    let shutdown = Arc::new(AtomicBool::new(false));

    // Byte già incorniciati (canale + dati) diretti al relay.
    let (to_relay_tx, mut to_relay_rx) = mpsc::channel::<Vec<u8>>(256);
    // Byte 0x00 dal Mac → tutti i client ISTA TCP.
    let (tcp_bcast, _dummy) = broadcast::channel::<Vec<u8>>(256);
    {
        let mut d = tcp_bcast.subscribe();
        tokio::spawn(async move { while d.recv().await.is_ok() {} });
    }

    // ── Socket UDP discovery ───────────────────────────────────────────────
    let udp_sock = Arc::new(
        UdpSocket::bind(&udp_addr).await.map_err(|e| format!("UDP bind {udp_addr}: {e}"))?
    );
    let _ = udp_sock.set_broadcast(true);
    let last_ista_udp: Arc<Mutex<Option<SocketAddr>>> = Arc::new(Mutex::new(None));
    state.log(&app, LogEntry::info(format!("UDP discovery in ascolto su {udp_addr}"))).await;

    // Task: ISTA UDP → relay (0x01)
    {
        let sock     = udp_sock.clone();
        let last     = last_ista_udp.clone();
        let to_relay = to_relay_tx.clone();
        let sd       = shutdown.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                if sd.load(Ordering::Relaxed) { break; }
                match sock.recv_from(&mut buf).await {
                    Ok((n, from)) if n > 0 => {
                        *last.lock().await = Some(from);
                        let mut framed = Vec::with_capacity(n + 1);
                        framed.push(CH_UDP);
                        framed.extend_from_slice(&buf[..n]);
                        if to_relay.send(framed).await.is_err() { break; }
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });
    }

    // ── Task: lettore relay → ISTA (TCP bcast / UDP) ───────────────────────
    {
        let sd       = shutdown.clone();
        let bcast    = tcp_bcast.clone();
        let sock     = udp_sock.clone();
        let last     = last_ista_udp.clone();
        let app_w    = app.clone();
        let st_w     = state.clone();
        tokio::spawn(async move {
            while let Some(msg) = ws_rx.next().await {
                if sd.load(Ordering::Relaxed) { break; }
                let text = match msg {
                    // HOT PATH: frame binario = dato (canale + payload) → a ISTA.
                    Ok(Message::Binary(buf)) => {
                        if let Some((&ch, rest)) = buf.split_first() {
                            match ch {
                                CH_TCP => { let _ = bcast.send(rest.to_vec()); }
                                CH_UDP => {
                                    if let Some(addr) = *last.lock().await {
                                        let _ = sock.send_to(rest, addr).await;
                                    }
                                }
                                _ => {}
                            }
                        }
                        continue;
                    }
                    Ok(Message::Text(t)) => t.to_string(),
                    Ok(Message::Close(_)) | Err(_) => {
                        st_w.log(&app_w, LogEntry::warn("Connessione relay chiusa")).await;
                        sd.store(true, Ordering::Relaxed); break;
                    }
                    _ => continue,
                };
                let val: Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
                match val.get("type").and_then(|t| t.as_str()) {
                    Some("data") => {
                        if let Some(b64) = val.get("payload").and_then(|p| p.as_str()) {
                            use base64::Engine;
                            if let Ok(buf) = base64::engine::general_purpose::STANDARD.decode(b64) {
                                if let Some((&ch, rest)) = buf.split_first() {
                                    match ch {
                                        CH_TCP => { let _ = bcast.send(rest.to_vec()); }
                                        CH_UDP => {
                                            if let Some(addr) = *last.lock().await {
                                                let _ = sock.send_to(rest, addr).await;
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                    Some("vin") => {
                        if let Some(v) = val.get("vin").and_then(|x| x.as_str()) {
                            if !v.is_empty() {
                                let _ = app_w.emit("vin_detected", v);
                                st_w.log(&app_w, LogEntry::info(format!("VIN dal Mac: {v}"))).await;
                            }
                        }
                    }
                    Some("peer_disconnected") => {
                        st_w.log(&app_w, LogEntry::warn("AutoBridge Mac disconnesso dal relay")).await;
                        sd.store(true, Ordering::Relaxed); break;
                    }
                    Some("heartbeat_ack") => {}
                    _ => {}
                }
            }
        });
    }

    // ── Task: scrittore relay + heartbeat ──────────────────────────────────
    {
        let sd  = shutdown.clone();
        let sid = session_id.clone();
        tokio::spawn(async move {
            let mut hb = interval(Duration::from_secs(2));
            loop {
                tokio::select! {
                    framed = to_relay_rx.recv() => {
                        match framed {
                            Some(bytes) => {
                                // Dato grezzo come frame BINARIO (no base64/JSON).
                                if ws_tx.send(Message::Binary(bytes.into())).await.is_err() {
                                    sd.store(true, Ordering::Relaxed); break;
                                }
                            }
                            None => break,
                        }
                    }
                    _ = hb.tick() => {
                        if sd.load(Ordering::Relaxed) { break; }
                        let hbm = json!({"type":"heartbeat","session_id":sid});
                        if ws_tx.send(Message::Text(hbm.to_string().into())).await.is_err() {
                            sd.store(true, Ordering::Relaxed); break;
                        }
                    }
                }
            }
        });
    }

    // ── TCP listener (ISTA) ────────────────────────────────────────────────
    let listener = TcpListener::bind(&tcp_addr).await
        .map_err(|e| format!("TCP bind {tcp_addr}: {e}"))?;
    state.log(&app, LogEntry::info(format!(
        "TCP in ascolto su {tcp_addr} — in attesa di ISTA"
    ))).await;

    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((tcp, peer)) => {
                        state.log(&app, LogEntry::info(format!("ISTA connesso da {peer}"))).await;
                        let to_relay = to_relay_tx.clone();
                        let rx_bcast = tcp_bcast.subscribe();
                        let sd       = shutdown.clone();
                        let app_c    = app.clone();
                        let st_c     = state.clone();
                        tokio::spawn(async move {
                            handle_ista_client(tcp, peer, to_relay, rx_bcast, sd, app_c, st_c).await;
                        });
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

// ── Per-client ISTA handler (trasparente) ───────────────────────────────────

async fn handle_ista_client(
    tcp:          tokio::net::TcpStream,
    peer:         SocketAddr,
    to_relay:     mpsc::Sender<Vec<u8>>,
    mut rx_bcast: broadcast::Receiver<Vec<u8>>,
    shutdown:     Arc<AtomicBool>,
    app:          AppHandle,
    state:        Arc<AppState>,
) {
    let _ = tcp.set_nodelay(true); // bassa latenza diagnostica
    let (mut tcp_rx, mut tcp_tx) = tcp.into_split();
    let mut buf = vec![0u8; 65536];

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }
        tokio::select! {
            r = tcp_rx.read(&mut buf) => {
                match r {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let mut framed = Vec::with_capacity(n + 1);
                        framed.push(CH_TCP);
                        framed.extend_from_slice(&buf[..n]);
                        if to_relay.send(framed).await.is_err() { break; }
                    }
                }
            }
            msg = rx_bcast.recv() => {
                match msg {
                    Ok(bytes) => { if tcp_tx.write_all(&bytes).await.is_err() { break; } }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
    state.log(&app, LogEntry::info(format!("ISTA disconnesso: {peer}"))).await;
}
