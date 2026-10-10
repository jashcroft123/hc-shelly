//! Direct Shelly RPC notifications, requiring no device MQTT configuration.
use anyhow::{bail, ensure, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tracing::warn;

fn auth(challenge: &Value, password: &str, cnonce: u32) -> Result<Value> {
    ensure!(!password.is_empty(), "device requires its admin password");
    ensure!(
        challenge["auth_type"] == "digest" && challenge["algorithm"] == "SHA-256",
        "unsupported WebSocket authentication challenge"
    );
    let realm = challenge["realm"]
        .as_str()
        .context("authentication realm missing")?;
    let nonce = match &challenge["nonce"] {
        Value::String(nonce) => nonce.clone(),
        Value::Number(nonce) => nonce.to_string(),
        _ => bail!("authentication nonce missing"),
    };
    let nc = format!("{:08x}", challenge["nc"].as_u64().unwrap_or(1));
    let hash = |value: &str| format!("{:x}", Sha256::digest(value.as_bytes()));
    let ha1 = hash(&format!("admin:{realm}:{password}"));
    let ha2 = hash("dummy_method:dummy_uri");
    let response = hash(&format!("{ha1}:{nonce}:{nc}:{cnonce}:auth:{ha2}"));
    Ok(
        json!({"realm":realm,"username":"admin","nonce":challenge["nonce"],
        "cnonce":cnonce,"nc":nc,"response":response,"algorithm":"SHA-256"}),
    )
}

fn merge_notification(state: &mut Value, frame: &Value) -> Result<Option<Value>> {
    if frame["method"] != "NotifyStatus" && frame["method"] != "NotifyFullStatus" {
        return Ok(None);
    }
    let Some(light) = frame["params"]["light:0"].as_object() else {
        return Ok(None);
    };
    let mut next = state.clone();
    let mut changed = false;
    for attribute in ["output", "brightness"] {
        if let Some(value) = light.get(attribute) {
            next[attribute] = value.clone();
            changed = true;
        }
    }
    if !changed {
        return Ok(None);
    }
    let published = crate::rpc::state_from_status(&next)?;
    *state = next;
    Ok(Some(published))
}

async fn session(
    device: &crate::config::Device,
    updates: &watch::Sender<Option<crate::mqtt::Update>>,
    retry: &mut Duration,
) -> Result<()> {
    let mut url = crate::rpc::base_url(&device.host)?.join("rpc")?;
    url.set_scheme("ws")
        .map_err(|_| anyhow::anyhow!("invalid WebSocket URL"))?;
    let (mut socket, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url.as_str()),
    )
    .await
    .context("WebSocket connection timed out")??;
    let mut request = json!({"id":1,"src":format!("hc-shelly-{}",device.id),
        "method":"Light.GetStatus","params":{"id":0}});
    socket
        .send(Message::Text(request.to_string().into()))
        .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut authenticated = false;
    let mut state = loop {
        let message = tokio::time::timeout_at(deadline, socket.next())
            .await
            .context("WebSocket initial state timed out")?
            .context("WebSocket closed before initial state")??;
        match message {
            Message::Text(text) => {
                let frame: Value = serde_json::from_str(&text)?;
                if frame["id"] != 1 {
                    continue;
                }
                if frame["error"]["code"] == 401 && !authenticated {
                    let challenge: Value = serde_json::from_str(
                        frame["error"]["message"]
                            .as_str()
                            .context("authentication challenge missing")?,
                    )?;
                    request["auth"] = auth(&challenge, &device.password, rand::random())?;
                    authenticated = true;
                    socket
                        .send(Message::Text(request.to_string().into()))
                        .await?;
                    continue;
                }
                ensure!(
                    frame.get("error").is_none(),
                    "WebSocket status request rejected"
                );
                let state = frame
                    .get("result")
                    .context("WebSocket status response missing")?
                    .clone();
                crate::rpc::state_from_status(&state)?;
                break state;
            }
            Message::Ping(data) => socket.send(Message::Pong(data)).await?,
            Message::Close(_) => bail!("WebSocket closed before initial state"),
            _ => {}
        }
    };
    *retry = Duration::from_secs(2);
    tracing::info!(device_id = %device.id, "WebSocket state stream connected");
    updates.send_replace(Some(crate::mqtt::Update::State(
        crate::rpc::state_from_status(&state)?,
    )));
    let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
    heartbeat.tick().await;
    let mut last_received = tokio::time::Instant::now();
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                ensure!(last_received.elapsed() < Duration::from_secs(60), "WebSocket heartbeat timed out");
                socket.send(Message::Ping(Vec::new().into())).await?;
            }
            message = socket.next() => {
                let message = message.context("WebSocket disconnected")??;
                last_received = tokio::time::Instant::now();
                match message {
                    Message::Text(text) => {
                        let result = serde_json::from_str(&text).map_err(anyhow::Error::from)
                            .and_then(|frame| merge_notification(&mut state, &frame));
                        match result {
                            Ok(Some(state)) => { updates.send_replace(Some(crate::mqtt::Update::State(state))); }
                            Ok(None) => {}
                            Err(error) => warn!(device_id = %device.id, %error, "Invalid WebSocket notification"),
                        }
                    }
                    Message::Ping(data) => socket.send(Message::Pong(data)).await?,
                    Message::Close(_) => bail!("WebSocket disconnected"),
                    _ => {}
                }
            }
        }
    }
}

pub async fn run(
    device: crate::config::Device,
    updates: watch::Sender<Option<crate::mqtt::Update>>,
) -> Result<()> {
    let mut retry = Duration::from_secs(2);
    loop {
        if let Err(error) = session(&device, &updates, &mut retry).await {
            updates.send_replace(Some(crate::mqtt::Update::Offline));
            warn!(device_id = %device.id, %error, retry_secs = retry.as_secs(), "WebSocket unavailable; HTTP fallback active");
        }
        tokio::time::sleep(retry).await;
        retry = (retry * 2).min(Duration::from_secs(60));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_updates_merge_without_losing_power_or_brightness() {
        let mut state = json!({"output":false,"brightness":27});
        let update = merge_notification(
            &mut state,
            &json!({"method":"NotifyStatus",
            "params":{"light:0":{"brightness":64}}}),
        )
        .unwrap();
        assert_eq!(update, Some(json!({"on":false,"brightness":64})));
        let update = merge_notification(
            &mut state,
            &json!({"method":"NotifyFullStatus",
            "params":{"light:0":{"output":true}}}),
        )
        .unwrap();
        assert_eq!(update, Some(json!({"on":true,"brightness":64})));
        assert!(merge_notification(
            &mut state,
            &json!({"method":"NotifyStatus",
            "params":{"light:0":{"brightness":200}}})
        )
        .is_err());
        assert_eq!(state, json!({"output":true,"brightness":64}));
    }

    #[test]
    fn authenticates_legacy_and_current_challenges_without_exposing_password() {
        for (nonce, expected) in [
            (
                json!(1625053638),
                "d70c68ef436d812170274bacb23c89044f406b2b9dc8f291355fe84b9a481bf8",
            ),
            (
                json!("base64-nonce"),
                "ce447de8f25e46b46a79967a74a80b3385266ac2cdeb005a7e3b467362b21e4e",
            ),
        ] {
            let challenge = json!({"auth_type":"digest","algorithm":"SHA-256",
                "realm":"dimmer","nonce":nonce,"nc":1});
            let auth = auth(&challenge, "password", 313273957).unwrap();
            assert_eq!(auth["nonce"], nonce);
            assert_eq!(auth["nc"], "00000001");
            assert_eq!(auth["username"], "admin");
            assert_eq!(auth["response"], expected);
            assert!(!auth.to_string().contains("password"));
        }
        assert!(auth(&json!({}), "", 1).is_err());
    }

    async fn check_session(password_required: bool) {
        use axum::{
            extract::ws::{Message as ServerMessage, WebSocketUpgrade},
            routing::get,
            Router,
        };
        let (push, push_requested) = tokio::sync::oneshot::channel();
        let (disconnect, disconnect_requested) = tokio::sync::oneshot::channel();
        let channels = std::sync::Arc::new(tokio::sync::Mutex::new(Some((
            push_requested,
            disconnect_requested,
        ))));
        let app = Router::new().route("/rpc", get(move |ws: WebSocketUpgrade| {
            let channels = channels.clone();
            async move { ws.on_upgrade(move |mut socket| async move {
                let (push_requested, disconnect_requested) = channels.lock().await.take().unwrap();
                let ServerMessage::Text(text) = socket.recv().await.unwrap().unwrap() else { panic!("RPC request expected"); };
                let request: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(request["method"], "Light.GetStatus");
                assert!(request["src"].as_str().unwrap().starts_with("hc-shelly-"));
                if password_required {
                    let challenge = json!({"auth_type":"digest","algorithm":"SHA-256",
                        "realm":"dimmer","nonce":1625053638,"nc":1});
                    socket.send(ServerMessage::Text(json!({"id":1,"error":{"code":401,
                        "message":challenge.to_string()}}).to_string().into())).await.unwrap();
                    let ServerMessage::Text(text) = socket.recv().await.unwrap().unwrap() else { panic!("Authenticated RPC expected"); };
                    let request: Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(request["auth"]["nonce"], 1625053638);
                    assert_eq!(request["auth"]["username"], "admin");
                    assert_eq!(request["auth"]["nc"], "00000001");
                    assert_eq!(request["auth"]["response"].as_str().unwrap().len(), 64);
                }
                socket.send(ServerMessage::Text(json!({"id":1,"result":{"output":false,"brightness":27}})
                    .to_string().into())).await.unwrap();
                push_requested.await.unwrap();
                socket.send(ServerMessage::Text(json!({"method":"NotifyStatus","params":{"light:0":{"brightness":64}}})
                    .to_string().into())).await.unwrap();
                disconnect_requested.await.unwrap();
                socket.send(ServerMessage::Close(None)).await.unwrap();
            }) }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let (tx, mut rx) = watch::channel(None);
        let task = tokio::spawn(run(
            crate::config::Device {
                id: "probe".into(),
                name: "Probe".into(),
                host: format!("127.0.0.1:{port}"),
                password: if password_required {
                    "password".into()
                } else {
                    String::new()
                },
                state_transport: crate::config::StateTransport::Websocket,
                mqtt_topic: String::new(),
            },
            tx,
        ));
        async fn next(
            rx: &mut watch::Receiver<Option<crate::mqtt::Update>>,
        ) -> crate::mqtt::Update {
            tokio::time::timeout(Duration::from_secs(3), rx.changed())
                .await
                .unwrap()
                .unwrap();
            rx.borrow_and_update().clone().unwrap()
        }
        assert_eq!(
            next(&mut rx).await,
            crate::mqtt::Update::State(json!({"on":false,"brightness":27}))
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), rx.changed())
                .await
                .is_err()
        );
        push.send(()).unwrap();
        assert_eq!(
            next(&mut rx).await,
            crate::mqtt::Update::State(json!({"on":false,"brightness":64}))
        );
        disconnect.send(()).unwrap();
        assert_eq!(next(&mut rx).await, crate::mqtt::Update::Offline);
        task.abort();
        server.abort();
    }

    #[tokio::test]
    async fn receives_websocket_push_updates_and_detects_disconnect() {
        check_session(false).await;
    }

    #[tokio::test]
    async fn answers_websocket_authentication_challenge_before_receiving_state() {
        check_session(true).await;
    }
}
