//! Native Shelly state notifications. Control and recovery still use local HTTP.
use anyhow::{ensure, Result};
use rumqttc::{AsyncClient, MqttOptions, Packet, QoS, SubscribeFilter};
use serde_json::Value;
use std::time::Duration;
use tokio::sync::watch;
use tracing::warn;

#[derive(Clone, Debug, PartialEq)]
pub enum Update {
    State(Value),
    Offline,
    Resync,
}

pub struct Route {
    pub prefix: String,
    pub updates: watch::Sender<Option<Update>>,
}

fn decode(prefix: &str, topic: &str, payload: &[u8], retained: bool) -> Result<Option<Update>> {
    if topic == format!("{prefix}/online") {
        let online: bool = serde_json::from_slice(payload)?;
        return Ok(Some(if online {
            Update::Resync
        } else {
            Update::Offline
        }));
    }
    if topic == format!("{prefix}/status/light:0") && !retained {
        let status: Value = serde_json::from_slice(payload)?;
        return Ok(Some(Update::State(crate::rpc::state_from_status(&status)?)));
    }
    // Do not let retained historical state replace a fresh HTTP startup read.
    Ok(None)
}

pub async fn run(config: crate::config::Mqtt, routes: Vec<Route>) -> Result<()> {
    let mut options = MqttOptions::new(&config.client_id, &config.broker_host, config.broker_port);
    options.set_keep_alive(Duration::from_secs(30));
    options.set_clean_session(true);
    if !config.username.is_empty() {
        options.set_credentials(config.username, config.password);
    }
    let (client, mut eventloop) = AsyncClient::new(options, 32);
    loop {
        match eventloop.poll().await {
            Ok(rumqttc::Event::Incoming(Packet::ConnAck(_))) => {
                let filters = routes.iter().flat_map(|route| {
                    [
                        SubscribeFilter::new(
                            format!("{}/status/light:0", route.prefix),
                            QoS::AtLeastOnce,
                        ),
                        SubscribeFilter::new(format!("{}/online", route.prefix), QoS::AtLeastOnce),
                    ]
                });
                client.subscribe_many(filters).await?;
            }
            Ok(rumqttc::Event::Incoming(Packet::SubAck(ack))) => {
                ensure!(
                    ack.return_codes.len() == routes.len() * 2
                        && ack
                            .return_codes
                            .iter()
                            .all(|code| !matches!(code, rumqttc::SubscribeReasonCode::Failure)),
                    "Shelly MQTT broker rejected required subscriptions"
                );
                for route in &routes {
                    route.updates.send_replace(Some(Update::Resync));
                }
                tracing::info!(
                    devices = routes.len(),
                    "Shelly MQTT state subscriptions confirmed"
                );
            }
            Ok(rumqttc::Event::Incoming(Packet::Publish(message))) => {
                for route in &routes {
                    match decode(
                        &route.prefix,
                        &message.topic,
                        &message.payload,
                        message.retain,
                    ) {
                        Ok(Some(update)) => {
                            route.updates.send_replace(Some(update));
                        }
                        Ok(None) => {}
                        Err(error) => {
                            warn!(topic = %message.topic, %error, "Invalid Shelly MQTT state")
                        }
                    }
                }
            }
            Ok(rumqttc::Event::Incoming(Packet::Disconnect)) | Err(_) => {
                for route in &routes {
                    route.updates.send_replace(Some(Update::Offline));
                }
                warn!("Shelly MQTT connection lost; retrying in two seconds");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Ok(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn read_packet(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
        let header = stream.read_u8().await.unwrap();
        let mut length = 0usize;
        let mut multiplier = 1usize;
        loop {
            let byte = stream.read_u8().await.unwrap();
            length += (byte as usize & 127) * multiplier;
            if byte & 128 == 0 {
                break;
            }
            multiplier *= 128;
            assert!(multiplier <= 128 * 128 * 128);
        }
        let mut body = vec![0; length];
        stream.read_exact(&mut body).await.unwrap();
        (header, body)
    }

    #[tokio::test]
    async fn subscribes_receives_push_state_and_detects_broker_disconnect() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (send_state, state_requested) = tokio::sync::oneshot::channel();
        let (disconnect, disconnect_requested) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (header, body) = read_packet(&mut stream).await;
            assert_eq!(header >> 4, 1);
            assert!(body.windows(8).any(|bytes| bytes == b"listener"));
            stream.write_all(&[0x20, 2, 0, 0]).await.unwrap();
            let (header, body) = read_packet(&mut stream).await;
            assert_eq!(header >> 4, 8);
            assert!(body
                .windows(21)
                .any(|bytes| bytes == b"dimmer/status/light:0"));
            assert!(body.windows(13).any(|bytes| bytes == b"dimmer/online"));
            stream
                .write_all(&[0x90, 4, body[0], body[1], 1, 1])
                .await
                .unwrap();
            state_requested.await.unwrap();
            let topic = b"dimmer/status/light:0";
            let payload = br#"{"output":true,"brightness":73}"#;
            let mut packet = vec![0x30, (2 + topic.len() + payload.len()) as u8];
            packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            packet.extend_from_slice(topic);
            packet.extend_from_slice(payload);
            stream.write_all(&packet).await.unwrap();
            disconnect_requested.await.unwrap();
            stream.write_all(&[0xe0, 0]).await.unwrap();
        });
        let (tx, mut rx) = watch::channel(None);
        let task = tokio::spawn(run(
            crate::config::Mqtt {
                broker_port: port,
                username: "listener".into(),
                password: "secret".into(),
                ..Default::default()
            },
            vec![Route {
                prefix: "dimmer".into(),
                updates: tx,
            }],
        ));
        async fn next(rx: &mut watch::Receiver<Option<Update>>) -> Update {
            tokio::time::timeout(Duration::from_secs(3), rx.changed())
                .await
                .unwrap()
                .unwrap();
            rx.borrow_and_update().clone().unwrap()
        }
        assert_eq!(next(&mut rx).await, Update::Resync);
        send_state.send(()).unwrap();
        assert_eq!(
            next(&mut rx).await,
            Update::State(serde_json::json!({"on":true,"brightness":73}))
        );
        disconnect.send(()).unwrap();
        assert_eq!(next(&mut rx).await, Update::Offline);
        server.await.unwrap();
        task.abort();
    }

    #[test]
    fn routes_complete_light_state_and_online_notifications() {
        assert_eq!(
            decode(
                "dimmer",
                "dimmer/status/light:0",
                br#"{"output":true,"brightness":42}"#,
                false
            )
            .unwrap(),
            Some(Update::State(
                serde_json::json!({"on":true,"brightness":42})
            ))
        );
        assert_eq!(
            decode("dimmer", "dimmer/online", b"false", false).unwrap(),
            Some(Update::Offline)
        );
        assert_eq!(
            decode("dimmer", "dimmer/online", b"true", true).unwrap(),
            Some(Update::Resync)
        );
        assert_eq!(
            decode("dimmer", "other/status/light:0", b"bad", false).unwrap(),
            None
        );
        assert_eq!(
            decode("dimmer", "dimmer/status/light:1", b"bad", false).unwrap(),
            None
        );
    }

    #[test]
    fn stale_retained_state_and_malformed_notifications_are_not_published() {
        assert_eq!(
            decode("dimmer", "dimmer/status/light:0", b"bad", true).unwrap(),
            None
        );
        assert!(decode(
            "dimmer",
            "dimmer/status/light:0",
            br#"{"output":true}"#,
            false
        )
        .is_err());
        assert!(decode("dimmer", "dimmer/online", b"not-json", false).is_err());
    }
}
