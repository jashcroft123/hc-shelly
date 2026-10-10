mod config;
mod mqtt;
mod rpc;
mod websocket;

use anyhow::Result;
use plugin_sdk_rs::mqtt_log_layer::{MqttLogHandle, MqttLogLayer};
use plugin_sdk_rs::types::schema::{
    AttributeKind, AttributeSchema, BoolStates, DeviceSchema, StateLabel,
};
use plugin_sdk_rs::types::{Capabilities, PluginNotice};
use plugin_sdk_rs::{DevicePublisher, PluginClient, PluginConfig, PluginNotices};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    time::Duration,
};
use tokio::sync::{mpsc, watch};
use tracing::{error, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    let (mqtt_layer, logs) = MqttLogLayer::new();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "hc_shelly=info,plugin_sdk_rs=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(mqtt_layer)
        .init();
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/config.toml".into());
    if let Err(e) = run(&path, logs).await {
        error!(error = %e, "Shelly plugin stopped");
        std::process::exit(1);
    }
}

async fn run(path: &str, logs: MqttLogHandle) -> Result<()> {
    let cfg = config::Config::load(path)?;
    let client = PluginClient::connect(PluginConfig {
        broker_host: cfg.homecore.broker_host,
        broker_port: cfg.homecore.broker_port,
        plugin_id: cfg.homecore.plugin_id.clone(),
        password: cfg.homecore.password,
    })
    .await?
    .with_device_persistence(
        Path::new(path)
            .parent()
            .unwrap_or(Path::new("."))
            .join(".published-device-ids.json"),
    );
    logs.connect(
        client.mqtt_client(),
        &cfg.homecore.plugin_id,
        &cfg.logging.forward_level,
    );
    let publisher = client.device_publisher();
    let notices = client.notices();
    let mut mgmt = client
        .enable_management(
            30,
            Some(env!("CARGO_PKG_VERSION").into()),
            Some(path.into()),
            None,
        )
        .await?
        .with_capabilities(capabilities())
        .with_config_descriptor(config::descriptor());
    if let Some(schema) = config::schema() {
        mgmt = mgmt.with_config_schema(schema);
    }

    // Per-device queues and workers keep a disconnected dimmer from blocking others.
    let mut routes = HashMap::new();
    let mut workers = Vec::new();
    let mut mqtt_routes = Vec::new();
    let mut websocket_routes = Vec::new();
    for device in &cfg.shelly.devices {
        let (tx, rx) = mpsc::channel::<Value>(32);
        routes.insert(device.id.clone(), tx);
        let updates = if device.state_transport != config::StateTransport::Http {
            let (tx, rx) = watch::channel(None);
            match device.state_transport {
                config::StateTransport::Mqtt => mqtt_routes.push(mqtt::Route {
                    prefix: device.mqtt_topic.clone(),
                    updates: tx,
                }),
                config::StateTransport::Websocket => websocket_routes.push((device.clone(), tx)),
                config::StateTransport::Http => unreachable!(),
            }
            Some(rx)
        } else {
            None
        };
        workers.push((device.clone(), rx, updates));
    }
    // Drive MQTT before registrations can fill its bounded publish queue.
    let mut mqtt_task = tokio::spawn(async move {
        client
            .run_managed(
                move |id, command| {
                    if let Some(tx) = routes.get(&id) {
                        if tx.try_send(command).is_err() {
                            warn!(device_id = %id, "command queue full");
                        }
                    }
                },
                mgmt,
            )
            .await
    });

    if cfg.shelly.devices.is_empty() {
        notices.raise(
            PluginNotice::warning("no_devices_configured", "No Shelly dimmers configured")
                .with_remedy("Add a Dimmer Gen3 IP address under shelly.devices."),
        );
    } else {
        notices.clear("no_devices_configured");
    }
    let live: HashSet<String> = cfg.shelly.devices.iter().map(|d| d.id.clone()).collect();
    for device in &cfg.shelly.devices {
        publisher
            .register_device_full(&device.id, &device.name, Some("light"), None, None)
            .await?;
        publisher
            .register_device_schema(&device.id, &device_schema())
            .await?;
        publisher.publish_availability(&device.id, false).await?;
        publisher.subscribe_commands(&device.id).await?;
    }
    publisher.reconcile_devices(live).await?;
    let mut tasks = tokio::task::JoinSet::new();
    if !mqtt_routes.is_empty() {
        let mqtt_config = cfg.shelly.mqtt.clone();
        tasks.spawn(async move { mqtt::run(mqtt_config, mqtt_routes).await });
    }
    for (device, updates) in websocket_routes {
        tasks.spawn(async move { websocket::run(device, updates).await });
    }
    for (device, rx, updates) in workers {
        tasks.spawn(worker(
            device,
            cfg.shelly.poll_interval_secs,
            rx,
            updates,
            publisher.clone(),
            notices.clone(),
        ));
    }
    tokio::select! {
        result = &mut mqtt_task => { result??; }
        _ = tokio::signal::ctrl_c() => {}
        result = tasks.join_next(), if !tasks.is_empty() => {
            match result {
                Some(Ok(Err(e))) => return Err(e),
                Some(Err(e)) => return Err(e.into()),
                _ => anyhow::bail!("device worker unexpectedly stopped"),
            }
        }
    }
    tasks.abort_all();
    mqtt_task.abort();
    Ok(())
}

async fn worker(
    device: config::Device,
    interval: u64,
    mut commands: mpsc::Receiver<Value>,
    mut updates: Option<watch::Receiver<Option<mqtt::Update>>>,
    publisher: DevicePublisher,
    notices: PluginNotices,
) -> Result<()> {
    let mut rpc = rpc::ShellyClient::new(&device.host, &device.password)?;
    let mut poll = tokio::time::interval(Duration::from_secs(interval));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut verified = false;
    let mut push_connected = false;
    let notice = format!("device_unreachable_{}", device.id);
    let push_notice = format!("device_push_disconnected_{}", device.id);
    loop {
        let command = tokio::select! {
            // Poll HTTP devices, failed verification, or disconnected WebSockets.
            _ = poll.tick(), if device.state_transport == config::StateTransport::Http
                || !verified
                || (device.state_transport == config::StateTransport::Websocket && !push_connected) => None,
            command = commands.recv() => match command { Some(c) => Some(c), None => return Ok(()) },
            update = async {
                match updates.as_mut() {
                    Some(rx) => {
                        rx.changed().await.ok()?;
                        rx.borrow_and_update().clone()
                    }
                    None => std::future::pending().await,
                }
            } => {
                match update {
                    Some(mqtt::Update::State(state)) => {
                        push_connected = true;
                        notices.clear(&push_notice);
                        if verified {
                            publisher.publish_state(&device.id, &state).await?;
                            publisher.publish_availability(&device.id, true).await?;
                            notices.clear(&notice);
                            continue;
                        }
                        None
                    }
                    Some(mqtt::Update::Offline) => {
                        push_connected = false;
                        publisher.publish_availability(&device.id, false).await?;
                        notices.raise(PluginNotice::warning(&push_notice,
                            format!("{} push connection is unavailable", device.name))
                            .with_remedy("Check Wi-Fi, device password and selected broker. WebSocket mode uses HTTP polling while reconnecting."));
                        continue;
                    }
                    Some(mqtt::Update::Resync) => None,
                    None => return Ok(()),
                }
            }
        };
        if let Some(ref payload) = command {
            if let Err(e) = rpc::command_params(payload) {
                warn!(device_id = %device.id, error = %e, "invalid command");
                continue;
            }
        }
        let result = async {
            if !verified {
                rpc.verify_device().await?;
                verified = true;
            }
            match command.as_ref() {
                Some(payload) => rpc.set(payload).await,
                None => rpc.status().await,
            }
        }
        .await;
        match result {
            Ok(state) => {
                if let Some(payload) = command.as_ref() {
                    publisher
                        .publish_state_for_command(&device.id, &state, payload, "shelly")
                        .await?;
                } else {
                    publisher.publish_state(&device.id, &state).await?;
                }
                publisher.publish_availability(&device.id, true).await?;
                notices.clear(&notice);
            }
            Err(e) => {
                verified = false;
                publisher.publish_availability(&device.id, false).await?;
                notices.raise(
                    PluginNotice::warning(
                        &notice,
                        format!("{} could not be read or controlled: {e}", device.name),
                    )
                    .with_remedy("Check device address, Wi-Fi, calibration and admin password."),
                );
                warn!(device_id = %device.id, error = %e, "Shelly request failed");
            }
        }
    }
}

fn capabilities() -> Capabilities {
    Capabilities {
        spec: "1".into(),
        plugin_id: String::new(),
        actions: vec![],
    }
}
fn device_schema() -> DeviceSchema {
    let mut attributes = HashMap::new();
    attributes.insert(
        "on".into(),
        AttributeSchema {
            kind: AttributeKind::Bool,
            writable: true,
            display_name: Some("Power".into()),
            states: Some(BoolStates {
                when_true: StateLabel::verbed("on", "turns on"),
                when_false: StateLabel::verbed("off", "turns off"),
            }),
            ..Default::default()
        },
    );
    attributes.insert(
        "brightness".into(),
        AttributeSchema {
            kind: AttributeKind::Integer,
            writable: true,
            display_name: Some("Brightness".into()),
            unit: Some("%".into()),
            min: Some(0.0),
            max: Some(100.0),
            step: Some(1.0),
            ..Default::default()
        },
    );
    DeviceSchema {
        attributes,
        ..Default::default()
    }
}
#[cfg(test)]
mod tests {
    #[test]
    fn plugin_conformance() {
        plugin_sdk_rs::conformance::check_all(&super::device_schema(), &super::capabilities())
            .assert_ok();
        plugin_sdk_rs::conformance::check_actions_routed(&super::capabilities(), &[]).assert_ok();
    }
}
