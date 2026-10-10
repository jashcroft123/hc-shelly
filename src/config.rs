use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use std::collections::HashSet;

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Config {
    #[serde(default)]
    pub homecore: HomeCore,
    #[serde(default)]
    pub shelly: Shelly,
    #[serde(default)]
    pub logging: Logging,
}

#[derive(Deserialize)]
#[serde(default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct HomeCore {
    pub broker_host: String,
    pub broker_port: u16,
    pub plugin_id: String,
    pub password: String,
}
impl Default for HomeCore {
    fn default() -> Self {
        Self {
            broker_host: "127.0.0.1".into(),
            broker_port: 1883,
            plugin_id: "plugin.shelly".into(),
            password: String::new(),
        }
    }
}

#[derive(Deserialize)]
#[serde(default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Shelly {
    pub poll_interval_secs: u64,
    pub devices: Vec<Device>,
    pub mqtt: Mqtt,
}
impl Default for Shelly {
    fn default() -> Self {
        Self {
            poll_interval_secs: 5,
            devices: vec![],
            mqtt: Mqtt::default(),
        }
    }
}

// Do not derive Debug: this struct contains device credentials.
#[derive(Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Device {
    pub id: String,
    pub name: String,
    pub host: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub state_transport: StateTransport,
    #[serde(default)]
    pub mqtt_topic: String,
}

#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum StateTransport {
    #[default]
    Websocket,
    Http,
    Mqtt,
}

// Credentials are intentionally not Debug-printable.
#[derive(Clone, Deserialize)]
#[serde(default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Mqtt {
    pub broker_host: String,
    pub broker_port: u16,
    pub client_id: String,
    pub username: String,
    pub password: String,
}
impl Default for Mqtt {
    fn default() -> Self {
        Self {
            broker_host: "127.0.0.1".into(),
            broker_port: 1883,
            client_id: "hc-shelly-events".into(),
            username: String::new(),
            password: String::new(),
        }
    }
}

#[derive(Deserialize)]
#[serde(default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Logging {
    pub forward_level: String,
}
impl Default for Logging {
    fn default() -> Self {
        Self {
            forward_level: "info".into(),
        }
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let cfg: Self = toml::from_str(
            &std::fs::read_to_string(path).with_context(|| format!("cannot read {path}"))?,
        )
        .context("invalid Shelly configuration")?;
        cfg.validate()?;
        Ok(cfg)
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            self.homecore.plugin_id == "plugin.shelly",
            "plugin_id must be plugin.shelly"
        );
        ensure!(
            self.shelly.poll_interval_secs >= 1,
            "poll interval must be at least 1 second"
        );
        let mut ids = HashSet::new();
        let mut topics = HashSet::new();
        for d in &self.shelly.devices {
            ensure!(
                !d.id.is_empty()
                    && d.id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-'),
                "device id must contain only letters, numbers, underscores or hyphens"
            );
            ensure!(ids.insert(&d.id), "duplicate device id");
            ensure!(!d.name.trim().is_empty(), "device name is required");
            crate::rpc::base_url(&d.host)?;
            if d.state_transport == StateTransport::Mqtt {
                ensure!(
                    !d.mqtt_topic.is_empty() && !d.mqtt_topic.contains(['+', '#', '\0'])
                        && !d.mqtt_topic.ends_with('/'),
                    "MQTT devices need their exact Shelly topic prefix without wildcards or trailing slash"
                );
                ensure!(
                    topics.insert(&d.mqtt_topic),
                    "duplicate Shelly MQTT topic prefix"
                );
            }
        }
        if !topics.is_empty() {
            ensure!(
                !self.shelly.mqtt.broker_host.trim().is_empty()
                    && self.shelly.mqtt.broker_port != 0
                    && !self.shelly.mqtt.client_id.trim().is_empty(),
                "MQTT broker host, port and client id are required"
            );
            ensure!(
                self.shelly.mqtt.client_id != self.homecore.plugin_id,
                "Shelly MQTT client id must differ from the homeCore plugin id"
            );
            ensure!(
                !self.shelly.mqtt.username.is_empty() || self.shelly.mqtt.password.is_empty(),
                "MQTT username is required when a broker password is supplied"
            );
        }
        Ok(())
    }
}

#[cfg(feature = "schema")]
pub fn schema() -> Option<serde_json::Value> {
    serde_json::to_value(schemars::schema_for!(Config)).ok()
}
#[cfg(not(feature = "schema"))]
pub fn schema() -> Option<serde_json::Value> {
    None
}

pub fn descriptor() -> serde_json::Value {
    use plugin_sdk_rs::config_descriptor::{Descriptor, Field, Section};
    Descriptor::new("plugin.shelly").title("Shelly Dimmer Gen3")
        .section(Section::new("shelly", "Shelly devices")
            .field(Field::duration("shelly.poll_interval_secs").label("State polling interval").unit("secs").min(1).default(5))
            .field(Field::table("shelly.devices").label("Dimmers").key_by("id")
                .columns([Field::text("name").label("Name"), Field::host("host").label("IP address or hostname"), Field::secret("password").label("Device admin password"),
                    Field::enumeration("state_transport").label("State updates").option("websocket", "WebSocket push (default)").option("http", "HTTP polling").option("mqtt", "MQTT push"),
                    Field::text("mqtt_topic").label("Shelly MQTT topic prefix")])
                .help("One row per Dimmer Gen3. WebSocket is the default, with HTTP fallback. MQTT push needs device MQTT setup. Commands use local HTTP in every mode.")))
        .section(Section::new("shelly_mqtt", "Shelly MQTT broker")
            .field(Field::host("shelly.mqtt.broker_host").label("Broker reachable by the devices"))
            .field(Field::port("shelly.mqtt.broker_port"))
            .field(Field::text("shelly.mqtt.client_id").label("Listener client id"))
            .field(Field::text("shelly.mqtt.username"))
            .field(Field::secret("shelly.mqtt.password")))
        .section(Section::new("logging", "Logging")
            .field(Field::enumeration("logging.forward_level").label("Forward to core")
                .option("trace", "Trace").option("debug", "Debug").option("info", "Info").option("warn", "Warn").option("error", "Error")))
        .section(Section::new("connection", "Connection").hidden()
            .field(Field::host("homecore.broker_host"))
            .field(Field::port("homecore.broker_port"))
            .field(Field::secret("homecore.password")))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_and_validation() {
        let mut cfg: Config = toml::from_str("").unwrap();
        cfg.validate().unwrap();
        cfg.shelly.poll_interval_secs = 0;
        assert!(cfg.validate().is_err());
        cfg.shelly.poll_interval_secs = 5;
        let d = Device {
            id: "dimmer".into(),
            name: "Light".into(),
            host: "192.168.1.100".into(),
            password: String::new(),
            state_transport: StateTransport::Http,
            mqtt_topic: String::new(),
        };
        cfg.shelly.devices = vec![d.clone(), d];
        assert!(cfg.validate().is_err());
    }
    #[test]
    fn old_configs_and_mixed_transport_example_are_valid() {
        let old: Config =
            toml::from_str("[[shelly.devices]]\nid='old'\nname='Light'\nhost='192.168.1.2'")
                .unwrap();
        old.validate().unwrap();
        assert!(old.shelly.devices[0].state_transport == StateTransport::Websocket);
        let mut mixed: Config =
            toml::from_str(include_str!("../config/config.toml.example")).unwrap();
        mixed.validate().unwrap();
        let mut device = mixed.shelly.devices[0].clone();
        device.id = "mqtt_dimmer".into();
        device.state_transport = StateTransport::Mqtt;
        device.mqtt_topic = "shellydimmerg3-abc".into();
        mixed.shelly.devices.push(device);
        mixed.validate().unwrap();
        mixed.shelly.devices[1].mqtt_topic = "bad/#".into();
        assert!(mixed.validate().is_err());
        mixed.shelly.devices[1].mqtt_topic = "shellydimmerg3-abc".into();
        mixed.shelly.mqtt.client_id = "plugin.shelly".into();
        assert!(mixed.validate().is_err());
    }
    #[cfg(feature = "schema")]
    #[test]
    fn descriptor_covers_every_schema_field() {
        let missing = plugin_sdk_rs::config_descriptor::missing_schema_coverage(
            &schema().unwrap(),
            &descriptor(),
            &["homecore.plugin_id"],
        );
        assert!(missing.is_empty(), "missing config fields: {missing:?}");
    }
}
