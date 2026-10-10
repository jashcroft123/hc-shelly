# Enable WebSocket, HTTP polling or MQTT push updates

Each Dimmer Gen3 can choose its own `state_transport`. **WebSocket is the
recommended default** because it receives changes directly without configuring
a separate device broker. HTTP polling remains a useful compatibility option;
MQTT is useful when you already have a LAN broker and want device publications.
A single plugin can run all three modes together; keep one row per dimmer.

| Behavior | `websocket` (default) | `http` | `mqtt` |
|---|---|---|---|
| State changes | Direct device notifications; HTTP polling while disconnected | Local HTTP reads every five seconds by default | Shelly publishes full light status on change |
| Dashboard commands | Local HTTP `Light.Set`, then `Light.GetStatus` | Same HTTP command/readback | Same HTTP command/readback |
| Startup/reconnect | HTTP model verification/read; WebSocket status snapshot | HTTP model verification/read | HTTP verification/read, then MQTT notifications |
| Offline detection | Socket close/heartbeat, plus HTTP fallback checks | Failed HTTP requests | MQTT online/LWT, broker disconnect, failed HTTP requests |
| Device setup | No extra settings | No extra settings | Enable MQTT and component status notifications |

No cloud account is required. In MQTT mode, normal state monitoring does not
periodically poll the dimmer. HTTP checks retry at `poll_interval_secs` when
initial verification or a later HTTP request fails, until it becomes reachable.
All modes need local HTTP access for commands and model verification. Since
version 0.2.0, existing rows without `state_transport` select WebSocket. Add
`state_transport = "http"` if you prefer their previous polling behavior.

## 1. Install the plugin

Build with `cargo build --release --locked`. Add the binary to homeCore's
`[[plugins]]` list as shown in the [README](../README.md). Edit the core-owned
configuration at `config/plugins/plugin.shelly.toml`, or use the plugin's
configuration UI. Restart the plugin after changing configuration.

Give each dimmer a DHCP reservation. `host` is its local address, and `password`
is its local **device admin password** if enabled. Broker passwords are separate.
Keep the `id` stable when switching transport so dashboards and rules still point
to the same light.

## 2. Use WebSocket (recommended)

```toml
[shelly]
poll_interval_secs = 5 # fallback while the WebSocket is disconnected

[[shelly.devices]]
id = "shelly_dimmer_living_room"
name = "Living room dimmer"
host = "192.168.1.100"
password = ""
state_transport = "websocket"
```

You may omit `state_transport`; WebSocket is the default. The plugin opens
`ws://DEVICE_IP/rpc`, requests an initial light status, then listens for
`NotifyStatus` and `NotifyFullStatus`. Partial updates retain the other known
attributes: a brightness notification does not erase power state. The configured
device admin password is used for SHA-256 RPC authentication, including legacy
numeric and newer string nonces. No device MQTT or outbound WebSocket setup is
required.

If the socket closes, the worker resumes HTTP polling and keeps retrying the
WebSocket, with reconnect backoff from two to sixty seconds. A thirty-second
ping checks idle connections; a silent connection times out after about sixty
seconds. HTTP fallback can keep the light available when only the push connection
fails. A plugin notice identifies the disconnected push connection. Normal
monitoring stops periodic HTTP polls once WebSocket state arrives.

## 3. Enable HTTP polling

No device MQTT setup is necessary:

```toml
[shelly]
poll_interval_secs = 5

[[shelly.devices]]
id = "shelly_dimmer_living_room"
name = "Living room dimmer"
host = "192.168.1.100"
password = ""
state_transport = "http"
```

Set `state_transport = "http"` explicitly. Physical switch changes
appear on the next poll, while dashboard commands trigger immediate readback.
The polling interval must be at least one second.

## 4. Enable native MQTT updates

### Prepare a broker

Use a broker reachable by **both the Shelly and the plugin**, for example a
Mosquitto server on your LAN. `[homecore]` connects the plugin to homeCore;
`[shelly.mqtt]` connects its native Shelly listener to the devices' broker. These
can refer to the same broker or different brokers. This listener needs a unique
client id; it must not reuse `plugin.shelly` or a device's client id.

When using homeCore's embedded broker, bind it to a LAN-reachable interface and
configure credentials for the listener and every Shelly client. Its default
loopback binding cannot be reached by physical devices. Docker needs its MQTT
port published to the LAN; WSL also needs working LAN routing/forwarding. The
current localhost-only preview setup is not sufficient for device MQTT access.

The native listener currently uses MQTT over TCP with optional username/password.
Use it on your trusted LAN or VPN; do not expose an unencrypted MQTT port to the
internet. TLS is not currently configurable in this plugin.

### Configure the dimmer

Open the dimmer's local web UI and enable MQTT. Set the broker address/port and
its device credentials. Enable **component status notifications** (`status_ntf`)
and note the exact topic prefix. Save and reboot the device if requested.

Alternatively, read the current settings:

```sh
curl "http://DEVICE_IP/rpc/MQTT.GetConfig"
```

Then enable MQTT and full component state notifications (replace the example
broker address; add broker credentials using the device UI):

```sh
curl -H 'Content-Type: application/json' \
  -d '{"config":{"enable":true,"server":"192.168.1.10:1883","status_ntf":true}}' \
  "http://DEVICE_IP/rpc/MQTT.SetConfig"
```

If local device authentication is enabled, add `--digest --user admin` to curl
to enter the password interactively. Follow the response's reboot requirement,
then check `MQTT.GetStatus`; `connected` should be `true`. Do not substitute
`localhost` for the broker address on the Shelly: that would address the dimmer
itself.

If `MQTT.GetConfig` reports `topic_prefix = null`, the prefix is the device id
returned by `Shelly.GetDeviceInfo`. Otherwise use the configured prefix exactly.
The listener consumes `<prefix>/status/light:0` and `<prefix>/online`. It does not
use `<prefix>/events/rpc`, so enabling only RPC notifications is insufficient.

### Configure the plugin

```toml
[shelly]
poll_interval_secs = 5 # HTTP devices and failed HTTP verification/recovery checks

[shelly.mqtt]
broker_host = "192.168.1.10"
broker_port = 1883
client_id = "hc-shelly-events"
username = "shelly-listener"
password = "REPLACE_LOCALLY"

[[shelly.devices]]
id = "shelly_dimmer_hall"
name = "Hall dimmer"
host = "192.168.1.101"
password = "" # local device admin password, not the MQTT password
state_transport = "mqtt"
mqtt_topic = "shellydimmerg3-device-id" # exact prefix from this dimmer
```

Restart the plugin. Flip the physical switch: its state should update when the
MQTT notification arrives. Broker last-will offline detection depends on its
keepalive timeout; it is not necessarily instantaneous.

You can verify device messages independently with:

```sh
mosquitto_sub -h 192.168.1.10 -t 'YOUR_PREFIX/status/light:0' -t 'YOUR_PREFIX/online'
```

Add authentication appropriate to your broker, without committing credentials.
If messages do not appear, check `status_ntf`, the exact topic prefix, credentials,
and LAN access. The plugin ignores retained old light state and malformed messages;
it refreshes state over HTTP on subscription/reconnection and online notifications.

## 5. Mix modes or switch back

Combine the `[[shelly.devices]]` examples in one file, with different stable
ids and addresses. Keep a single `[shelly]` and `[shelly.mqtt]` section. HTTP
and WebSocket devices ignore MQTT settings; MQTT devices receive push updates.

To switch a dimmer back to polling, change its existing row to
`state_transport = "http"` and restart the plugin. No dashboard or rule edits
are needed when its `id` remains unchanged. The plugin never enables/disables
device MQTT, changes firmware, or performs calibration automatically.

Protocol references: [Shelly MQTT](https://shelly-api-docs.shelly.cloud/gen2/ComponentsAndServices/Mqtt/),
[RPC channels](https://shelly-api-docs.shelly.cloud/gen2/General/RPCChannels/),
[authentication](https://shelly-api-docs.shelly.cloud/gen2/General/Authentication/),
[Light](https://shelly-api-docs.shelly.cloud/gen2/ComponentsAndServices/Light/).
