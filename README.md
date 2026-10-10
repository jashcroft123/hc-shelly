# hc-shelly

A standalone plugin for [homeCore](https://github.com/homeCore-io/homeCore).
Developed alongside [jashcroft123/homeCore](https://github.com/jashcroft123/homeCore).
The Rust SDK dependency comes directly from the original homeCore repository,
pinned to [88f87c4](https://github.com/homeCore-io/homeCore/commit/88f87c4edfef47d966036086117b563d17d499fa)
for reproducible builds. This repository originated from that project's plugin
template and retains its dual MIT/Apache-2.0 licensing.

Local integration for **Shelly Dimmer Gen3** (app `DimmerG3`, model
`S3DM-0A101WWL`). Uses the device's HTTP RPC API, without a cloud account or
changes to the Shelly's MQTT settings. Gen1 Dimmer 2 is not supported.

Features: on/off, brightness 0–100%, five-second state polling (including
physical switch changes), hardware state readback after commands, offline
availability, reconnects, per-device command queues, SHA-256 Digest authentication,
and homeCore's typed configuration UI and management heartbeat.

## Build and configure

```sh
git clone https://github.com/jashcroft123/hc-shelly.git
cd hc-shelly
cargo build --release --locked
# Copy config/config.toml.example into your homeCore installation as
# config/plugins/plugin.shelly.toml, then edit its device addresses.
```

Add one `[[shelly.devices]]` row per dimmer with a stable id, name, IP/hostname
and optional local admin password. Prefer a DHCP reservation. This version uses
configured addresses rather than automatic discovery. Keep credentials local.

For core to supervise the binary, add to `homecore.toml` and restart core:

```toml
[[plugins]]
id = "plugin.shelly"
binary = "/absolute/path/to/hc-shelly"
config = "/absolute/path/to/config/plugins/plugin.shelly.toml"
enabled = true
```

The plugin builds independently of the homeCore workspace; it is not in the upstream signed
plugin registry. To run manually: `hc-shelly /path/to/plugin.shelly.toml`.
The plugin and devices must be able to reach each other on the network.

Commands use `{"on":true}` or `{"brightness":60}`; both can be sent together.
Brightness is Shelly's native percentage, not 0–255. Brightness-only commands
leave power behavior to the device; supply `on` explicitly when desired.
Only confirmed device state is published. No automatic calibration, firmware
updates or device configuration changes are performed. Commands are not retried
after transport failures, because their outcome may be unknown.

Verification: `cargo test --locked`, `cargo clippy --all-targets
--all-features --locked -- -D warnings`. Tests use simulated HTTP endpoints for control
and errors; physical lighting should be tested separately.

API references: [Dimmer Gen3](https://shelly-api-docs.shelly.cloud/gen2/Devices/Gen3/ShellyDimmerG3/),
[Light](https://shelly-api-docs.shelly.cloud/gen2/ComponentsAndServices/Light/),
[authentication](https://shelly-api-docs.shelly.cloud/gen2/General/Authentication/).
