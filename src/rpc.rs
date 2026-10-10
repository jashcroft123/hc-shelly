use anyhow::{bail, ensure, Context, Result};
use digest_auth::{AuthContext, WwwAuthenticateHeader};
use reqwest::{header, Client, StatusCode, Url};
use serde_json::{json, Value};
use std::time::Duration;

pub fn base_url(host: &str) -> Result<Url> {
    ensure!(!host.trim().is_empty(), "Shelly host is required");
    let url = Url::parse(&format!("http://{host}")).context("invalid Shelly host")?;
    ensure!(url.host_str().is_some() && url.username().is_empty() && url.password().is_none()
        && url.path() == "/" && url.query().is_none() && url.fragment().is_none(),
        "host must be an IP address or hostname, optionally with a port; no URL, path or credentials");
    Ok(url)
}

pub struct ShellyClient {
    client: Client,
    base: Url,
    password: String,
    // Reuse the nonce and increment its count; firmware 2.x rate-limits fresh challenges.
    challenge: Option<WwwAuthenticateHeader>,
}

impl ShellyClient {
    pub fn new(host: &str, password: &str) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .build()?,
            base: base_url(host)?,
            password: password.into(),
            challenge: None,
        })
    }

    pub async fn call(&mut self, method: &str, params: Value) -> Result<Value> {
        let mut url = self.base.join(&format!("rpc/{method}"))?;
        if let Some(params) = params.as_object() {
            let mut query = url.query_pairs_mut();
            for (key, value) in params {
                query.append_pair(key, &value.to_string());
            }
        }
        let uri = match url.query() {
            Some(query) => format!("{}?{query}", url.path()),
            None => url.path().into(),
        };
        // Only retry an authentication challenge. Never retry a timed-out command.
        for attempt in 0..2 {
            let mut request = self.client.get(url.clone());
            if let Some(challenge) = self.challenge.as_mut() {
                let context = AuthContext::new("admin", &self.password, &uri);
                request = request.header(
                    header::AUTHORIZATION,
                    challenge.respond(&context)?.to_string(),
                );
            }
            let response = request
                .send()
                .await
                .context("Shelly RPC connection failed")?;
            if response.status() == StatusCode::UNAUTHORIZED && attempt == 0 {
                ensure!(
                    !self.password.is_empty(),
                    "device requires its admin password"
                );
                let challenge = response
                    .headers()
                    .get(header::WWW_AUTHENTICATE)
                    .context("device omitted authentication challenge")?
                    .to_str()?;
                self.challenge =
                    Some(digest_auth::parse(challenge).context("invalid digest challenge")?);
                continue;
            }
            let value: Value = response
                .error_for_status()
                .context("Shelly RPC HTTP error")?
                .json()
                .await?;
            if let Some(error) = value.get("error") {
                bail!("Shelly rejected RPC: {error}");
            }
            if value.get("code").is_some() && value.get("message").is_some() {
                bail!("Shelly rejected RPC: {value}");
            }
            return Ok(value);
        }
        bail!("device authentication failed")
    }

    pub async fn verify_device(&mut self) -> Result<()> {
        let info = self.call("Shelly.GetDeviceInfo", json!({})).await?;
        ensure!(
            info["gen"] == 3 && info["app"] == "DimmerG3",
            "host is not a Shelly Dimmer Gen3"
        );
        Ok(())
    }

    pub async fn status(&mut self) -> Result<Value> {
        state_from_status(&self.call("Light.GetStatus", json!({"id": 0})).await?)
    }
    pub async fn set(&mut self, payload: &Value) -> Result<Value> {
        self.call("Light.Set", command_params(payload)?).await?;
        // Read hardware state; a successful request alone is not the resulting state.
        self.status().await
    }
}

pub fn command_params(payload: &Value) -> Result<Value> {
    let object = payload
        .as_object()
        .context("command must be a JSON object")?;
    let mut params = json!({"id": 0});
    if let Some(on) = object.get("on") {
        params["on"] = json!(on.as_bool().context("on must be boolean")?);
    }
    if let Some(brightness) = object.get("brightness") {
        let brightness = brightness
            .as_u64()
            .context("brightness must be an integer from 0 to 100")?;
        ensure!(brightness <= 100, "brightness must be from 0 to 100");
        params["brightness"] = json!(brightness);
    }
    ensure!(
        params.get("on").is_some() || params.get("brightness").is_some(),
        "command needs on or brightness"
    );
    Ok(params)
}

pub fn state_from_status(status: &Value) -> Result<Value> {
    let on = status["output"].as_bool().context("status lacks output")?;
    let brightness = status["brightness"]
        .as_f64()
        .context("status lacks brightness")?;
    ensure!(
        brightness.is_finite() && (0.0..=100.0).contains(&brightness),
        "invalid device brightness"
    );
    Ok(json!({"on": on, "brightness": brightness.round() as u64}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::Query, routing::get, Json, Router};
    use std::collections::HashMap;

    #[test]
    fn commands_validate_types_and_preserve_explicit_power() {
        assert_eq!(
            command_params(&json!({"on": false, "brightness": 70})).unwrap(),
            json!({"id":0,"on":false,"brightness":70})
        );
        assert_eq!(
            command_params(&json!({"brightness": 0})).unwrap(),
            json!({"id":0,"brightness":0})
        );
        for bad in [
            json!({}),
            json!({"on":"true"}),
            json!({"brightness":101}),
            json!({"brightness":-1}),
            json!({"brightness":1.5}),
        ] {
            assert!(command_params(&bad).is_err());
        }
    }
    #[test]
    fn off_light_keeps_its_reported_brightness() {
        assert_eq!(
            state_from_status(&json!({"output":false,"brightness":75,"source":"button"})).unwrap(),
            json!({"on":false,"brightness":75})
        );
        assert!(state_from_status(&json!({"output":false})).is_err());
    }
    #[test]
    fn rejects_hosts_with_paths_or_credentials() {
        for host in [
            "",
            "http://192.168.1.1",
            "user:secret@192.168.1.1",
            "192.168.1.1/path",
            "192.168.1.1?foo",
        ] {
            assert!(base_url(host).is_err(), "accepted {host}");
        }
        assert!(base_url("192.168.1.1:8080").is_ok());
    }
    #[tokio::test]
    async fn controls_and_reads_back_over_real_http() {
        let app = Router::new()
            .route(
                "/rpc/Shelly.GetDeviceInfo",
                get(|| async { Json(json!({"gen":3,"app":"DimmerG3"})) }),
            )
            .route(
                "/rpc/Light.Set",
                get(|Query(q): Query<HashMap<String, String>>| async move {
                    assert_eq!(q.get("id").unwrap(), "0");
                    assert_eq!(q.get("brightness").unwrap(), "60");
                    Json(json!({"was_on":false}))
                }),
            )
            .route(
                "/rpc/Light.GetStatus",
                get(|| async { Json(json!({"output":true,"brightness":60})) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut client = ShellyClient::new(&host, "").unwrap();
        client.verify_device().await.unwrap();
        assert_eq!(
            client.set(&json!({"brightness":60})).await.unwrap(),
            json!({"on":true,"brightness":60})
        );
        server.abort();
    }
    #[tokio::test]
    async fn digest_authenticates_and_reuses_nonce() {
        use axum::http::HeaderMap;
        use sha2::{Digest, Sha256};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let challenges = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let challenge_count = challenges.clone();
        let request_count = requests.clone();
        let app = Router::new().route("/rpc/Light.GetStatus", get(move |headers: HeaderMap| {
            let challenges = challenge_count.clone();
            let requests = request_count.clone();
            async move {
                let Some(header) = headers.get(header::AUTHORIZATION) else {
                    challenges.fetch_add(1, Ordering::SeqCst);
                    return (StatusCode::UNAUTHORIZED, [(header::WWW_AUTHENTICATE,
                        "Digest realm=\"shelly-test\", nonce=\"test-nonce\", algorithm=SHA-256, qop=\"auth\"")], Json(json!({})));
                };
                let auth = digest_auth::AuthorizationHeader::parse(header.to_str().unwrap()).unwrap();
                let count = requests.fetch_add(1, Ordering::SeqCst) + 1;
                assert_eq!(auth.nc as usize, count);
                assert_eq!(auth.uri, "/rpc/Light.GetStatus?id=0");
                let hash = |s: &str| format!("{:x}", Sha256::digest(s.as_bytes()));
                let ha1 = hash("admin:shelly-test:secret");
                let ha2 = hash("GET:/rpc/Light.GetStatus?id=0");
                let expected = hash(&format!("{ha1}:test-nonce:{:08x}:{}:auth:{ha2}", auth.nc, auth.cnonce.unwrap()));
                assert_eq!(auth.response, expected);
                (StatusCode::OK, [(header::WWW_AUTHENTICATE, "")], Json(json!({"output":false,"brightness":40})))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut client = ShellyClient::new(&host, "secret").unwrap();
        client.status().await.unwrap();
        client.status().await.unwrap();
        assert_eq!(challenges.load(Ordering::SeqCst), 1);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        assert!(ShellyClient::new(&host, "")
            .unwrap()
            .status()
            .await
            .unwrap_err()
            .to_string()
            .contains("requires its admin password"));
        server.abort();
    }

    #[tokio::test]
    async fn rpc_errors_do_not_become_successful_state() {
        let app = Router::new().route(
            "/rpc/Light.Set",
            get(|| async { Json(json!({"code":-109,"message":"Output not calibrated"})) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        assert!(ShellyClient::new(&host, "")
            .unwrap()
            .set(&json!({"on":true}))
            .await
            .is_err());
        server.abort();
    }
}
