//! 总控入口：加载持久令牌，启动只在内存保留在线地址的目录服务，并挂上苹果离线推送。
//!
//! 两件事：设备（插件宿主）用长连接把可用地址报上来；APP 用长连接查 / 订阅这些地址。
//! 在线地址无数据库，地址没变化时不推送，链路保持静默。
//!
//! 另外两个 HTTP 入口负责离线推送：APP 报 APNs 令牌、插件请求发一条通知。APNs 密钥只配在
//! 这一台服务器上（没配就是关的），中转与自建中转都不用管推送。

mod model;
mod push;
mod push_store;
mod state;
mod token_store;
mod ws_app;
mod ws_device;

use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};

use state::{AppState, Limits};

#[tokio::main]
async fn main() {
    let bind = std::env::var("BIND").unwrap_or_else(|_| "0.0.0.0:8787".to_string());

    let limits = Limits::from_env();
    let token_path = std::env::var_os("TOKEN_STORE_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| "data/tokens.jsonl".into());
    let push_path = std::env::var_os("PUSH_STORE_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| "data/push-tokens.jsonl".into());
    let state = match AppState::open(limits, &token_path, Some(&push_path)) {
        Ok(state) => state,
        Err(error) => {
            eprintln!(
                "dsh-directory: open token store {} / push store {} failed: {error}",
                token_path.display(),
                push_path.display()
            );
            std::process::exit(1);
        }
    };
    push::init_from_env();

    // 定期清理长期无活动的「未授权注册失败」IP 额度桶，防止被轮换 IP 撑大内存。
    {
        let sweep_state = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(300));
            loop {
                tick.tick().await;
                let mut guard = sweep_state.inner.write().await;
                guard.sweep_reg_fails(&sweep_state.limits, std::time::Instant::now());
                guard.sweep_push_buckets(std::time::Duration::from_secs(600));
            }
        });
    }

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/ws/device", get(ws_device::handler))
        .route("/ws/app", get(ws_app::handler))
        .merge(push::routes())
        .with_state(state);

    let listener = match tokio::net::TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("dsh-directory: bind {bind} failed: {e}");
            std::process::exit(1);
        }
    };
    println!(
        "dsh-directory listening on http://{}",
        listener.local_addr().map(|a| a.to_string()).unwrap_or(bind)
    );

    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        eprintln!("dsh-directory: serve error: {e}");
    }
}

async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    let g = state.inner.read().await;
    let watchers: usize = g.watchers.values().map(|m| m.len()).sum();
    Json(serde_json::json!({
        "ok": true,
        "devices": g.devices.len(),
        "watchers": watchers,
        "push": push::enabled(),
    }))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
