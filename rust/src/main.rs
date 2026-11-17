//! 公用中转总控（地址目录）。
//!
//! 两件事：设备（插件宿主）用长连接把可用地址报上来；APP 用长连接查 / 订阅这些地址。
//! 全内存、无数据库，地址没变化时不推送，链路保持静默。

mod model;
mod state;
mod ws_app;
mod ws_device;

use axum::{
    extract::State,
    response::IntoResponse,
    routing::get,
    Json, Router,
};

use state::{AppState, Limits};

#[tokio::main]
async fn main() {
    let bind = std::env::var("BIND").unwrap_or_else(|_| "0.0.0.0:8787".to_string());

    let limits = Limits::from_env();
    let state = AppState::new(limits);

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/ws/device", get(ws_device::handler))
        .route("/ws/app", get(ws_app::handler))
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
