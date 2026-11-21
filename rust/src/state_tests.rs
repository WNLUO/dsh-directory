//! 新身份限速、死链期限、阻塞写端回收及持久令牌恢复的行为验证。

use super::*;

fn limits(max_devices: usize, reg_per_sec: f64, reg_burst: f64, max_tokens: usize) -> Limits {
    Limits {
        max_devices,
        max_tokens,
        max_conns_per_ip: 50,
        reg_per_sec,
        reg_burst,
    }
}

fn tx() -> Tx {
    let (tx, rx) = mpsc::unbounded_channel::<Message>();
    std::mem::forget(rx);
    tx
}

#[test]
fn rate_limit_rejects_after_burst() {
    let l = limits(100, 0.0, 1.0, 1000);
    let mut inner = Inner::new(1.0);
    assert!(matches!(
        inner.register("a", 1, "pa".into(), tx(), None, 1000, &l),
        RegisterOutcome::Ok { .. }
    ));
    assert!(matches!(
        inner.register("b", 2, "pb".into(), tx(), None, 1000, &l),
        RegisterOutcome::RateLimited
    ));
}

#[test]
fn over_capacity_evicts_least_recently_interested() {
    let l = limits(1, 1000.0, 1000.0, 1000);
    let mut inner = Inner::new(1000.0);
    assert!(matches!(
        inner.register("a", 1, "pa".into(), tx(), None, 100, &l),
        RegisterOutcome::Ok { .. }
    ));
    match inner.register("b", 2, "pb".into(), tx(), None, 200, &l) {
        RegisterOutcome::Ok { evicted, .. } => assert_eq!(evicted.len(), 1),
        _ => panic!("expected ok"),
    }
    assert!(inner.devices.contains_key("b"));
    assert!(!inner.devices.contains_key("a"));
}

#[test]
fn full_when_everything_is_watched() {
    let l = limits(1, 1000.0, 1000.0, 1000);
    let mut inner = Inner::new(1000.0);
    assert!(matches!(
        inner.register("a", 1, "pa".into(), tx(), None, 100, &l),
        RegisterOutcome::Ok { .. }
    ));
    inner
        .watchers
        .entry("a".into())
        .or_default()
        .insert(1, tx());
    assert!(matches!(
        inner.register("b", 2, "pb".into(), tx(), None, 200, &l),
        RegisterOutcome::Full
    ));
}

#[test]
fn re_register_requires_matching_token() {
    let l = limits(100, 1000.0, 1000.0, 1000);
    let mut inner = Inner::new(1000.0);
    let first = registered_token(inner.register("a", 1, "pa".into(), tx(), None, 100, &l));
    assert!(matches!(
        inner.register("a", 2, "pa".into(), tx(), None, 200, &l),
        RegisterOutcome::Unauthorized
    ));
    assert!(matches!(
        inner.register("a", 2, "pa".into(), tx(), Some("wrong"), 200, &l),
        RegisterOutcome::Unauthorized
    ));
    assert_eq!(
        registered_token(inner.register("a", 2, "pa".into(), tx(), Some(&first), 200, &l)),
        first
    );
}

#[test]
fn token_map_is_capped() {
    let l = limits(100, 1000.0, 1000.0, 2);
    let mut inner = Inner::new(1000.0);
    for (i, id) in ["a", "b", "c"].iter().enumerate() {
        assert!(matches!(
            inner.register(id, i as u64 + 1, "p".into(), tx(), None, 100 + i as u64, &l),
            RegisterOutcome::Ok { .. }
        ));
    }
    assert!(inner.tokens.len() <= 2, "tokens={}", inner.tokens.len());
    assert!(!inner.tokens.contains_key("a"));
    assert!(inner.tokens.contains_key("c"));
}

fn registered_token(outcome: RegisterOutcome) -> String {
    match outcome {
        RegisterOutcome::Ok { token, .. } => token,
        _ => panic!("expected successful registration"),
    }
}

fn temp_path() -> std::path::PathBuf {
    std::env::temp_dir()
        .join(format!("dsh-directory-state-{}", uuid::Uuid::new_v4()))
        .join("tokens.jsonl")
}

#[tokio::test]
async fn persistent_tokens_survive_restart_without_online_records() {
    let path = temp_path();
    let l = limits(100, 1000.0, 1000.0, 1000);
    let state = AppState::open(l.clone(), &path).unwrap();
    let first = {
        let mut inner = state.inner.write().await;
        let token =
            registered_token(inner.register("a", 1, "old-address".into(), tx(), None, 100, &l));
        inner
            .watchers
            .entry("a".into())
            .or_default()
            .insert(1, tx());
        token
    };
    drop(state);
    let state = AppState::open(l.clone(), &path).unwrap();
    assert_eq!(snapshot(&state, "a").await, (false, None));
    {
        let mut inner = state.inner.write().await;
        assert!(inner.watchers.is_empty());
        assert_eq!(
            inner.tokens["a"].token, first,
            "APP原令牌仍可通过同一张鉴权表"
        );
        assert!(matches!(
            inner.register("a", 2, "bad".into(), tx(), None, 200, &l),
            RegisterOutcome::Unauthorized
        ));
        assert!(matches!(
            inner.register("a", 2, "bad".into(), tx(), Some("forged"), 200, &l),
            RegisterOutcome::Unauthorized
        ));
        assert_eq!(
            registered_token(inner.register(
                "a",
                2,
                "new-address".into(),
                tx(),
                Some(&first),
                200,
                &l
            )),
            first
        );
    }
    assert_eq!(
        snapshot(&state, "a").await,
        (true, Some("new-address".into()))
    );
    drop(state);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn unknown_client_token_is_not_adopted_or_persisted() {
    let path = temp_path();
    let l = limits(100, 1000.0, 1000.0, 1000);
    let state = AppState::open(l.clone(), &path).unwrap();
    let first = registered_token(state.inner.write().await.register(
        "new",
        1,
        "p".into(),
        tx(),
        Some("forged"),
        100,
        &l,
    ));
    assert_ne!(first, "forged");
    drop(state);
    let state = AppState::open(l.clone(), &path).unwrap();
    let inner = state.inner.read().await;
    assert_eq!(inner.tokens["new"].token, first);
    drop(inner);
    drop(state);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn token_eviction_and_recent_registration_persist_across_restarts() {
    let path = temp_path();
    let l = limits(100, 1000.0, 1000.0, 2);
    let state = AppState::open(l.clone(), &path).unwrap();
    {
        let mut inner = state.inner.write().await;
        let a = registered_token(inner.register("a", 1, "p".into(), tx(), None, 100, &l));
        registered_token(inner.register("b", 2, "p".into(), tx(), None, 200, &l));
        registered_token(inner.register("a", 3, "p".into(), tx(), Some(&a), 300, &l));
        registered_token(inner.register("c", 4, "p".into(), tx(), None, 400, &l));
    }
    drop(state);
    let state = AppState::open(l, &path).unwrap();
    let inner = state.inner.read().await;
    assert!(inner.tokens.contains_key("a"));
    assert!(!inner.tokens.contains_key("b"));
    assert!(inner.tokens.contains_key("c"));
    drop(inner);
    drop(state);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn failed_persistence_does_not_publish_or_evict_devices() {
    let path = temp_path();
    let l = limits(1, 1000.0, 1000.0, 1000);
    let state = AppState::open(l.clone(), &path).unwrap();
    let original = registered_token(state.inner.write().await.register(
        "a",
        1,
        "p".into(),
        tx(),
        None,
        100,
        &l,
    ));
    // 只读句柄让真实write_all失败，不借助文件权限或运行用户假设。
    let store = TokenStore::read_only_for_test(&path);
    {
        let mut inner = state.inner.write().await;
        inner.token_store = Some(store);
        assert!(matches!(
            inner.register("b", 2, "p".into(), tx(), None, 200, &l),
            RegisterOutcome::StorageUnavailable
        ));
        assert_eq!(inner.tokens["a"].token, original);
        assert!(!inner.tokens.contains_key("b"));
        assert!(inner.devices.contains_key("a"));
        assert!(!inner.devices.contains_key("b"));
    }
    drop(state);
    let state = AppState::open(l, &path).unwrap();
    let inner = state.inner.read().await;
    assert_eq!(inner.tokens.len(), 1);
    assert_eq!(inner.tokens["a"].token, original);
    drop(inner);
    drop(state);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}


#[test]
fn authenticated_reconnect_does_not_consume_new_registration_budget() {
    let l = limits(100, 0.0, 1.0, 1000);
    let mut inner = Inner::new(1.0);
    let token = registered_token(inner.register("a", 1, "old".into(), tx(), None, 1000, &l));
    assert!(matches!(inner.register("a", 2, "bad".into(), tx(), Some("wrong"), 1001, &l), RegisterOutcome::Unauthorized));
    assert_eq!(registered_token(inner.register("a", 3, "new".into(), tx(), Some(&token), 1002, &l)), token);
    assert_eq!(inner.devices["a"].conn_id, 3);
    assert_eq!(inner.devices["a"].payload, "new");
    assert!(matches!(inner.register("b", 4, "other".into(), tx(), None, 1003, &l), RegisterOutcome::RateLimited));
}

#[tokio::test]
async fn unanswered_pings_expire_even_when_the_writer_queue_accepts_them() {
    let (sender, mut replies) = mpsc::unbounded_channel();
    let mut stream = futures_util::stream::pending::<Result<Message, ()>>();
    let mut last_received = tokio::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(1), receive_ws(
        &mut stream, &sender, &mut last_received, Duration::from_millis(2), Duration::from_millis(15),
    )).await.expect("死链必须有界退出");
    assert_eq!(result.unwrap_err(), "hb_timeout");
    assert!(matches!(replies.try_recv(), Ok(Message::Ping(_))));
}

#[tokio::test]
async fn incoming_pong_refreshes_the_monotonic_deadline() {
    let (sender, _replies) = mpsc::unbounded_channel();
    let mut stream = futures_util::stream::iter([Ok::<_, ()>(Message::Pong(Vec::new().into()))]);
    let mut last_received = tokio::time::Instant::now() - Duration::from_secs(1);
    let before = tokio::time::Instant::now();
    assert!(matches!(receive_ws(&mut stream, &sender, &mut last_received, Duration::from_secs(20), Duration::from_secs(60)).await, Ok(Message::Pong(_))));
    assert!(last_received >= before);
}

#[tokio::test]
async fn stopping_a_blocked_writer_drops_its_socket_task() {
    let (dropped, wait_drop) = tokio::sync::oneshot::channel();
    struct OnDrop(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for OnDrop { fn drop(&mut self) { let _ = self.0.take().unwrap().send(()); } }
    let guard = OnDrop(Some(dropped));
    let writer = tokio::spawn(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    });
    tokio::time::timeout(Duration::from_secs(1), stop_writer(writer)).await.expect("写端不能阻塞离线广播和配额释放");
    wait_drop.await.unwrap();
}


#[test]
fn old_connection_cleanup_preserves_the_replacement_address() {
    let l = limits(100, 0.0, 1.0, 1000);
    let mut inner = Inner::new(1.0);
    let token = registered_token(inner.register("a", 1, "old".into(), tx(), None, 1000, &l));
    registered_token(inner.register("a", 2, "new".into(), tx(), Some(&token), 1001, &l));
    assert_eq!(inner.disconnect_device("a", 1, true), (false, true));
    assert_eq!(inner.devices["a"].payload, "new");
    assert_eq!(inner.disconnect_device("a", 2, true), (true, false));
    assert!(!inner.devices.contains_key("a"));
}


#[tokio::test]
async fn offline_is_queued_before_replacement_update_while_old_writer_stalls() {
    let l = limits(100, 0.0, 1.0, 1000);
    let mut inner = Inner::new(1.0);
    let token = registered_token(inner.register("a", 1, "old".into(), tx(), None, 1000, &l));
    let (watcher, mut events) = mpsc::unbounded_channel();
    inner.watchers.entry("a".into()).or_default().insert(1, watcher);
    let state = AppState { inner: Arc::new(RwLock::new(inner)), limits: l.clone() };
    {
        let mut guard = state.inner.write().await;
        assert_eq!(guard.disconnect_device("a", 1, true), (true, false));
    }
    let writer = tokio::spawn(std::future::pending::<()>());
    let stopping = tokio::spawn(stop_writer(writer));
    {
        let mut guard = state.inner.write().await;
        registered_token(guard.register("a", 2, "new".into(), tx(), Some(&token), 1001, &l));
    }
    broadcast_update(&state, "a", "new").await;
    stopping.await.unwrap();
    let event = |message: Message| match message { Message::Text(text) => serde_json::from_str::<serde_json::Value>(&text).unwrap(), _ => panic!("text event") };
    assert_eq!(event(events.try_recv().expect("断开必须同步排离线"))["type"], "offline");
    assert_eq!(event(events.try_recv().unwrap())["type"], "update");
    assert!(events.try_recv().is_err(), "写端收尾不能再发迟到离线");
    {
        let mut guard = state.inner.write().await;
        assert_eq!(guard.disconnect_device("a", 1, true), (false, true));
    }
    assert!(events.try_recv().is_err(), "旧连接不能为新记录发offline");
    assert_eq!(snapshot(&state, "a").await, (true, Some("new".into())));
}

fn text_event(message: Message) -> serde_json::Value {
    match message {
        Message::Text(text) => serde_json::from_str(&text).unwrap(),
        _ => panic!("text event"),
    }
}

fn channel() -> (Tx, mpsc::UnboundedReceiver<Message>) {
    mpsc::unbounded_channel()
}

#[test]
fn ask_without_an_online_device_is_refused() {
    let mut inner = Inner::new(1000.0);
    let (asker, _asker_rx) = channel();
    assert!(!inner.ask_device("a", 1, asker));
    assert!(inner.diagnose_waiters.is_empty());
}

#[test]
fn answer_reaches_only_the_waiter_of_that_connection() {
    let l = limits(10, 1000.0, 1000.0, 1000);
    let mut inner = Inner::new(1000.0);
    let (device, mut device_rx) = channel();
    assert!(matches!(
        inner.register("a", 7, "pa".into(), device, None, 1000, &l),
        RegisterOutcome::Ok { .. }
    ));
    let (asker, mut asker_rx) = channel();
    assert!(inner.ask_device("a", 11, asker));
    assert_eq!(text_event(device_rx.try_recv().expect("设备要收到 ask"))["type"], "ask");

    // 旧连接（换了号的那条）回话不算数，等待位不动。
    assert!(!inner.answer_diagnosis("a", 8, "stale".into()));
    assert!(inner.diagnose_waiters.contains_key("a"));
    assert!(asker_rx.try_recv().is_err());

    assert!(inner.answer_diagnosis("a", 7, "sealed".into()));
    let answer = text_event(asker_rx.try_recv().expect("APP 要收到 diagnosis"));
    assert_eq!(answer["type"], "diagnosis");
    assert_eq!(answer["payload"], "sealed");
    assert!(inner.diagnose_waiters.is_empty());
    assert!(!inner.answer_diagnosis("a", 7, "again".into()));
}

#[test]
fn device_going_offline_tells_the_waiting_app() {
    let l = limits(10, 1000.0, 1000.0, 1000);
    let mut inner = Inner::new(1000.0);
    let (device, _device_rx) = channel();
    assert!(matches!(
        inner.register("a", 7, "pa".into(), device, None, 1000, &l),
        RegisterOutcome::Ok { .. }
    ));
    let (asker, mut asker_rx) = channel();
    assert!(inner.ask_device("a", 11, asker));
    assert_eq!(inner.disconnect_device("a", 7, true), (true, false));
    let event = text_event(asker_rx.try_recv().expect("等结果的那位要被告知"));
    assert_eq!(event["type"], "diagnosis_unavailable");
    assert_eq!(event["reason"], "device_offline");
    assert!(inner.diagnose_waiters.is_empty());
}

#[test]
fn closing_the_asking_connection_clears_only_its_own_slot() {
    let l = limits(10, 1000.0, 1000.0, 1000);
    let mut inner = Inner::new(1000.0);
    let (device, _device_rx) = channel();
    assert!(matches!(
        inner.register("a", 7, "pa".into(), device, None, 1000, &l),
        RegisterOutcome::Ok { .. }
    ));
    let (asker, _asker_rx) = channel();
    assert!(inner.ask_device("a", 11, asker));
    inner.drop_diagnose_waiter("a", 12);
    assert!(inner.diagnose_waiters.contains_key("a"));
    inner.drop_diagnose_waiter("a", 11);
    assert!(inner.diagnose_waiters.is_empty());
}
