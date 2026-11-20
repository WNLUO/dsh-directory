//! 注册限速、容量、鉴权与持久令牌恢复的状态层验证。

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
