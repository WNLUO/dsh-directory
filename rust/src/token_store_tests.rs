//! 令牌日志的重启读取、写入失败、压缩和未完成尾记录验证。

use super::*;

fn path() -> PathBuf {
    std::env::temp_dir()
        .join(format!("dsh-directory-tokens-{}", uuid::Uuid::new_v4()))
        .join("tokens.jsonl")
}

fn entry(token: &str, last_seen: u64) -> TokenEntry {
    TokenEntry {
        token: token.into(),
        last_seen,
    }
}

#[test]
fn journal_replays_tokens_updates_and_removals() {
    let path = path();
    let (mut store, initial) = TokenStore::open(&path).unwrap();
    assert!(initial.is_empty());
    store
        .record("a", &entry("token-a", 100), Vec::new())
        .unwrap();
    store
        .record("b", &entry("token-b", 200), Vec::new())
        .unwrap();
    store
        .record("a", &entry("token-a", 300), vec!["b".into()])
        .unwrap();
    drop(store);
    let (store, tokens) = TokenStore::open(&path).unwrap();
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens["a"].token, "token-a");
    assert_eq!(tokens["a"].last_seen, 300);
    drop(store);
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn incomplete_tail_is_removed_before_next_registration() {
    let path = path();
    let (mut store, _) = TokenStore::open(&path).unwrap();
    store
        .record("a", &entry("token-a", 100), Vec::new())
        .unwrap();
    store
        .file
        .write_all(b"{\"instance_id\":\"unfinished")
        .unwrap();
    drop(store);
    let (mut store, tokens) = TokenStore::open(&path).unwrap();
    assert_eq!(tokens.len(), 1);
    store
        .record("b", &entry("token-b", 200), Vec::new())
        .unwrap();
    drop(store);
    let (store, tokens) = TokenStore::open(&path).unwrap();
    assert_eq!(tokens.len(), 2);
    drop(store);
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn malformed_committed_record_fails_instead_of_resetting_authentication() {
    let path = path();
    let (mut store, _) = TokenStore::open(&path).unwrap();
    store
        .record("a", &entry("token-a", 100), Vec::new())
        .unwrap();
    store.file.write_all(b"broken-json\n").unwrap();
    drop(store);
    assert!(
        matches!(TokenStore::open(&path), Err(error) if error.kind() == io::ErrorKind::InvalidData)
    );
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn compaction_keeps_current_tokens_and_accepts_more_records() {
    let path = path();
    let (mut store, _) = TokenStore::open(&path).unwrap();
    let mut tokens = HashMap::new();
    tokens.insert("a".into(), entry("token-a", 1025));
    for seen in 0..1025 {
        store
            .record("a", &entry("token-a", seen), Vec::new())
            .unwrap();
    }
    store.compact_if_needed(&tokens).unwrap();
    assert_eq!(store.records, 1);
    store
        .record("b", &entry("token-b", 2000), Vec::new())
        .unwrap();
    drop(store);
    let (store, tokens) = TokenStore::open(&path).unwrap();
    assert_eq!(tokens.len(), 2);
    assert_eq!(tokens["a"].last_seen, 1025);
    assert_eq!(tokens["b"].token, "token-b");
    drop(store);
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn failed_write_stops_further_registrations_until_reload() {
    let path = path();
    let (store, _) = TokenStore::open(&path).unwrap();
    drop(store);
    let mut store = TokenStore::read_only_for_test(&path);
    assert!(store
        .record("a", &entry("token-a", 100), Vec::new())
        .is_err());
    assert!(store.failed);
    assert!(store
        .record("b", &entry("token-b", 200), Vec::new())
        .is_err());
    drop(store);
    let (store, tokens) = TokenStore::open(&path).unwrap();
    assert!(tokens.is_empty());
    drop(store);
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[cfg(unix)]
#[test]
fn new_token_file_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let path = path();
    let (store, _) = TokenStore::open(&path).unwrap();
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    drop(store);
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}
