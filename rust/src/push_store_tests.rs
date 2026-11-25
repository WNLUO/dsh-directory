//! 推送令牌日志的重启读取、写入失败、压缩与未完成尾记录。

use super::*;

fn path() -> PathBuf {
    std::env::temp_dir()
        .join(format!("dsh-directory-push-{}", uuid::Uuid::new_v4()))
        .join("push.jsonl")
}

fn entry(token: &str, env: &str, updated_at: u64) -> PushEntry {
    PushEntry {
        token: token.into(),
        platform: "ios".into(),
        env: env.into(),
        updated_at,
    }
}

#[test]
fn journal_replays_upserts_and_removals() {
    let path = path();
    let (mut store, initial) = PushStore::open(&path).unwrap();
    assert!(initial.is_empty());
    store
        .record("inst-a", "phone-1", &entry("t1", "production", 100), Vec::new())
        .unwrap();
    store
        .record("inst-a", "phone-2", &entry("t2", "sandbox", 200), Vec::new())
        .unwrap();
    store
        .record(
            "inst-a",
            "phone-1",
            &entry("t3", "production", 300),
            vec![("inst-a".into(), "phone-2".into())],
        )
        .unwrap();
    drop(store);
    let (store, entries) = PushStore::open(&path).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[&("inst-a".into(), "phone-1".into())].token, "t3");
    assert_eq!(entries[&("inst-a".into(), "phone-1".into())].updated_at, 300);
    drop(store);
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn same_device_on_two_instances_stays_separate() {
    let path = path();
    let (mut store, _) = PushStore::open(&path).unwrap();
    store
        .record("inst-a", "phone", &entry("ta", "production", 1), Vec::new())
        .unwrap();
    store
        .record("inst-b", "phone", &entry("tb", "production", 2), Vec::new())
        .unwrap();
    drop(store);
    let (store, entries) = PushStore::open(&path).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[&("inst-a".into(), "phone".into())].token, "ta");
    assert_eq!(entries[&("inst-b".into(), "phone".into())].token, "tb");
    drop(store);
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn incomplete_tail_is_removed_before_next_write() {
    let path = path();
    let (mut store, _) = PushStore::open(&path).unwrap();
    store
        .record("inst-a", "phone", &entry("t1", "production", 1), Vec::new())
        .unwrap();
    store.file.write_all(b"{\"instance_id\":\"unfinished").unwrap();
    drop(store);
    let (mut store, entries) = PushStore::open(&path).unwrap();
    assert_eq!(entries.len(), 1);
    store
        .record("inst-a", "phone", &entry("t2", "production", 2), Vec::new())
        .unwrap();
    drop(store);
    let (store, entries) = PushStore::open(&path).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[&("inst-a".into(), "phone".into())].token, "t2");
    drop(store);
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn malformed_committed_record_fails_instead_of_silently_dropping_tokens() {
    let path = path();
    let (mut store, _) = PushStore::open(&path).unwrap();
    store
        .record("inst-a", "phone", &entry("t1", "production", 1), Vec::new())
        .unwrap();
    store.file.write_all(b"broken-json\n").unwrap();
    drop(store);
    assert!(
        matches!(PushStore::open(&path), Err(error) if error.kind() == io::ErrorKind::InvalidData)
    );
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn compaction_keeps_current_entries_and_accepts_more_records() {
    let path = path();
    let (mut store, _) = PushStore::open(&path).unwrap();
    let mut entries = HashMap::new();
    entries.insert(
        ("inst-a".into(), "phone".into()),
        entry("t1", "production", 1025),
    );
    for at in 0..1025 {
        store
            .record("inst-a", "phone", &entry("t1", "production", at), Vec::new())
            .unwrap();
    }
    store.compact_if_needed(&entries).unwrap();
    assert_eq!(store.records, 1);
    store
        .record("inst-b", "phone", &entry("t2", "sandbox", 2000), Vec::new())
        .unwrap();
    drop(store);
    let (store, entries) = PushStore::open(&path).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[&("inst-a".into(), "phone".into())].updated_at, 1025);
    assert_eq!(entries[&("inst-b".into(), "phone".into())].token, "t2");
    drop(store);
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn failed_write_stops_further_writes_until_reload() {
    let path = path();
    let (store, _) = PushStore::open(&path).unwrap();
    drop(store);
    let mut store = PushStore::read_only_for_test(&path);
    assert!(store
        .record("inst-a", "phone", &entry("t1", "production", 1), Vec::new())
        .is_err());
    assert!(store.failed);
    assert!(store
        .record("inst-a", "phone", &entry("t2", "production", 2), Vec::new())
        .is_err());
    drop(store);
    let (store, entries) = PushStore::open(&path).unwrap();
    assert!(entries.is_empty());
    drop(store);
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[cfg(unix)]
#[test]
fn new_push_file_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let path = path();
    let (store, _) = PushStore::open(&path).unwrap();
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    drop(store);
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn removal_record_keeps_a_dead_token_deleted_across_restart() {
    let path = path();
    let (mut store, _) = PushStore::open(&path).unwrap();
    store
        .record("inst-a", "phone", &entry("t1", "production", 1), Vec::new())
        .unwrap();
    store.record_removal("inst-a", "phone").unwrap();
    drop(store);
    let (store, entries) = PushStore::open(&path).unwrap();
    assert!(entries.is_empty(), "删除记录必须能重放，否则死令牌会回来");
    drop(store);
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}
