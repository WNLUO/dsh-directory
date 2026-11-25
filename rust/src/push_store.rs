//! 推送令牌的追加日志：先落盘，再对外说「这台手机已登记」。
//!
//! 与设备令牌那份同一个形状（`token_store.rs`）：一行一条记录，重放得到当前表；
//! 定期压成当前快照，日志不随上报次数无限增长。区别只在键是
//! `(instanceId, deviceId)` —— 同一台电脑可以被多台手机登记。

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

/// 一台手机在某个实例上的推送令牌。
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PushEntry {
    pub token: String,
    pub platform: String,
    /// 这条令牌属于苹果的哪个环境：`production` / `sandbox`。
    pub env: String,
    pub updated_at: u64,
}

pub type PushKey = (String, String);

#[derive(Clone, Deserialize, Serialize)]
struct Key {
    instance_id: String,
    device_id: String,
}

#[derive(Deserialize, Serialize)]
struct Record {
    instance_id: String,
    device_id: String,
    /// `None` = 这条是删除记录（苹果说这枚令牌失效了）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entry: Option<PushEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    removed: Vec<Key>,
}

pub struct PushStore {
    path: PathBuf,
    file: File,
    records: usize,
    failed: bool,
}

impl PushStore {
    #[cfg(test)]
    pub fn read_only_for_test(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            file: File::open(path).unwrap(),
            records: 0,
            failed: false,
        }
    }

    pub fn open(path: &Path) -> io::Result<(Self, HashMap<PushKey, PushEntry>)> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let file = private_file(path, false)?;
        let mut entries = HashMap::new();
        let mut reader = BufReader::new(file.try_clone()?);
        let mut line = Vec::new();
        let mut complete_bytes = 0;
        let mut records = 0;
        loop {
            line.clear();
            if reader.read_until(b'\n', &mut line)? == 0 {
                break;
            }
            // 没有换行的末条是没写完的半条：截掉，它不可能对应一次成功的上报。
            if line.last() != Some(&b'\n') {
                file.set_len(complete_bytes)?;
                file.sync_data()?;
                break;
            }
            let record: Record = serde_json::from_slice(&line)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            for key in record.removed {
                entries.remove(&(key.instance_id, key.device_id));
            }
            let key = (record.instance_id, record.device_id);
            match record.entry {
                Some(entry) => {
                    entries.insert(key, entry);
                }
                // 删除记录：这条令牌没了，重启后也别再回来。
                None => {
                    entries.remove(&key);
                }
            }
            complete_bytes += line.len() as u64;
            records += 1;
        }
        Ok((
            Self {
                path: path.to_path_buf(),
                file,
                records,
                failed: false,
            },
            entries,
        ))
    }

    pub fn record(
        &mut self,
        instance_id: &str,
        device_id: &str,
        entry: &PushEntry,
        removed: Vec<PushKey>,
    ) -> io::Result<()> {
        let mut bytes = serde_json::to_vec(&Record {
            instance_id: instance_id.to_string(),
            device_id: device_id.to_string(),
            entry: Some(entry.clone()),
            removed: removed
                .into_iter()
                .map(|(instance_id, device_id)| Key {
                    instance_id,
                    device_id,
                })
                .collect(),
        })?;
        bytes.push(b'\n');
        self.write(&bytes)
    }

    /// 摘掉一条令牌。删除也要过日志，否则重启后这枚死令牌又回来了。
    pub fn record_removal(&mut self, instance_id: &str, device_id: &str) -> io::Result<()> {
        let mut bytes = serde_json::to_vec(&Record {
            instance_id: instance_id.to_string(),
            device_id: device_id.to_string(),
            entry: None,
            removed: Vec::new(),
        })?;
        bytes.push(b'\n');
        self.write(&bytes)
    }

    fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other(
                "push store requires restart after a write failure",
            ));
        }
        if let Err(error) = self.file.write_all(bytes).and_then(|_| self.file.sync_data()) {
            self.failed = true;
            return Err(error);
        }
        self.records += 1;
        Ok(())
    }

    /// 定期压成当前表：日志只该随「真正变了的令牌」长，不该随每次重报长。
    pub fn compact_if_needed(
        &mut self,
        entries: &HashMap<PushKey, PushEntry>,
    ) -> io::Result<()> {
        if self.records <= entries.len().saturating_mul(2).max(1024) {
            return Ok(());
        }
        let temporary = self.path.with_extension("tmp");
        let mut next = private_file(&temporary, true)?;
        for ((instance_id, device_id), entry) in entries {
            serde_json::to_writer(
                &mut next,
                &Record {
                    instance_id: instance_id.clone(),
                    device_id: device_id.clone(),
                    entry: Some(entry.clone()),
                    removed: Vec::new(),
                },
            )?;
            next.write_all(b"\n")?;
        }
        next.sync_all()?;
        fs::rename(&temporary, &self.path)?;
        self.file = next;
        self.records = entries.len();
        Ok(())
    }
}

fn private_file(path: &Path, truncate: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    if truncate {
        options.truncate(true);
    } else {
        options.append(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

#[cfg(test)]
#[path = "push_store_tests.rs"]
mod tests;
