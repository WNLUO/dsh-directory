//! 设备令牌的追加日志：先持久化注册结果，再允许发布在线地址。

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
pub struct TokenEntry {
    pub token: String,
    pub last_seen: u64,
}

#[derive(Deserialize, Serialize)]
struct Record {
    instance_id: String,
    entry: TokenEntry,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    removed: Vec<String>,
}

pub struct TokenStore {
    path: PathBuf,
    file: File,
    records: usize,
    failed: bool,
}

impl TokenStore {
    #[cfg(test)]
    pub fn read_only_for_test(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            file: File::open(path).unwrap(),
            records: 0,
            failed: false,
        }
    }

    pub fn open(path: &Path) -> io::Result<(Self, HashMap<String, TokenEntry>)> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let file = private_file(path, false)?;
        let mut tokens = HashMap::new();
        let mut reader = BufReader::new(file.try_clone()?);
        let mut line = Vec::new();
        let mut complete_bytes = 0;
        let mut records = 0;
        loop {
            line.clear();
            if reader.read_until(b'\n', &mut line)? == 0 {
                break;
            }
            // 没有换行的末条未完成写入不可能收到注册成功回执。
            if line.last() != Some(&b'\n') {
                file.set_len(complete_bytes)?;
                file.sync_data()?;
                break;
            }
            let record: Record = serde_json::from_slice(&line)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            for id in record.removed {
                tokens.remove(&id);
            }
            tokens.insert(record.instance_id, record.entry);
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
            tokens,
        ))
    }

    pub fn record(
        &mut self,
        instance_id: &str,
        entry: &TokenEntry,
        removed: Vec<String>,
    ) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other(
                "token store requires restart after a write failure",
            ));
        }
        let mut bytes = serde_json::to_vec(&Record {
            instance_id: instance_id.to_string(),
            entry: entry.clone(),
            removed,
        })?;
        bytes.push(b'\n');
        if let Err(error) = self
            .file
            .write_all(&bytes)
            .and_then(|_| self.file.sync_data())
        {
            self.failed = true;
            return Err(error);
        }
        self.records += 1;
        Ok(())
    }

    /// 重连也会更新最近注册时间；定期压成当前令牌表，日志不随重连次数无限增长。
    pub fn compact_if_needed(&mut self, tokens: &HashMap<String, TokenEntry>) -> io::Result<()> {
        if self.records <= tokens.len().saturating_mul(2).max(1024) {
            return Ok(());
        }
        let temporary = self.path.with_extension("tmp");
        let mut next = private_file(&temporary, true)?;
        for (instance_id, entry) in tokens {
            serde_json::to_writer(
                &mut next,
                &Record {
                    instance_id: instance_id.clone(),
                    entry: entry.clone(),
                    removed: Vec::new(),
                },
            )?;
            next.write_all(b"\n")?;
        }
        next.sync_all()?;
        fs::rename(&temporary, &self.path)?;
        self.file = next;
        self.records = tokens.len();
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
#[path = "token_store_tests.rs"]
mod tests;
