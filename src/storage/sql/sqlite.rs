use serde::{Deserialize, Deserializer, de::Error as _};
use serde_json::{Map, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{AssertSqlSafe, SqlitePool};
use std::{
    collections::{HashMap, HashSet},
    env,
    path::PathBuf,
    time::Duration,
};

use crate::{
    errors::RvError,
    storage::{Backend, BackendEntry},
};

const DEFAULT_SQLITE_FILENAME: &str = "vault.db";
const DEFAULT_SQLITE_TABLE: &str = "vault";
const DEFAULT_SQLITE_TIMEOUT: u64 = 7200;

#[derive(Clone, Debug)]
pub struct SqliteBackendConfig {
    filename: PathBuf,
    table: String,
    timeout: Duration,
    create_if_missing: bool,
}

impl<'de> Deserialize<'de> for SqliteBackendConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let default_cfg = Self::default();
        let deserializer_map: Map<String, Value> = <Map<String, Value>>::deserialize(deserializer)?;
        let create_if_missing: bool = deserializer_map
            .get("create_if_missing")
            .and_then(|key| {
                serde_json::from_value::<bool>(key.clone())
                    .map_err(|err| {
                        log::warn!("SQLite Backend: `create_if_missing` from value failed: {err:?}")
                    })
                    .ok()
            })
            .unwrap_or(default_cfg.create_if_missing);
        Ok(Self {
            filename: {
                let path = std::env::var("VAULT_SQLITE_FILENAME")
                    .ok()
                    .map(PathBuf::from)
                    .unwrap_or(
                        deserializer_map
                            .get("filename")
                            .and_then(|filename| {
                                serde_json::from_value::<PathBuf>(filename.clone())
                                    .map_err(|err| {
                                        log::warn!(
                                            "SQLite Backend: `filename` from value failed: {err:?}"
                                        )
                                    })
                                    .ok()
                            })
                            .unwrap_or(default_cfg.filename),
                    );
                match path.canonicalize() {
                    Ok(filename) => filename,
                    Err(_) if create_if_missing && path.is_absolute() => path,
                    Err(_) if create_if_missing => env::current_dir()
                        .map_err(|err| {
                            D::Error::custom(format!(
                                "SQLite Backend: failed to resolve current directory: {err}"
                            ))
                        })?
                        .join(path),
                    Err(err) => Err(D::Error::custom(&err))?,
                }
            },
            table: deserializer_map
                .get("table")
                .and_then(|table| {
                    serde_json::from_value::<String>(table.clone())
                        .map_err(|err| {
                            log::warn!("SQLite Backend: `table` from value failed: {err:?}")
                        })
                        .ok()
                })
                .unwrap_or(default_cfg.table),
            timeout: {
                let timeout = match std::env::var("VAULT_SQLITE_TIMEOUT")
                    .map(Value::String)
                    .ok()
                    .or(deserializer_map.get("timeout").cloned())
                {
                    Some(Value::String(duration)) => match duration.is_empty() {
                        true => default_cfg.timeout,
                        false => {
                            humantime::parse_duration(duration.trim()).map_err(D::Error::custom)?
                        }
                    },
                    Some(Value::Number(secs)) => {
                        Duration::from_secs(secs.as_u64().unwrap_or(5_u64))
                    }
                    _ => default_cfg.timeout,
                };
                match timeout.gt(&Duration::ZERO)
                    && timeout.lt(&Duration::from_secs(DEFAULT_SQLITE_TIMEOUT))
                {
                    true => timeout,
                    false => Err(D::Error::custom(format!(
                        "SQLite Backend: Timeout must be greater than 0s and less than {}s.",
                        DEFAULT_SQLITE_TIMEOUT
                    )))?,
                }
            },
            create_if_missing,
        })
    }
}

impl Default for SqliteBackendConfig {
    fn default() -> Self {
        Self {
            filename: env::temp_dir().join(DEFAULT_SQLITE_FILENAME),
            table: DEFAULT_SQLITE_TABLE.to_string(),
            timeout: Duration::new(5, 0),
            create_if_missing: true,
        }
    }
}

pub struct SqliteBackend {
    pool: SqlitePool,
    table: String,
}

impl SqliteBackend {
    pub async fn new(conf: &HashMap<String, Value>) -> Result<Self, RvError> {
        let conf: SqliteBackendConfig = serde_json::from_value(serde_json::to_value(conf)?)?;
        Self::from_config(conf).await
    }

    async fn from_config(conf: SqliteBackendConfig) -> Result<Self, RvError> {
        if conf.table.is_empty()
            || !conf
                .table
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_')
        {
            let err = RvError::ErrSqliteDisallowedFields(conf.table.clone());
            log::debug!("{err:?}");
            Err(err)?;
        }
        let opts = SqliteConnectOptions::new()
            .filename(conf.filename)
            .busy_timeout(conf.timeout)
            .create_if_missing(conf.create_if_missing)
            .read_only(false);
        log::debug!("Sqlite connect options: {:?}", opts);

        // SQLx 0.9's idle counter can transiently underflow on connection
        // return. Its reaper snapshots that count in a non-yielding loop.
        // Disable both timers so runtime shutdown cannot get stuck there.
        let pool = SqlitePoolOptions::new()
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(opts)
            .await?;
        // INVARIANT: table identifiers are validated above; values are bound.
        sqlx::query(AssertSqlSafe(format!(
            r#"CREATE TABLE IF NOT EXISTS `{}` (
    `vault_key` TEXT NOT NULL,
    `vault_value` BLOB NOT NULL,
    PRIMARY KEY (`vault_key`)
);"#,
            conf.table
        )))
        .execute(&pool)
        .await?;

        Ok(Self {
            pool,
            table: conf.table,
        })
    }
}

#[async_trait::async_trait]
impl Backend for SqliteBackend {
    async fn get(&self, key: &str) -> Result<Option<BackendEntry>, RvError> {
        #[derive(Debug, sqlx::FromRow)]
        struct SqliteBackendEntry(Vec<u8>);

        // This will change.
        if key.starts_with("/") {
            return Err(RvError::ErrSqliteBackendNotSupportAbsolute);
        }

        let sql = format!(
            "SELECT vault_value FROM `{}` WHERE vault_key = ?",
            self.table
        );
        let ret: Option<SqliteBackendEntry> = sqlx::query_as(AssertSqlSafe(sql))
            .bind(key.as_bytes())
            .fetch_optional(&self.pool)
            .await?;

        if let Some(item) = ret {
            Ok(Some(BackendEntry {
                key: key.to_string(),
                value: item.0,
            }))
        } else {
            Ok(None)
        }
    }

    async fn put(&self, entry: &BackendEntry) -> Result<(), RvError> {
        if entry.key.starts_with("/") {
            Err(RvError::ErrSqliteBackendNotSupportAbsolute)?;
        }

        let sql = format!(
            "INSERT INTO `{}` (vault_key, vault_value) VALUES (?, ?) ON CONFLICT(vault_key) DO UPDATE SET vault_value = excluded.vault_value",
            self.table
        );
        sqlx::query(AssertSqlSafe(sql))
            .bind(entry.key.as_bytes())
            .bind(&entry.value)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), RvError> {
        if key.starts_with("/") {
            Err(RvError::ErrSqliteBackendNotSupportAbsolute)?;
        }

        let sql = format!("DELETE FROM `{}` WHERE vault_key = ?", self.table);
        sqlx::query(AssertSqlSafe(sql))
            .bind(key.as_bytes())
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, RvError> {
        if prefix.starts_with("/") {
            Err(RvError::ErrSqliteBackendNotSupportAbsolute)?;
        }

        let sql = format!(
            "SELECT vault_key FROM `{}` WHERE vault_key LIKE ? ESCAPE '\\'",
            self.table
        );
        // Escape the LIKE wildcard characters (% and _) and the escape character (\)
        // so that `prefix` is treated as a literal prefix.
        let escaped_prefix = prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let keys: Vec<Vec<u8>> = sqlx::query_scalar(AssertSqlSafe(sql))
            .bind(format!("{}%", escaped_prefix).as_bytes())
            .fetch_all(&self.pool)
            .await?;
        let mut res = HashSet::new();
        for key_bytes in keys {
            let key = String::from_utf8(key_bytes)?;
            let key = key.strip_prefix(prefix).unwrap_or(&key);

            match key.find('/') {
                Some(i) => {
                    let key = &key[0..i + 1];
                    res.insert(key.to_string());
                }
                None => {
                    res.insert(key.to_string());
                }
            }
        }

        Ok(res.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_pool_lifecycle_preserves_stored_bytes_and_literal_prefixes() {
        let dir = tempfile::tempdir().unwrap();
        let config = SqliteBackendConfig {
            filename: dir.path().join("vault.db"),
            table: "vault".to_owned(),
            timeout: Duration::from_secs(5),
            create_if_missing: true,
        };
        let backend = SqliteBackend::from_config(config.clone()).await.unwrap();
        assert_eq!(backend.pool.options().get_idle_timeout(), None);
        assert_eq!(backend.pool.options().get_max_lifetime(), None);
        assert_eq!(backend.pool.options().get_max_connections(), 10);
        assert_eq!(
            backend.pool.options().get_acquire_timeout(),
            Duration::from_secs(30)
        );
        // Seed the exact schema/bindings used by libvault 0.2.x before testing
        // new CRUD, so the regression checks existing persisted vault data.
        sqlx::query("INSERT INTO vault (vault_key, vault_value) VALUES (?, ?)")
            .bind(b"old/key".as_slice())
            .bind(b"\0\xffold".as_slice())
            .execute(&backend.pool)
            .await
            .unwrap();
        assert_eq!(
            backend.get("old/key").await.unwrap().unwrap().value,
            b"\0\xffold"
        );
        for key in ["a%/one", "a_/two", "a\\/three", "abc/other", "a%/dir/leaf"] {
            backend
                .put(&BackendEntry {
                    key: key.to_owned(),
                    value: vec![0, 255, 1],
                })
                .await
                .unwrap();
        }
        let mut listed = backend.list("a%/").await.unwrap();
        listed.sort();
        assert_eq!(listed, ["dir/", "one"]);
        assert_eq!(backend.list("a_/").await.unwrap(), ["two"]);
        assert_eq!(backend.list("a\\/").await.unwrap(), ["three"]);
        backend
            .put(&BackendEntry {
                key: "old/key".to_owned(),
                value: vec![7, 0, 255],
            })
            .await
            .unwrap();
        backend.pool.close().await;
        let mut existing = config.clone();
        existing.create_if_missing = false;
        let backend = SqliteBackend::from_config(existing).await.unwrap();
        assert_eq!(
            backend.get("old/key").await.unwrap().unwrap().value,
            [7, 0, 255]
        );
        let mut reads = tokio::task::JoinSet::new();
        let backend = std::sync::Arc::new(backend);
        for _ in 0..8 {
            let backend = backend.clone();
            reads.spawn(async move {
                for _ in 0..16 {
                    assert!(backend.get("old/key").await.unwrap().is_some());
                }
            });
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(result) = reads.join_next().await {
                result.unwrap();
            }
        })
        .await
        .expect("concurrent vault reads must finish");
        backend.delete("old/key").await.unwrap();
        backend.delete("old/key").await.unwrap();
        assert!(backend.get("old/key").await.unwrap().is_none());
        assert!(backend.get("/absolute").await.is_err());
        assert!(backend.list("/absolute").await.is_err());
        assert!(backend.delete("/absolute").await.is_err());
        assert!(
            backend
                .put(&BackendEntry {
                    key: "/absolute".to_owned(),
                    value: vec![]
                })
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(10), backend.pool.close())
            .await
            .unwrap();
        let mut invalid = config.clone();
        invalid.table = "vault`; DROP TABLE vault;--".to_owned();
        assert!(SqliteBackend::from_config(invalid).await.is_err());
        let mut absent = config;
        absent.filename = dir.path().join("missing.db");
        absent.create_if_missing = false;
        assert!(SqliteBackend::from_config(absent.clone()).await.is_err());
        assert!(!absent.filename.exists());
    }
}
