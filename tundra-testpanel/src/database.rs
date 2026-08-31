use anyhow::Context;
use rusqlite::{Connection, OptionalExtension, params};
use std::{collections::HashMap, path::Path};
use tundra_common::{
    hash::Hash32,
    state::{AclEntry, MAX_SERVER_IDX, NodeEntry, PortSpec, ServerEntry, Snapshot},
};

const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    uuid         TEXT PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    host         TEXT NOT NULL,
    tunnel_port  INTEGER NOT NULL,
    token_sha256 BLOB NOT NULL,
    cert_sha256  BLOB,
    last_seen    INTEGER
);
CREATE INDEX IF NOT EXISTS nodes_token ON nodes(token_sha256);

CREATE TABLE IF NOT EXISTS servers (
    uuid          TEXT PRIMARY KEY,
    idx           INTEGER NOT NULL UNIQUE,
    node_uuid     TEXT NOT NULL REFERENCES nodes(uuid) ON DELETE CASCADE,
    name          TEXT NOT NULL UNIQUE,
    -- newline separated, since a hostname can never contain one
    aliases       TEXT NOT NULL DEFAULT '',
    container_ref TEXT NOT NULL,
    dial_addr     TEXT
);

CREATE TABLE IF NOT EXISTS ports (
    server_uuid TEXT NOT NULL REFERENCES servers(uuid) ON DELETE CASCADE,
    port        INTEGER NOT NULL,
    proto       TEXT NOT NULL,
    PRIMARY KEY (server_uuid, port)
);

CREATE TABLE IF NOT EXISTS acls (
    src_server TEXT NOT NULL REFERENCES servers(uuid) ON DELETE CASCADE,
    dst_server TEXT NOT NULL REFERENCES servers(uuid) ON DELETE CASCADE,
    PRIMARY KEY (src_server, dst_server)
);
"#;

fn read_epoch(conn: &Connection) -> rusqlite::Result<u64> {
    let raw: Option<String> = conn
        .query_row("SELECT value FROM meta WHERE key = 'epoch'", [], |r| {
            r.get(0)
        })
        .optional()?;
    Ok(raw.and_then(|s| s.parse().ok()).unwrap_or(0))
}

fn bump_epoch(conn: &Connection) -> rusqlite::Result<u64> {
    let next = read_epoch(conn)? + 1;
    conn.execute(
        "INSERT INTO meta(key, value) VALUES('epoch', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![next.to_string()],
    )?;

    Ok(next)
}

// reusing gaps, so churn does not exhaust the 127.0.x.y space
fn next_free_idx(conn: &Connection) -> rusqlite::Result<Option<u16>> {
    let mut stmt = conn.prepare("SELECT idx FROM servers ORDER BY idx")?;
    let mut rows = stmt.query([])?;
    let mut expect = 0;
    while let Some(r) = rows.next()? {
        let idx: u16 = r.get(0)?;
        if idx != expect {
            break;
        }
        expect += 1;
    }

    Ok((expect <= MAX_SERVER_IDX).then_some(expect))
}

#[inline]
fn parse_uuid(s: String) -> rusqlite::Result<uuid::Uuid> {
    s.parse().map_err(|_| {
        rusqlite::Error::InvalidColumnType(0, "uuid".into(), rusqlite::types::Type::Text)
    })
}

#[derive(Debug, Clone)]
pub struct NodeRow {
    pub uuid: uuid::Uuid,
    pub name: String,
    pub host: String,
    pub tunnel_port: u16,

    pub cert_sha256: Option<Hash32>,
    pub last_seen: Option<i64>,
}

pub struct Database {
    conn: parking_lot::Mutex<Connection>,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self, anyhow::Error> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).ok();
        }

        let conn = Connection::open(path).context(format!("failed to open {}", path.display()))?;
        conn.execute_batch(SCHEMA)?;

        Ok(Self {
            conn: parking_lot::Mutex::new(conn),
        })
    }

    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self, anyhow::Error> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: parking_lot::Mutex::new(conn),
        })
    }

    // so the guard can never be held across an await
    #[inline]
    fn locked<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, anyhow::Error>,
    ) -> Result<T, anyhow::Error> {
        let conn = self.conn.lock();

        f(&conn)
    }

    #[inline]
    pub fn epoch(&self) -> Result<u64, anyhow::Error> {
        self.locked(|c| Ok(read_epoch(c)?))
    }

    pub fn create_node(
        &self,
        name: &str,
        host: &str,
        tunnel_port: u16,
    ) -> Result<(uuid::Uuid, String), anyhow::Error> {
        let uuid = uuid::Uuid::new_v4();
        let token = hex::encode(crate::keys::random_bytes::<32>());
        let digest = tundra_common::hash::sha256(token.as_bytes());

        self.locked(|c| {
            c.execute(
                "INSERT INTO nodes(uuid, name, host, tunnel_port, token_sha256)
                 VALUES(?1, ?2, ?3, ?4, ?5)",
                params![uuid.to_string(), name, host, tunnel_port, digest.0],
            )?;
            Ok(bump_epoch(c)?)
        })?;

        Ok((uuid, token))
    }

    pub fn rotate_node(&self, uuid: &uuid::Uuid) -> Result<String, anyhow::Error> {
        let token = hex::encode(crate::keys::random_bytes::<32>());
        let digest = tundra_common::hash::sha256(token.as_bytes());

        let changed = self.locked(|c| {
            let n = c.execute(
                "UPDATE nodes SET token_sha256 = ?2, cert_sha256 = NULL WHERE uuid = ?1",
                params![uuid.to_string(), digest.0],
            )?;
            if n > 0 {
                bump_epoch(c)?;
            }
            Ok::<_, anyhow::Error>(n)
        })?;

        if changed == 0 {
            return Err(anyhow::anyhow!("no such node {uuid}"));
        }

        Ok(token)
    }

    pub fn delete_node(&self, uuid: &uuid::Uuid) -> Result<bool, anyhow::Error> {
        let changed = self.locked(|c| {
            let n = c.execute(
                "DELETE FROM nodes WHERE uuid = ?1",
                params![uuid.to_string()],
            )?;
            if n > 0 {
                bump_epoch(c)?;
            }
            Ok::<_, anyhow::Error>(n)
        })?;

        Ok(changed > 0)
    }

    pub fn node_by_token(&self, token: &str) -> Result<Option<uuid::Uuid>, anyhow::Error> {
        let digest = tundra_common::hash::sha256(token.as_bytes());
        self.locked(|c| {
            Ok(c.query_row(
                "SELECT uuid FROM nodes WHERE token_sha256 = ?1",
                params![digest.0],
                |r| r.get::<_, String>(0),
            )
            .optional()?)
        })
        .map(|opt| opt.and_then(|s| s.parse().ok()))
    }

    pub fn store_node_cert(
        &self,
        uuid: &uuid::Uuid,
        cert_sha256: Hash32,
    ) -> Result<(), anyhow::Error> {
        self.locked(|c| {
            c.execute(
                "UPDATE nodes SET cert_sha256 = ?2 WHERE uuid = ?1",
                params![uuid.to_string(), cert_sha256.0],
            )?;
            Ok(bump_epoch(c)?)
        })?;

        Ok(())
    }

    pub fn touch_node(&self, uuid: &uuid::Uuid, at: i64) -> Result<(), anyhow::Error> {
        self.locked(|c| {
            Ok(c.execute(
                "UPDATE nodes SET last_seen = ?2 WHERE uuid = ?1",
                params![uuid.to_string(), at],
            )?)
        })?;

        Ok(())
    }

    pub fn list_nodes(&self) -> Result<Vec<NodeRow>, anyhow::Error> {
        self.locked(|c| {
            let mut stmt = c.prepare(
                "SELECT uuid, name, host, tunnel_port, cert_sha256, last_seen
                 FROM nodes ORDER BY name",
            )?;
            let rows = stmt.query_map([], |r| {
                Ok(NodeRow {
                    uuid: parse_uuid(r.get::<_, String>(0)?)?,
                    name: r.get(1)?,
                    host: r.get(2)?,
                    tunnel_port: r.get(3)?,
                    cert_sha256: r.get::<_, Option<[u8; 32]>>(4)?.map(Hash32),
                    last_seen: r.get(5)?,
                })
            })?;

            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    pub fn create_server(
        &self,
        node_uuid: &uuid::Uuid,
        name: &str,
        aliases: &[String],
        container_ref: &str,
        dial_addr: Option<&str>,
        ports: &[PortSpec],
    ) -> Result<ServerEntry, anyhow::Error> {
        if ports.is_empty() {
            return Err(anyhow::anyhow!("a server needs at least one port"));
        }

        let uuid = uuid::Uuid::new_v4();

        let idx = self.locked(|c| {
            let exists: bool = c
                .query_row(
                    "SELECT 1 FROM nodes WHERE uuid = ?1",
                    params![node_uuid.to_string()],
                    |_| Ok(true),
                )
                .optional()?
                .unwrap_or(false);
            if !exists {
                return Err(anyhow::anyhow!("no such node {node_uuid}"));
            }

            let Some(idx) = next_free_idx(c)? else {
                return Err(anyhow::anyhow!(
                    "server index space exhausted at {MAX_SERVER_IDX}"
                ));
            };

            c.execute(
                "INSERT INTO servers(uuid, idx, node_uuid, name, aliases, container_ref, dial_addr)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    uuid.to_string(),
                    idx,
                    node_uuid.to_string(),
                    name,
                    aliases.join("\n"),
                    container_ref,
                    dial_addr
                ],
            )?;
            for p in ports {
                c.execute(
                    "INSERT INTO ports(server_uuid, port, proto) VALUES(?1, ?2, ?3)",
                    params![uuid.to_string(), p.port, p.proto.to_str()],
                )?;
            }

            bump_epoch(c)?;

            Ok::<_, anyhow::Error>(idx)
        })?;

        Ok(ServerEntry {
            uuid,
            idx,
            node_uuid: *node_uuid,
            name: name.into(),
            aliases: aliases.to_vec(),
            container_ref: container_ref.into(),
            dial_addr: dial_addr.map(Into::into),
            ports: ports.to_vec(),
        })
    }

    pub fn delete_server(&self, uuid: &uuid::Uuid) -> Result<bool, anyhow::Error> {
        let changed = self.locked(|c| {
            let n = c.execute(
                "DELETE FROM servers WHERE uuid = ?1",
                params![uuid.to_string()],
            )?;
            if n > 0 {
                bump_epoch(c)?;
            }
            Ok::<_, anyhow::Error>(n)
        })?;

        Ok(changed > 0)
    }

    pub fn create_acl(&self, src: &uuid::Uuid, dst: &uuid::Uuid) -> Result<(), anyhow::Error> {
        if src == dst {
            return Err(anyhow::anyhow!(
                "a server does not need an ACL to reach itself"
            ));
        }

        self.locked(|c| {
            c.execute(
                "INSERT OR IGNORE INTO acls(src_server, dst_server) VALUES(?1, ?2)",
                params![src.to_string(), dst.to_string()],
            )?;
            Ok(bump_epoch(c)?)
        })?;

        Ok(())
    }

    pub fn delete_acl(&self, src: &uuid::Uuid, dst: &uuid::Uuid) -> Result<bool, anyhow::Error> {
        let changed = self.locked(|c| {
            let n = c.execute(
                "DELETE FROM acls WHERE src_server = ?1 AND dst_server = ?2",
                params![src.to_string(), dst.to_string()],
            )?;
            if n > 0 {
                bump_epoch(c)?;
            }
            Ok::<_, anyhow::Error>(n)
        })?;

        Ok(changed > 0)
    }

    pub fn snapshot(&self, jwt_pubkey: Hash32) -> Result<Snapshot, anyhow::Error> {
        self.locked(|c| {
            let epoch = read_epoch(c)?;

            let mut stmt = c.prepare(
                "SELECT uuid, name, host, tunnel_port, cert_sha256 FROM nodes ORDER BY uuid",
            )?;
            let nodes = stmt
                .query_map([], |r| {
                    Ok(NodeEntry {
                        uuid: parse_uuid(r.get::<_, String>(0)?)?,
                        name: r.get(1)?,
                        host: r.get(2)?,
                        tunnel_port: r.get(3)?,
                        cert_sha256: r.get::<_, Option<[u8; 32]>>(4)?.map(Hash32),
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            let mut stmt = c.prepare("SELECT server_uuid, port, proto FROM ports")?;
            let mut ports: HashMap<uuid::Uuid, Vec<PortSpec>> = HashMap::new();
            let mut rows = stmt.query([])?;
            while let Some(r) = rows.next()? {
                let owner = parse_uuid(r.get::<_, String>(0)?)?;
                let proto = r.get::<_, String>(2)?.parse()?;
                ports.entry(owner).or_default().push(PortSpec {
                    port: r.get(1)?,
                    proto,
                });
            }

            let mut stmt = c.prepare(
                "SELECT uuid, idx, node_uuid, name, aliases, container_ref, dial_addr
                 FROM servers ORDER BY idx",
            )?;
            let servers = stmt
                .query_map([], |r| {
                    let uuid = parse_uuid(r.get::<_, String>(0)?)?;
                    Ok(ServerEntry {
                        uuid,
                        idx: r.get(1)?,
                        node_uuid: parse_uuid(r.get::<_, String>(2)?)?,
                        name: r.get(3)?,
                        aliases: r.get::<_, String>(4)?.lines().map(str::to_owned).collect(),
                        container_ref: r.get(5)?,
                        dial_addr: r.get(6)?,
                        ports: ports.remove(&uuid).unwrap_or_default(),
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            let mut stmt = c.prepare("SELECT src_server, dst_server FROM acls")?;
            let acls = stmt
                .query_map([], |r| {
                    Ok(AclEntry {
                        src_server: parse_uuid(r.get::<_, String>(0)?)?,
                        dst_server: parse_uuid(r.get::<_, String>(1)?)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            drop(stmt);

            Ok(Snapshot {
                epoch,
                jwt_pubkey,
                nodes,
                servers,
                acls,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tundra_common::state::Proto;

    fn db() -> Database {
        Database::open_in_memory().unwrap()
    }

    fn port(p: u16) -> Vec<PortSpec> {
        vec![PortSpec {
            port: p,
            proto: Proto::Both,
        }]
    }

    fn node(db: &Database, name: &str) -> uuid::Uuid {
        db.create_node(name, "10.0.0.1", 7100).unwrap().0
    }

    // Database::epoch

    #[test]
    fn every_mutation_bumps_the_epoch_and_reads_do_not() {
        let db = db();
        assert_eq!(db.epoch().unwrap(), 0);

        let n = node(&db, "a");
        let after_node = db.epoch().unwrap();
        assert!(after_node > 0);

        db.snapshot(Hash32([0; 32])).unwrap();
        db.list_nodes().unwrap();
        assert_eq!(db.epoch().unwrap(), after_node);

        let s1 = db
            .create_server(&n, "s1", &[], "c1", None, &port(25565))
            .unwrap();
        let s2 = db
            .create_server(&n, "s2", &[], "c2", None, &port(25565))
            .unwrap();
        assert!(db.epoch().unwrap() > after_node);

        let before = db.epoch().unwrap();
        db.create_acl(&s1.uuid, &s2.uuid).unwrap();
        assert_eq!(db.epoch().unwrap(), before + 1);

        db.delete_acl(&s1.uuid, &s2.uuid).unwrap();
        assert_eq!(db.epoch().unwrap(), before + 2);

        db.delete_acl(&s1.uuid, &s2.uuid).unwrap();
        assert_eq!(db.epoch().unwrap(), before + 2);
    }

    // Database::node_by_token

    #[test]
    fn tokens_authenticate_exactly_one_node_and_are_not_stored_in_the_clear() {
        let db = db();
        let (uuid, token) = db.create_node("a", "10.0.0.1", 7100).unwrap();

        assert_eq!(db.node_by_token(&token).unwrap(), Some(uuid));
        assert_eq!(db.node_by_token("wrong").unwrap(), None);

        let stored: Vec<u8> = db
            .locked(|c| Ok(c.query_row("SELECT token_sha256 FROM nodes", [], |r| r.get(0))?))
            .unwrap();
        assert_eq!(stored, tundra_common::hash::sha256(token.as_bytes()).0);
        assert_ne!(stored, token.as_bytes());
    }

    // Database::rotate_node

    #[test]
    fn rotation_invalidates_the_old_token_and_clears_the_certificate() {
        let db = db();
        let (uuid, old) = db.create_node("a", "10.0.0.1", 7100).unwrap();
        db.store_node_cert(&uuid, Hash32([3; 32])).unwrap();
        assert_eq!(
            db.list_nodes().unwrap()[0].cert_sha256,
            Some(Hash32([3; 32]))
        );

        let new = db.rotate_node(&uuid).unwrap();
        assert_ne!(old, new);
        assert_eq!(db.node_by_token(&old).unwrap(), None);
        assert_eq!(db.node_by_token(&new).unwrap(), Some(uuid));
        assert_eq!(db.list_nodes().unwrap()[0].cert_sha256, None);
    }

    #[test]
    fn rotating_an_unknown_node_is_an_error() {
        assert!(db().rotate_node(&uuid::Uuid::new_v4()).is_err());
    }

    // Database::create_server

    #[test]
    fn indices_are_dense_and_reused_after_deletion() {
        let db = db();
        let n = node(&db, "a");

        let a = db.create_server(&n, "a", &[], "c", None, &port(1)).unwrap();
        let b = db.create_server(&n, "b", &[], "c", None, &port(1)).unwrap();
        let c = db.create_server(&n, "c", &[], "c", None, &port(1)).unwrap();
        assert_eq!([a.idx, b.idx, c.idx], [0, 1, 2]);

        db.delete_server(&b.uuid).unwrap();
        let d = db.create_server(&n, "d", &[], "c", None, &port(1)).unwrap();
        assert_eq!(d.idx, 1);
    }

    #[test]
    fn servers_require_a_known_node_and_at_least_one_port() {
        let db = db();
        let n = node(&db, "a");

        assert!(
            db.create_server(&uuid::Uuid::new_v4(), "x", &[], "c", None, &port(1))
                .is_err()
        );
        assert!(db.create_server(&n, "x", &[], "c", None, &[]).is_err());
    }

    // Database::delete_node

    #[test]
    fn deleting_a_node_removes_its_servers_acls_and_ports() {
        let db = db();
        let n1 = node(&db, "a");
        let n2 = node(&db, "b");
        let s1 = db
            .create_server(&n1, "s1", &[], "c1", None, &port(1))
            .unwrap();
        let s2 = db
            .create_server(&n2, "s2", &[], "c2", None, &port(1))
            .unwrap();
        db.create_acl(&s1.uuid, &s2.uuid).unwrap();
        db.create_acl(&s2.uuid, &s1.uuid).unwrap();

        db.delete_node(&n1).unwrap();

        let snap = db.snapshot(Hash32([0; 32])).unwrap();
        assert_eq!(snap.nodes.len(), 1);
        assert_eq!(snap.servers.len(), 1);
        assert!(snap.acls.is_empty());
    }

    // Database::snapshot

    #[test]
    fn snapshot_carries_ports_grouped_per_server() {
        let db = db();
        let n = node(&db, "a");
        let ports = vec![
            PortSpec {
                port: 25565,
                proto: Proto::Both,
            },
            PortSpec {
                port: 8080,
                proto: Proto::Tcp,
            },
            PortSpec {
                port: 19132,
                proto: Proto::Udp,
            },
        ];
        let s = db
            .create_server(&n, "s", &[], "c", Some("172.17.0.5:25565"), &ports)
            .unwrap();

        let snap = db.snapshot(Hash32([7; 32])).unwrap();
        assert_eq!(snap.jwt_pubkey, Hash32([7; 32]));

        let got = snap.servers.iter().find(|x| x.uuid == s.uuid).unwrap();
        assert_eq!(got.dial_addr.as_deref(), Some("172.17.0.5:25565"));

        let mut mine = got.ports.clone();
        mine.sort_by_key(|p| p.port);
        let mut want = ports;
        want.sort_by_key(|p| p.port);
        assert_eq!(mine, want);
    }

    // Database::create_acl

    #[test]
    fn duplicate_acls_collapse_and_self_acls_are_refused() {
        let db = db();
        let n = node(&db, "a");
        let s1 = db
            .create_server(&n, "s1", &[], "c1", None, &port(1))
            .unwrap();
        let s2 = db
            .create_server(&n, "s2", &[], "c2", None, &port(1))
            .unwrap();

        db.create_acl(&s1.uuid, &s2.uuid).unwrap();
        db.create_acl(&s1.uuid, &s2.uuid).unwrap();
        assert_eq!(db.snapshot(Hash32([0; 32])).unwrap().acls.len(), 1);

        assert!(db.create_acl(&s1.uuid, &s1.uuid).is_err());
    }
}
