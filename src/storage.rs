use crate::error::Result;

use heed::types::*;
use heed::{Database, Env, PolyDatabase};
use heed_traits::{BytesDecode, BytesEncode};
use log::{info, warn};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use prost::Message;
use raft::prelude::*;

use std::borrow::Cow;
use std::fs;
use std::path::Path;
use std::sync::Arc;

pub trait LogStore: Storage {
    fn append(&mut self, entries: &[Entry]) -> Result<()>;
    fn set_hard_state(&mut self, hard_state: &HardState) -> Result<()>;
    fn set_conf_state(&mut self, conf_state: &ConfState) -> Result<()>;
    fn create_snapshot(&mut self, data: Vec<u8>) -> Result<()>;
    fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()>;
    fn compact(&mut self, index: u64) -> Result<()>;
}

const SNAPSHOT_KEY: &str = "snapshot";
const LAST_INDEX_KEY: &str = "last_index";
const HARD_STATE_KEY: &str = "hard_state";
const CONF_STATE_KEY: &str = "conf_state";

macro_rules! heed_type {
    ($heed_type:ident, $type:ty) => {
        struct $heed_type;

        impl<'a> BytesEncode<'a> for $heed_type {
            type EItem = $type;
            fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
                let mut bytes = vec![];
                prost::Message::encode(item, &mut bytes).ok()?;
                Some(Cow::Owned(bytes))
            }
        }

        impl<'a> BytesDecode<'a> for $heed_type {
            type DItem = $type;
            fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
                prost::Message::decode(bytes).ok()
            }
        }
    };
}

heed_type!(HeedSnapshot, Snapshot);
heed_type!(HeedEntry, Entry);
heed_type!(HeedHardState, HardState);
heed_type!(HeedConfState, ConfState);

pub struct HeedStorageCore {
    env: Env,
    entries_db: Database<OwnedType<u64>, HeedEntry>,
    metadata_db: PolyDatabase,
}

impl HeedStorageCore {
    pub fn create(path: impl AsRef<Path>, id: u64) -> Result<Self> {
        let path = path.as_ref();
        let name = format!("raft-{}.mdb", id);

        fs::create_dir_all(Path::new(&path).join(&name))?;

        let path = path.join(&name);

        let env = heed::EnvOpenOptions::new()
            .map_size(100 * 4096)
            .max_dbs(3000)
            .open(path)?;
        let entries_db: Database<OwnedType<u64>, HeedEntry> =
            env.create_database(Some("entries"))?;

        let metadata_db = env.create_poly_database(Some("meta"))?;

        let hard_state = HardState::default();
        let conf_state = ConfState::default();

        let storage = Self {
            metadata_db,
            entries_db,
            env,
        };

        let mut writer = storage.env.write_txn()?;
        storage.set_hard_state(&mut writer, &hard_state)?;
        storage.set_conf_state(&mut writer, &conf_state)?;
        storage.append(&mut writer, &[Entry::default()])?;
        writer.commit()?;

        Ok(storage)
    }

    fn set_hard_state(&self, writer: &mut heed::RwTxn, hard_state: &HardState) -> Result<()> {
        self.metadata_db
            .put::<_, Str, HeedHardState>(writer, HARD_STATE_KEY, hard_state)?;
        Ok(())
    }

    fn hard_state(&self, reader: &heed::RoTxn) -> Result<HardState> {
        let hard_state = self
            .metadata_db
            .get::<_, Str, HeedHardState>(reader, HARD_STATE_KEY)?;
        Ok(hard_state.expect("missing hard_state"))
    }

    pub fn set_conf_state(&self, writer: &mut heed::RwTxn, conf_state: &ConfState) -> Result<()> {
        self.metadata_db
            .put::<_, Str, HeedConfState>(writer, CONF_STATE_KEY, conf_state)?;
        Ok(())
    }

    pub fn conf_state(&self, reader: &heed::RoTxn) -> Result<ConfState> {
        let conf_state = self
            .metadata_db
            .get::<_, Str, HeedConfState>(reader, CONF_STATE_KEY)?;
        Ok(conf_state.expect("there should be a conf state"))
    }

    fn set_snapshot(&self, writer: &mut heed::RwTxn, snapshot: &Snapshot) -> Result<()> {
        self.metadata_db
            .put::<_, Str, HeedSnapshot>(writer, SNAPSHOT_KEY, snapshot)?;
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Option<Snapshot>> {
        let reader = self.env.read_txn()?;
        let snapshot = self
            .metadata_db
            .get::<_, Str, HeedSnapshot>(&reader, SNAPSHOT_KEY)?;
        Ok(snapshot)
    }

    fn last_index(&self, r: &heed::RoTxn) -> Result<u64> {
        let last_index = self
            .metadata_db
            .get::<_, Str, OwnedType<u64>>(r, LAST_INDEX_KEY)?
            .unwrap_or(0);

        Ok(last_index)
    }

    fn set_last_index(&self, w: &mut heed::RwTxn, index: u64) -> Result<()> {
        self.metadata_db
            .put::<_, Str, OwnedType<u64>>(w, LAST_INDEX_KEY, &index)?;
        Ok(())
    }

    fn first_index(&self, r: &heed::RoTxn) -> Result<u64> {
        let first_entry = self
            .entries_db
            .first(r)?
            .expect("There should always be at least one entry in the db");
        Ok(first_entry.0 + 1)
    }

    fn entry(&self, reader: &heed::RoTxn, index: u64) -> Result<Option<Entry>> {
        let entry = self.entries_db.get(reader, &index)?;
        Ok(entry)
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        info!("entries requested: {}->{}", low, high);

        let reader = self.env.read_txn()?;
        let iter = self.entries_db.range(&reader, &(low..high))?;
        let max_size: Option<u64> = max_size.into();
        let mut size_count = 0;
        let mut buf = vec![];
        let entries = iter
            .filter_map(|e| match e {
                Ok((_, e)) => Some(e),
                _ => None,
            })
            .take_while(|entry| match max_size {
                Some(max_size) => {
                    entry.encode(&mut buf).unwrap();
                    size_count += buf.len() as u64;
                    buf.clear();
                    size_count < max_size
                }
                None => true,
            })
            .collect();
        Ok(entries)
    }

    fn append(&self, writer: &mut heed::RwTxn, entries: &[Entry]) -> Result<()> {
        let mut last_index = self.last_index(&writer)?;
        // TODO: ensure entry arrive in the right order
        for entry in entries {
            //assert_eq!(entry.get_index(), last_index + 1);
            let index = entry.index;
            last_index = std::cmp::max(index, last_index);
            self.entries_db.put(writer, &index, entry)?;
        }
        self.set_last_index(writer, last_index)?;
        Ok(())
    }
}

pub struct HeedStorage(Arc<RwLock<HeedStorageCore>>);

impl HeedStorage {
    pub fn create(path: impl AsRef<Path>, id: u64) -> Result<Self> {
        let core = HeedStorageCore::create(path, id)?;
        Ok(Self(Arc::new(RwLock::new(core))))
    }

    fn wl(&mut self) -> RwLockWriteGuard<HeedStorageCore> {
        self.0.write()
    }

    fn rl(&self) -> RwLockReadGuard<HeedStorageCore> {
        self.0.read()
    }
}

impl LogStore for HeedStorage {
    fn compact(&mut self, index: u64) -> Result<()> {
        let store = self.wl();
        let mut writer = store.env.write_txn()?;
        // TODO, check that compaction is legal
        //let last_index = self.last_index(&writer)?;
        // there should always be at least one entry in the log
        //assert!(last_index > index + 1);
        store.entries_db.delete_range(&mut writer, &(..index))?;
        writer.commit()?;
        Ok(())
    }

    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        let store = self.wl();
        let mut writer = store.env.write_txn()?;
        store.append(&mut writer, entries)?;
        writer.commit()?;
        Ok(())
    }

    fn set_hard_state(&mut self, hard_state: &HardState) -> Result<()> {
        let store = self.wl();
        let mut writer = store.env.write_txn()?;
        store.set_hard_state(&mut writer, hard_state)?;
        writer.commit()?;
        Ok(())
    }

    fn set_conf_state(&mut self, conf_state: &ConfState) -> Result<()> {
        let store = self.wl();
        let mut writer = store.env.write_txn()?;
        store.set_conf_state(&mut writer, conf_state)?;
        writer.commit()?;
        Ok(())
    }

    fn create_snapshot(&mut self, data: Vec<u8>) -> Result<()> {
        let store = self.wl();
        let mut writer = store.env.write_txn()?;
        let hard_state = store.hard_state(&writer)?;
        let conf_state = store.conf_state(&writer)?;

        let mut snapshot = Snapshot::default();
        snapshot.set_data(data);

        let meta = snapshot.mut_metadata();
        meta.set_conf_state(conf_state);
        meta.index = hard_state.commit;
        meta.term = hard_state.term;

        store.set_snapshot(&mut writer, &snapshot)?;
        writer.commit()?;
        Ok(())
    }

    fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        let store = self.wl();
        let mut writer = store.env.write_txn()?;
        let metadata = snapshot.get_metadata();
        let conf_state = metadata.get_conf_state();
        let mut hard_state = store.hard_state(&writer)?;
        hard_state.set_term(metadata.term);
        hard_state.set_commit(metadata.index);
        store.set_hard_state(&mut writer, &hard_state)?;
        store.set_conf_state(&mut writer, conf_state)?;
        store.set_last_index(&mut writer, metadata.index)?;
        writer.commit()?;
        Ok(())
    }
}

impl Storage for HeedStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let store = self.rl();
        let reader = store
            .env
            .read_txn()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?;
        let raft_state = RaftState {
            hard_state: store
                .hard_state(&reader)
                .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?,
            conf_state: store
                .conf_state(&reader)
                .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?,
        };
        warn!("raft_state: {:#?}", raft_state);
        Ok(raft_state)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<Entry>> {
        let store = self.rl();
        let entries = store
            .entries(low, high, max_size)
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?;
        Ok(entries)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let store = self.rl();
        let reader = store
            .env
            .read_txn()
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        let first_index = store
            .first_index(&reader)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        let last_index = store
            .last_index(&reader)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        let hard_state = store
            .hard_state(&reader)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        if idx == hard_state.commit {
            return Ok(hard_state.term);
        }

        if idx < first_index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        if idx > last_index {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        let entry = store
            .entry(&reader, idx)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        Ok(entry.map(|e| e.term).unwrap_or(0))
    }

    fn first_index(&self) -> raft::Result<u64> {
        let store = self.rl();
        let reader = store.env.read_txn().unwrap();
        store
            .first_index(&reader)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))
    }

    fn last_index(&self) -> raft::Result<u64> {
        let store = self.rl();
        let reader = store.env.read_txn().unwrap();
        let last_index = store
            .last_index(&reader)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;

        Ok(last_index)
    }

    fn snapshot(&self, _index: u64) -> raft::Result<Snapshot> {
        let store = self.rl();
        match store.snapshot() {
            Ok(Some(snapshot)) => Ok(snapshot),
            _ => Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            )),
        }
    }
}
