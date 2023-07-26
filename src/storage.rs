use crate::error::Result;

use raft::prelude::*;
use raft::storage::MemStorage as CoreMemStorage;
use raft::GetEntriesContext;

pub trait LogStore: Storage {
    fn append(&mut self, entries: &[Entry]) -> Result<()>;
    fn set_hard_state(&mut self, hard_state: &HardState) -> Result<()>;
    fn set_hard_state_comit(&mut self, comit: u64) -> Result<()>;
    fn set_conf_state(&mut self, conf_state: &ConfState) -> Result<()>;
    fn create_snapshot(&mut self, data: Vec<u8>) -> Result<()>;
    fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()>;
    fn compact(&mut self, index: u64) -> Result<()>;
}

pub struct MemStorage {
    core: CoreMemStorage,
    snapshot: Snapshot,
}

impl MemStorage {
    #[inline]
    pub fn create() -> Self {
        let core = CoreMemStorage::default();
        let snapshot = Default::default();
        Self { core, snapshot }
    }
}

impl LogStore for MemStorage {
    #[inline]
    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        let mut store = self.core.wl();
        store.append(entries)?;
        Ok(())
    }

    #[inline]
    fn set_hard_state(&mut self, hard_state: &HardState) -> Result<()> {
        let mut store = self.core.wl();
        store.set_hardstate(hard_state.clone());
        Ok(())
    }

    #[inline]
    fn set_hard_state_comit(&mut self, comit: u64) -> Result<()> {
        let mut store = self.core.wl();
        let mut hard_state = store.hard_state().clone();
        hard_state.set_commit(comit);
        store.set_hardstate(hard_state);
        Ok(())
    }

    #[inline]
    fn set_conf_state(&mut self, conf_state: &ConfState) -> Result<()> {
        let mut store = self.core.wl();
        store.set_conf_state(conf_state.clone());
        Ok(())
    }

    #[inline]
    fn create_snapshot(&mut self, data: Vec<u8>) -> Result<()> {
        let mut snapshot = self.core.snapshot(0, 0)?;
        snapshot.set_data(data);
        self.snapshot = snapshot;
        Ok(())
    }

    #[inline]
    fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        let mut store = self.core.wl();
        store.apply_snapshot(snapshot)?;
        Ok(())
    }

    #[inline]
    fn compact(&mut self, index: u64) -> Result<()> {
        let mut store = self.core.wl();
        store.compact(index)?;
        Ok(())
    }
}

impl Storage for MemStorage {
    #[inline]
    fn initial_state(&self) -> raft::Result<RaftState> {
        let raft_state = self.core.initial_state()?;
        Ok(raft_state)
    }

    #[inline]
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let entries = self.core.entries(low, high, max_size, context)?;
        Ok(entries)
    }

    #[inline]
    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.core.term(idx)
    }

    #[inline]
    fn first_index(&self) -> raft::Result<u64> {
        self.core.first_index()
    }

    #[inline]
    fn last_index(&self) -> raft::Result<u64> {
        self.core.last_index()
    }

    #[inline]
    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        Ok(self.snapshot.clone())
    }
}
