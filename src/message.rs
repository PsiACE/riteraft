use std::collections::HashMap;

use raft::eraftpb::{ConfChange, Message as RaftMessage};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::Sender;

#[derive(Serialize, Deserialize, Debug)]
pub enum RaftResponse {
    WrongLeader {
        leader_id: u64,
        leader_addr: String,
    },
    JoinSuccess {
        assigned_id: u64,
        peer_addrs: HashMap<u64, String>,
    },
    IdReserved {
        id: u64,
    },
    Error,
    Response {
        data: Vec<u8>,
    },
    Ok,
}

#[allow(dead_code)]
pub enum Message {
    Propose {
        proposal: Vec<u8>,
        chan: Sender<RaftResponse>,
    },
    ConfigChange {
        change: ConfChange,
        chan: Sender<RaftResponse>,
    },
    RequestId {
        chan: Sender<RaftResponse>,
    },
    ReportUnreachable {
        node_id: u64,
    },
    Raft(Box<RaftMessage>),
}
