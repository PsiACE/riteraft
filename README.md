# RiteRaft - A raft framework, for regular people 

This is an attempt to create a layer on top of
[tikv/raft-rs](https://github.com/tikv/raft-rs), that is easier to use and
implement. This is not supposed to be the most featureful raft, but instead a
convenient interface to get started quickly, and have a working raft in no
time.

The interface is strongly inspired by the one used by [canonical/raft](https://github.com/canonical/raft).

## Getting started

In order to "raft" storage, we need to implement the `Storage` trait for it.
Bellow is an example with `HashStore`, which is a thread-safe wrapper around an
`HashMap`:

```rust
/// convienient data structure to pass Message in the raft
#[derive(Serialize, Deserialize)]
pub enum Message {
    Insert { key: u64, value: String },
}

#[derive(Clone)]
struct HashStore(Arc<RwLock<HashMap<u64, String>>>);

impl Store for HashStore {
    type Error = RaftError;

    fn apply(&mut self, message: &[u8]) -> Result<Vec<u8>, Self::Error> {
        let message: Message = deserialize(message).unwrap();
        let message: Vec<u8> = match message {
            Message::Insert { key, value } => {

                let mut db = self.0.write().unwrap();
                db.insert(key, value.clone());
                serialize(&value).unwrap()
            }
        };
        Ok(message)
    }

    fn snapshot(&self) -> Vec<u8> {
        serialize(&self.0.read().unwrap().clone()).unwrap()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error> {
        let new: HashMap<u64, String> = deserialize(snapshot).unwrap();
        let mut db = self.0.write().unwrap();
        let _ = std::mem::replace(&mut *db, new);
        Ok(())
    }
}
```

Only 3 methods need to be implemented for the Store: 
- `Store::apply`: applies a commited entry to the store.  
- `Store::snapshot`: returns snapshot data for the store. 
- `Store::apply`: applies the snapshot passed as argument.

### running the raft

```rust
#[tokio::main]
fn main() {
    let store = HashStore::new();

    let raft = Raft::new(options.raft_addr, store.clone());
    let mailbox = Arc::new(raft.mailbox());
    let (raft_handle, mailbox) = match options.peer_addr {
        Some(addr) => {
            info!("running in follower mode");
            let handle = tokio::spawn(raft.join(addr));
            (handle, mailbox)
        }
        None => {
            info!("running in leader mode");
            let handle =  tokio::spawn(raft.lead());
            (handle, mailbox)
        }
    };
    
    tokio::join!(raft);
}

```

The `mailbox` gives you a way to interact with the raft, for sending a message, or leaving the cluster for example.

