use crate::{resp::CmdError, stream::Stream};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, atomic::AtomicU64},
    time::SystemTime,
    u64,
};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug)]
pub(crate) enum Data {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Stream(Stream),
}

impl Data {
    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Data::String(s) => str::from_utf8(s).ok(),
            _ => None,
        }
    }

    pub(crate) fn as_vec(&self) -> Option<&VecDeque<Vec<u8>>> {
        match self {
            Data::List(vec) => Some(vec),
            _ => None,
        }
    }

    pub(crate) fn as_vec_mut(&mut self) -> Option<&mut VecDeque<Vec<u8>>> {
        match self {
            Data::List(vec) => Some(vec),
            _ => None,
        }
    }

    pub(crate) fn as_stream(&self) -> Option<&Stream> {
        match self {
            Data::Stream(stream) => Some(stream),
            _ => None,
        }
    }

    pub(crate) fn as_stream_mut(&mut self) -> Option<&mut Stream> {
        match self {
            Data::Stream(stream) => Some(stream),
            _ => None,
        }
    }

    pub(crate) fn try_str(&self) -> Result<&str, CmdError> {
        self.as_str().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_vec(&self) -> Result<&VecDeque<Vec<u8>>, CmdError> {
        self.as_vec().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_vec_mut(&mut self) -> Result<&mut VecDeque<Vec<u8>>, CmdError> {
        self.as_vec_mut().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_stream(&self) -> Result<&Stream, CmdError> {
        self.as_stream().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_stream_mut(&mut self) -> Result<&mut Stream, CmdError> {
        self.as_stream_mut().ok_or(CmdError::WrongType)
    }

    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            Data::String(_) => "string",
            Data::List(_) => "list",
            Data::Stream(_) => "stream",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Value {
    pub(crate) data: Data,
    pub(crate) expires_at: Option<SystemTime>,
}

pub(crate) struct Replica {
    pub(crate) sender: mpsc::UnboundedSender<Vec<u8>>,
    pub(crate) offset: Arc<AtomicU64>,
}

pub(crate) struct Store {
    pub(crate) entries: HashMap<Vec<u8>, Value>,
    pub(crate) blpop_waiters: HashMap<Vec<u8>, VecDeque<oneshot::Sender<Vec<u8>>>>,
    pub(crate) xread_waiters: HashMap<u64, oneshot::Sender<()>>,
    pub(crate) xread_waiters_by_key: HashMap<Vec<u8>, VecDeque<u64>>,
    pub(crate) next_id: u64,
    pub(crate) replicas: Vec<Replica>,
    pub(crate) master_offset: u64,
}

pub(crate) type SharedStore = Arc<Mutex<Store>>;

impl Store {
    pub(crate) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            blpop_waiters: HashMap::new(),
            xread_waiters: HashMap::new(),
            xread_waiters_by_key: HashMap::new(),
            next_id: 1,
            replicas: vec![],
            master_offset: 0,
        }
    }

    pub(crate) fn add_xread_waiter(&mut self, waiter: oneshot::Sender<()>) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.xread_waiters.insert(id, waiter);
        id
    }

    pub(crate) fn add_key_for_xread_waiter(&mut self, key: Vec<u8>, waiter_id: u64) {
        self.xread_waiters_by_key
            .entry(key)
            .or_default()
            .push_back(waiter_id);
    }
}
