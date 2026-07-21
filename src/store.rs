use crate::{
    common::{CmdError, OrderedF64},
    stream::Stream,
};
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    sync::{Arc, Mutex, atomic::AtomicU64},
    time::SystemTime,
    u64,
};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug)]
pub(crate) struct SortedSet {
    by_score: BTreeSet<(OrderedF64, Vec<u8>)>,
    by_member: HashMap<Vec<u8>, OrderedF64>,
}

impl SortedSet {
    pub(crate) fn new() -> Self {
        Self {
            by_score: BTreeSet::new(),
            by_member: HashMap::new(),
        }
    }

    pub(crate) fn insert(&mut self, member: Vec<u8>, score: f64) -> bool {
        let score = OrderedF64::new(score).unwrap();
        let is_new = match self.by_member.insert(member.clone(), score) {
            Some(prev_score) => {
                self.by_score.remove(&(prev_score, member.clone()));
                false
            }
            None => true,
        };

        self.by_score.insert((score, member));

        is_new
    }

    pub(crate) fn score(&self, member: &[u8]) -> Option<f64> {
        self.by_member.get(member).map(|f| f.get())
    }

    pub(crate) fn rank(&self, member: &[u8]) -> Option<usize> {
        let Some(score) = self.by_member.get(member) else {
            return None;
        };
        let entry = self.by_score.get(&(*score, member.to_vec())).unwrap();

        Some(self.by_score.range(..entry).count())
    }

    pub(crate) fn range(&self, start: usize, stop: usize) -> Vec<(f64, &Vec<u8>)> {
        self.by_score
            .iter()
            .skip(start)
            .take(stop - start + 1)
            .map(|(score, member)| (score.get(), member))
            .collect()
    }

    pub(crate) fn remove(&mut self, member: &[u8]) -> bool {
        match self.by_member.remove(member) {
            Some(f) => {
                self.by_score.remove(&(f, member.to_vec()));
                true
            }
            None => false,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.by_score.len()
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Data {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    SortedSet(SortedSet),
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

    pub(crate) fn as_set(&self) -> Option<&SortedSet> {
        match self {
            Data::SortedSet(btree_set) => Some(btree_set),
            _ => None,
        }
    }

    pub(crate) fn as_set_mut(&mut self) -> Option<&mut SortedSet> {
        match self {
            Data::SortedSet(btree_set) => Some(btree_set),
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

    pub(crate) fn try_set(&self) -> Result<&SortedSet, CmdError> {
        self.as_set().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_set_mut(&mut self) -> Result<&mut SortedSet, CmdError> {
        self.as_set_mut().ok_or(CmdError::WrongType)
    }

    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            Data::String(_) => "string",
            Data::List(_) => "list",
            Data::Stream(_) => "stream",
            Data::SortedSet(_) => "zset",
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
    pub(crate) next_xread_waiter_id: u64,
    pub(crate) replicas: Vec<Replica>,
    pub(crate) master_offset: u64,
    pub(crate) next_connection_id: u64,
    pub(crate) channel_subscriptions:
        HashMap<Vec<u8>, HashMap<u64, mpsc::UnboundedSender<Vec<u8>>>>,
    pub(crate) users: HashMap<Vec<u8>, Vec<[u8; 32]>>,
}

pub(crate) type SharedStore = Arc<Mutex<Store>>;

impl Store {
    pub(crate) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            blpop_waiters: HashMap::new(),
            xread_waiters: HashMap::new(),
            xread_waiters_by_key: HashMap::new(),
            next_xread_waiter_id: 1,
            replicas: vec![],
            master_offset: 0,
            next_connection_id: 1,
            channel_subscriptions: HashMap::new(),
            users: HashMap::from([(b"default".to_vec(), vec![])]),
        }
    }

    pub(crate) fn get_next_connection_id(&mut self) -> u64 {
        self.next_connection_id += 1;
        self.next_connection_id - 1
    }

    pub(crate) fn add_xread_waiter(&mut self, waiter: oneshot::Sender<()>) -> u64 {
        let id = self.next_xread_waiter_id;
        self.next_xread_waiter_id += 1;
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
