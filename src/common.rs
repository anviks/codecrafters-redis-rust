use std::sync::Arc;
use thiserror::Error;

pub(crate) struct Config {
    pub(crate) is_replica: bool,
    pub(crate) dir: String,
    pub(crate) dbfilename: String,
}

pub(crate) type SharedConfig = Arc<Config>;

#[derive(PartialEq, PartialOrd, Clone, Copy, Debug)]
pub(crate) struct OrderedF64(f64);

impl Eq for OrderedF64 {}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl OrderedF64 {
    pub(crate) fn new(f: f64) -> Option<Self> {
        f.is_finite().then_some(Self(f))
    }

    pub(crate) fn get(&self) -> f64 {
        self.0
    }
}

#[derive(Error, Debug)]
pub(crate) enum CmdError {
    #[error("ERR invalid longitude,latitude pair {longitude},{latitude}")]
    InvalidCoords { longitude: f64, latitude: f64 },

    #[error(
        "ERR Can't execute '{0}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
    )]
    NotSubModeCmd(String),

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("ERR value is not an integer or out of range")]
    NotInt,

    #[error("ERR value is not an integer or out of range")]
    NotUint,

    #[error("ERR value is not a double or out of range")]
    NotDouble,

    #[error("Invalid stream ID specified as stream command argument")]
    InvalidStreamId,

    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    BadStreamId,

    #[error("ERR The ID specified in XADD must be greater than 0-0")]
    ZeroStreamId,

    #[error("ERR EXEC without MULTI")]
    ExecWithoutMulti,

    #[error("ERR DISCARD without MULTI")]
    DiscardWithoutMulti,

    #[error("ERR MULTI calls can not be nested")]
    NestedMulti,

    #[error("ERR wrong number of arguments for command")]
    WrongArgs,

    #[error("ERR syntax error")]
    Syntax,

    #[error("ERR unknown command")]
    Unknown,
}
