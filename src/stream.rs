use crate::resp::CmdError;
use std::{
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct StreamId {
    pub(crate) ms: u64,
    pub(crate) seq: u64,
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl FromStr for StreamId {
    type Err = CmdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.splitn(2, "-").collect();
        if parts.len() != 2 {
            return Err(CmdError::InvalidStreamId);
        }

        let ms: u64 = parts[0].parse().map_err(|_| CmdError::InvalidStreamId)?;
        let seq: u64 = parts[1].parse().map_err(|_| CmdError::InvalidStreamId)?;

        Ok(StreamId { ms, seq })
    }
}

impl StreamId {
    pub(crate) fn next_from_str(&self, s: &str) -> Result<Self, CmdError> {
        let parts: Vec<&str> = s.split("-").collect();
        let id = match parts[..] {
            ["*"] => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                // If there's a manually created id with a higher ms value than current time, then use that
                let ms = self.ms.max(now);
                let seq = if self.ms == ms { self.seq + 1 } else { 0 };
                return Ok(StreamId { ms, seq });
            }
            [millis, "*"] => {
                let ms = millis.parse().map_err(|_| CmdError::InvalidStreamId)?;
                let seq = if self.ms == ms { self.seq + 1 } else { 0 };
                // Can never end up as 0-0 in practice, due to new streams having 0-0 as last_id
                StreamId { ms, seq }
            }
            [millis, sequence] => {
                let ms = millis.parse().map_err(|_| CmdError::InvalidStreamId)?;
                let seq = sequence.parse().map_err(|_| CmdError::InvalidStreamId)?;

                if ms == 0 && seq == 0 {
                    return Err(CmdError::ZeroStreamId);
                }

                StreamId { ms, seq }
            }
            _ => return Err(CmdError::InvalidStreamId),
        };

        if id > *self {
            Ok(id)
        } else {
            Err(CmdError::BadStreamId)
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StreamEntry {
    pub(crate) id: StreamId,
    pub(crate) fields: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
pub(crate) struct Stream {
    pub(crate) entries: Vec<StreamEntry>,
    pub(crate) last_id: StreamId,
}
