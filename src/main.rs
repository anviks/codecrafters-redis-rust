use crate::resp::{CmdError, RESPValue, decode, encode};
use std::{
    collections::{HashMap, VecDeque},
    ops::Add,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};
mod resp;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct StreamId {
    pub(crate) ms: u64,
    pub(crate) seq: u64,
}

impl StreamId {
    fn next_from_str(&self, s: &str) -> Result<Self, CmdError> {
        if s == "*" {
            let ms = {
                let millis = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                self.ms.max(millis)
            };

            let seq = if self.ms == ms { self.seq + 1 } else { 0 };

            return Ok(StreamId { ms, seq });
        } else {
            let parts: Vec<&str> = s.splitn(2, "-").collect();
            if parts.len() != 2 {
                return Err(CmdError::InvalidStreamId);
            }

            let ms: u64 = parts[0].parse().map_err(|_| CmdError::InvalidStreamId)?;

            let seq = if parts[1] == "*" {
                if self.ms == ms { self.seq + 1 } else { 0 }
            } else {
                parts[1].parse().map_err(|_| CmdError::InvalidStreamId)?
            };

            if ms == 0 && seq == 0 {
                return Err(CmdError::ZeroStreamId);
            }

            let id = StreamId { ms, seq };
            if id > *self {
                Ok(id)
            } else {
                Err(CmdError::BadStreamId)
            }
        }
    }
}

#[derive(Clone, Debug)]
struct StreamEntry {
    id: StreamId,
    fields: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
struct Stream {
    entries: Vec<StreamEntry>,
    last_id: StreamId,
}

#[derive(Clone, Debug)]
enum Data {
    String(String),
    List(VecDeque<String>),
    Stream(Stream),
}

impl Data {
    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Data::String(s) => Some(s),
            _ => None,
        }
    }

    pub(crate) fn as_vec(&self) -> Option<&VecDeque<String>> {
        match self {
            Data::List(vec) => Some(vec),
            _ => None,
        }
    }

    pub(crate) fn as_vec_mut(&mut self) -> Option<&mut VecDeque<String>> {
        match self {
            Data::List(vec) => Some(vec),
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

    pub(crate) fn try_vec(&self) -> Result<&VecDeque<String>, CmdError> {
        self.as_vec().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_vec_mut(&mut self) -> Result<&mut VecDeque<String>, CmdError> {
        self.as_vec_mut().ok_or(CmdError::WrongType)
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
struct Value {
    data: Data,
    expires_at: Option<Instant>,
}

fn arg(arr: &[RESPValue], i: usize) -> Result<&str, CmdError> {
    let arg = arr.get(i).ok_or(CmdError::WrongArgs)?;
    arg.try_str()
}

fn arg_int(arr: &[RESPValue], i: usize) -> Result<i64, CmdError> {
    arg(arr, i)?.parse().map_err(|_| CmdError::NotInt)
}

fn arg_uint(arr: &[RESPValue], i: usize) -> Result<u64, CmdError> {
    arg(arr, i)?.parse().map_err(|_| CmdError::NotUint)
}

fn arg_double(arr: &[RESPValue], i: usize) -> Result<f64, CmdError> {
    arg(arr, i)?.parse().map_err(|_| CmdError::NotDouble)
}

fn to_strings(arr: &[RESPValue]) -> Result<Vec<String>, CmdError> {
    arr.iter()
        .map(|resp| resp.try_str().map(str::to_string))
        .collect()
}

fn parse_expiry(args: &[RESPValue]) -> Option<Instant> {
    let str = arg(args, 0).ok()?;
    let uint = arg_uint(args, 1).ok()?;

    match str.to_lowercase().as_str() {
        "ex" => Some(Instant::now().add(Duration::from_secs(uint))),
        "px" => Some(Instant::now().add(Duration::from_millis(uint))),
        _ => None,
    }
}

fn cmd_lpop(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let mut lock = store.lock().unwrap();

    match lock.entries.get_mut(key) {
        Some(val) => {
            let vec = val.data.try_vec_mut()?;

            if arr.len() > 2 {
                let amount = (arg_int(&arr, 2)? as usize).min(vec.len());
                let popped: Vec<RESPValue> = vec.drain(..amount).map(RESPValue::from).collect();
                Ok(popped.into())
            } else {
                Ok(if vec.is_empty() {
                    RESPValue::BulkString(None)
                } else {
                    vec.pop_front().unwrap().into()
                })
            }
        }
        None => Ok(RESPValue::BulkString(None)),
    }
}

fn cmd_lpush(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let mut lock = store.lock().unwrap();

    let vec_len = match lock.entries.get_mut(key) {
        Some(val) => {
            let vec = val.data.try_vec_mut()?;
            for v in to_strings(&arr[2..])? {
                vec.push_front(v);
            }
            vec.len()
        }
        None => {
            let mut vec = to_strings(&arr[2..])?;
            vec.reverse();
            let len = vec.len();
            lock.entries.insert(
                key.to_string(),
                Value {
                    data: Data::List(vec.into()),
                    expires_at: None,
                },
            );
            len
        }
    };

    Ok(RESPValue::Integer(vec_len as i64))
}

fn cmd_llen(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let lock = store.lock().unwrap();

    let vec_len = match lock.entries.get(key) {
        Some(val) => val.data.try_vec()?.len(),
        None => 0,
    };

    Ok(RESPValue::Integer(vec_len as i64))
}

fn cmd_lrange(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let lock = store.lock().unwrap();
    match lock.entries.get(key) {
        Some(val) => {
            let vec = val.data.try_vec()?;
            if vec.is_empty() {
                return Ok(vec![].into());
            }

            let start: usize = {
                let s = arg_int(&arr, 2)?;

                if s < 0 {
                    (vec.len() as i64 + s).max(0) as usize
                } else {
                    s as usize
                }
            };

            let stop: usize = {
                let s = arg_int(&arr, 3)?.min((vec.len() - 1) as i64);

                if s < 0 {
                    (vec.len() as i64 + s).max(0) as usize
                } else {
                    s as usize
                }
            };

            if start > stop || start >= vec.len() {
                Ok(vec![].into())
            } else {
                Ok(vec
                    .range(start..=stop)
                    .into_iter()
                    .map(|s| s.clone().into())
                    .collect::<Vec<RESPValue>>()
                    .into())
            }
        }
        None => Ok(vec![].into()),
    }
}

fn cmd_get(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let mut lock = store.lock().unwrap();
    match lock.entries.get(key) {
        Some(val) if val.expires_at.map_or(false, |inst| Instant::now() >= inst) => {
            lock.entries.remove(key);
            Ok(RESPValue::BulkString(None))
        }
        Some(val) => match &val.data {
            Data::String(s) => Ok(s.clone().into()),
            _ => Err(CmdError::WrongType),
        },
        None => Ok(RESPValue::BulkString(None)),
    }
}

fn cmd_set(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let value = Value {
        data: Data::String(arg(&arr, 2)?.to_string()),
        expires_at: parse_expiry(&arr[3..]),
    };

    store.lock().unwrap().entries.insert(key.to_string(), value);
    Ok(RESPValue::SimpleString("OK".to_string()))
}

fn cmd_rpush(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let mut values: VecDeque<String> = to_strings(&arr[2..])?.into();
    let mut sent_values = 0;
    let mut lock = store.lock().unwrap();

    if let Some(waiters) = lock.waiters.get_mut(key) {
        while let Some(val) = values.pop_front() {
            let Some(w) = waiters.pop_front() else {
                values.push_front(val);
                break;
            };

            if let Err(returned) = w.send(val) {
                values.push_front(returned);
                continue;
            }

            sent_values += 1;
        }
    };

    let vec_len = match lock.entries.get_mut(key) {
        Some(val) => {
            let vec = val.data.try_vec_mut()?;
            vec.extend(values);
            vec.len()
        }
        None => {
            let len = values.len();
            lock.entries.insert(
                key.to_string(),
                Value {
                    data: Data::List(VecDeque::from(values)),
                    expires_at: None,
                },
            );
            len
        }
    };

    Ok(RESPValue::Integer((vec_len + sent_values) as i64))
}

async fn cmd_blpop(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let timeout = arg_double(&arr, 2)?;

    let receiver = {
        let mut lock = store.lock().unwrap();

        if let Some(val) = lock.entries.get_mut(key) {
            let vec = val.data.try_vec_mut()?;
            if let Some(v) = vec.pop_front() {
                return Ok(vec![key.to_string().into(), v.into()].into());
            }
        }

        let (sender, receiver) = oneshot::channel();
        lock.waiters
            .entry(key.to_string())
            .or_default()
            .push_back(sender);
        receiver
    };

    if timeout > 0.0 {
        let duration = Duration::from_secs_f64(timeout);
        Ok(match tokio::time::timeout(duration, receiver).await {
            Ok(Ok(value)) => vec![key.to_string().into(), value.into()].into(),
            _ => RESPValue::Array(None),
        })
    } else {
        Ok(receiver
            .await
            .map(|value| vec![key.to_string().into(), value.into()].into())
            .unwrap_or(RESPValue::Array(None)))
    }
}

fn cmd_type(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    Ok(match store.lock().unwrap().entries.get(key) {
        Some(Value {
            data: value,
            expires_at: _,
        }) => RESPValue::SimpleString(value.type_name().to_string()),
        None => RESPValue::SimpleString("none".to_string()),
    })
}

fn cmd_xadd(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let entry_id = arg(&arr, 2)?;
    arg(&arr, 3)?;

    let mut fields = vec![];
    for i in (3..arr.len()).step_by(2) {
        let field = arg(&arr, i)?.to_string();
        let value = arg(&arr, i + 1)?.to_string();
        fields.push((field, value));
    }

    let mut lock = store.lock().unwrap();
    let val = lock.entries.entry(key.to_string()).or_insert(Value {
        data: Data::Stream(Stream {
            entries: vec![],
            last_id: StreamId { ms: 0, seq: 0 },
        }),
        expires_at: None,
    });
    let stream = val.data.try_stream_mut()?;

    let id = stream.last_id.next_from_str(entry_id)?;

    stream.entries.push(StreamEntry { id, fields });
    stream.last_id = id;

    Ok(format!("{}-{}", id.ms, id.seq).into())
}

struct Store {
    entries: HashMap<String, Value>,
    waiters: HashMap<String, VecDeque<oneshot::Sender<String>>>,
}
type SharedStore = Arc<Mutex<Store>>;

#[tokio::main]
async fn main() {
    let store: SharedStore = Arc::new(Mutex::new(Store {
        entries: HashMap::new(),
        waiters: HashMap::new(),
    }));
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let loc_store = Arc::clone(&store);
                tokio::spawn(async move {
                    loop {
                        let mut buf = [0; 512];
                        match stream.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                let parsed = decode(&buf[..n]);
                                println!("{:?}", parsed);

                                if let RESPValue::Array(array) = parsed
                                    && let Some(arr) = array
                                    && !arr.is_empty()
                                    && let Some(cmd) = arr[0].as_str()
                                {
                                    let response: Result<RESPValue, CmdError> = match cmd
                                        .to_lowercase()
                                        .as_str()
                                    {
                                        "ping" => Ok(RESPValue::SimpleString("PONG".to_string())),
                                        "echo" if arr.len() > 1 => Ok(arr[1].clone()),
                                        "get" => cmd_get(&arr, &loc_store),
                                        "set" => cmd_set(&arr, &loc_store),
                                        "rpush" => cmd_rpush(&arr, &loc_store),
                                        "lpush" => cmd_lpush(&arr, &loc_store),
                                        "lpop" => cmd_lpop(&arr, &loc_store),
                                        "llen" => cmd_llen(&arr, &loc_store),
                                        "lrange" => cmd_lrange(&arr, &loc_store),
                                        "blpop" => cmd_blpop(&arr, &loc_store).await,
                                        "type" => cmd_type(&arr, &loc_store),
                                        "xadd" => cmd_xadd(&arr, &loc_store),
                                        _ => Err(CmdError::Unknown),
                                    };

                                    let output = match response {
                                        Ok(val) => encode(&val),
                                        Err(err) => {
                                            encode(&RESPValue::SimpleError(format!("{}", err)))
                                        }
                                    };

                                    if stream.write_all(&output).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
