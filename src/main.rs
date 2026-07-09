use crate::resp::{CmdError, RESPValue, decode, encode};
use std::{
    collections::{HashMap, VecDeque},
    ops::Add,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};
mod resp;

#[derive(Clone, Debug)]
enum Data {
    String(String),
    List(Vec<String>),
}

impl Data {
    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Data::String(s) => Some(s),
            _ => None,
        }
    }

    pub(crate) fn as_vec(&self) -> Option<&Vec<String>> {
        match self {
            Data::List(vec) => Some(vec),
            _ => None,
        }
    }

    pub(crate) fn as_vec_mut(&mut self) -> Option<&mut Vec<String>> {
        match self {
            Data::List(vec) => Some(vec),
            _ => None,
        }
    }

    pub(crate) fn try_str(&self) -> Result<&str, CmdError> {
        self.as_str().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_vec(&self) -> Result<&Vec<String>, CmdError> {
        self.as_vec().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_vec_mut(&mut self) -> Result<&mut Vec<String>, CmdError> {
        self.as_vec_mut().ok_or(CmdError::WrongType)
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
                Ok(vec
                    .splice(0..amount, [])
                    .map(RESPValue::from)
                    .collect::<Vec<RESPValue>>()
                    .into())
            } else {
                Ok(if vec.is_empty() {
                    RESPValue::BulkString(None)
                } else {
                    vec.remove(0).into()
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
            let values = arr[2..]
                .iter()
                .rev()
                .map(|resp| resp.try_str().map(str::to_string))
                .collect::<Result<Vec<String>, CmdError>>()?;
            vec.splice(0..0, values);
            vec.len()
        }
        None => {
            let mut vec = arr[2..]
                .iter()
                .map(|resp| resp.try_str().map(str::to_string))
                .collect::<Result<Vec<String>, CmdError>>()?;
            vec.reverse();
            let len = vec.len();
            lock.entries.insert(
                key.to_string(),
                Value {
                    data: Data::List(vec),
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
                Ok(vec[start..=stop]
                    .to_vec()
                    .into_iter()
                    .map(RESPValue::from)
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
    let mut values = arr[2..]
        .iter()
        .map(|resp| resp.try_str().map(str::to_string))
        .collect::<Result<VecDeque<String>, CmdError>>()?;
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
                    data: Data::List(Vec::from(values)),
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

    if let Some(val) = store.lock().unwrap().entries.get_mut(key) {
        let vec = val.data.try_vec_mut()?;
        if !vec.is_empty() {
            return Ok(vec.remove(0).into());
        }
    }

    let (sender, receiver) = oneshot::channel();

    {
        let mut lock = store.lock().unwrap();
        let waiter = lock.waiters.entry(key.to_string()).or_insert(vec![].into());
        waiter.push_back(sender);
    }

    let timeout = arg_double(&arr, 2)?;

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
        }) => match value {
            Data::String(_) => RESPValue::SimpleString("string".to_string()),
            _ => RESPValue::SimpleString("none".to_string()),
        },
        None => RESPValue::SimpleString("none".to_string()),
    })
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
