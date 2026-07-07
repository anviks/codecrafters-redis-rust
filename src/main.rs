use crate::resp::{CmdError, RESPValue, decode, encode};
use std::{
    collections::HashMap,
    ops::Add,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
mod resp;

#[derive(Clone, Debug)]
struct Value {
    value: RESPValue,
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

fn parse_expiry(args: &[RESPValue]) -> Option<Instant> {
    let str = arg(args, 0).ok()?;
    let uint = arg_uint(args, 1).ok()?;

    match str.to_lowercase().as_str() {
        "ex" => Some(Instant::now().add(Duration::from_secs(uint))),
        "px" => Some(Instant::now().add(Duration::from_millis(uint))),
        _ => None,
    }
}

fn cmd_lpop(arr: &[RESPValue], store: &Store) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let mut lock = store.lock().unwrap();

    match lock.get_mut(key) {
        Some(val) => {
            let vec = val.value.try_vec_mut()?;

            if arr.len() > 2 {
                let amount = (arg_int(&arr, 2)? as usize).min(vec.len());
                Ok(RESPValue::Array(Some(vec.splice(0..amount, []).collect())))
            } else {
                Ok(if vec.is_empty() {
                    RESPValue::BulkString(None)
                } else {
                    vec.remove(0)
                })
            }
        }
        None => Ok(RESPValue::BulkString(None)),
    }
}

fn cmd_lpush(arr: &[RESPValue], store: &Store) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let mut lock = store.lock().unwrap();

    let vec_len = match lock.get_mut(key) {
        Some(val) => {
            let vec = val.value.try_vec_mut()?;
            vec.splice(0..0, arr[2..].iter().rev().cloned());
            vec.len()
        }
        None => {
            let mut vec = arr[2..].to_vec();
            vec.reverse();
            let len = vec.len();
            lock.insert(
                key.to_string(),
                Value {
                    value: vec.into(),
                    expires_at: None,
                },
            );
            len
        }
    };

    Ok(RESPValue::Integer(vec_len as i64))
}

fn cmd_llen(arr: &[RESPValue], store: &Store) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let lock = store.lock().unwrap();

    let vec_len = match lock.get(key) {
        Some(val) => val.value.try_vec()?.len(),
        None => 0,
    };

    Ok(RESPValue::Integer(vec_len as i64))
}

fn cmd_lrange(arr: &[RESPValue], store: &Store) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let lock = store.lock().unwrap();
    match lock.get(key) {
        Some(val) => {
            let vec = val.value.try_vec()?;

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
                Ok(vec[start..=stop].to_vec().into())
            }
        }
        None => Ok(vec![].into()),
    }
}

fn cmd_get(arr: &[RESPValue], store: &Store) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let mut lock = store.lock().unwrap();
    match lock.get(key) {
        Some(val) if val.expires_at.map_or(false, |inst| Instant::now() >= inst) => {
            lock.remove(key);
            Ok(RESPValue::BulkString(None))
        }
        Some(val) => Ok(val.value.clone()),
        None => Ok(RESPValue::BulkString(None)),
    }
}

fn cmd_set(arr: &[RESPValue], store: &Store) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let value = Value {
        value: arg(&arr, 2)?.to_string().into(),
        expires_at: parse_expiry(&arr[3..]),
    };

    store.lock().unwrap().insert(key.to_string(), value);
    Ok(RESPValue::SimpleString("OK".to_string()))
}

fn cmd_rpush(arr: &[RESPValue], store: &Store) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let mut lock = store.lock().unwrap();

    let vec_len = match lock.get_mut(key) {
        Some(val) => {
            let vec = val.value.try_vec_mut()?;
            vec.extend_from_slice(&arr[2..]);
            vec.len()
        }
        None => {
            let vec = arr[2..].to_vec();
            let len = vec.len();
            lock.insert(
                key.to_string(),
                Value {
                    value: vec.into(),
                    expires_at: None,
                },
            );
            len
        }
    };

    Ok(RESPValue::Integer(vec_len as i64))
}

type Store = Arc<Mutex<HashMap<String, Value>>>;

#[tokio::main]
async fn main() {
    let store: Store = Arc::new(Mutex::new(HashMap::new()));
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
                                        _ => Err(CmdError::Unknown),
                                    };

                                    let output = match response {
                                        Ok(val) => encode(&val),
                                        Err(err) => {
                                            encode(&RESPValue::SimpleError(format!("{:?}", err)))
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
