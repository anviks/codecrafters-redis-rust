use crate::resp::{RESPValue, decode, encode};
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

fn parse_expiry(args: &[RESPValue]) -> Option<Instant> {
    if args.len() > 1
        && let Some(str) = args[0].as_str()
        && let Some(int) = args[1].as_str()
    {
        match str.to_lowercase().as_str() {
            "ex" => Some(Instant::now().add(Duration::from_secs(int.parse().unwrap()))),
            "px" => Some(Instant::now().add(Duration::from_millis(int.parse().unwrap()))),
            _ => None,
        }
    } else {
        None
    }
}

#[tokio::main]
async fn main() {
    let store: Arc<Mutex<HashMap<RESPValue, Value>>> = Arc::new(Mutex::new(HashMap::new()));
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
                                    let response = match cmd.to_lowercase().as_str() {
                                        "ping" => RESPValue::SimpleString("PONG".to_string()),
                                        "echo" if arr.len() > 1 => arr[1].clone(),
                                        "get" if arr.len() > 1 => {
                                            let key = &arr[1];
                                            let mut lock = loc_store.lock().unwrap();
                                            match lock.get(key) {
                                                Some(val)
                                                    if val.expires_at.map_or(false, |inst| {
                                                        Instant::now() >= inst
                                                    }) =>
                                                {
                                                    lock.remove(key);
                                                    RESPValue::BulkString(None)
                                                }
                                                Some(val) => val.value.clone(),
                                                None => RESPValue::BulkString(None),
                                            }
                                        }
                                        "set" if arr.len() > 2 => {
                                            let key = arr[1].clone();
                                            let value = Value {
                                                value: arr[2].clone(),
                                                expires_at: parse_expiry(&arr[3..]),
                                            };

                                            loc_store.lock().unwrap().insert(key, value);
                                            RESPValue::SimpleString("OK".to_string())
                                        }
                                        "rpush" if arr.len() > 2 => {
                                            let key = arr[1].clone();
                                            let mut lock = loc_store.lock().unwrap();

                                            let vec_len = match lock.get_mut(&key) {
                                                Some(val) => {
                                                    let vec = val.value.as_vec_mut().unwrap();
                                                    vec.extend_from_slice(&arr[2..]);
                                                    vec.len()
                                                }
                                                None => {
                                                    let vec = arr[2..].to_vec();
                                                    let len = vec.len();
                                                    lock.insert(
                                                        key,
                                                        Value {
                                                            value: vec.into(),
                                                            expires_at: None,
                                                        },
                                                    );
                                                    len
                                                }
                                            };

                                            RESPValue::Integer(vec_len as i64)
                                        }
                                        "lpush" if arr.len() > 2 => {
                                            let key = arr[1].clone();
                                            let mut lock = loc_store.lock().unwrap();

                                            let vec_len = match lock.get_mut(&key) {
                                                Some(val) => {
                                                    let vec = val.value.as_vec_mut().unwrap();
                                                    vec.splice(
                                                        0..0,
                                                        arr[2..].iter().rev().cloned(),
                                                    );
                                                    vec.len()
                                                }
                                                None => {
                                                    let mut vec = arr[2..].to_vec();
                                                    vec.reverse();
                                                    let len = vec.len();
                                                    lock.insert(
                                                        key,
                                                        Value {
                                                            value: vec.into(),
                                                            expires_at: None,
                                                        },
                                                    );
                                                    len
                                                }
                                            };

                                            RESPValue::Integer(vec_len as i64)
                                        }
                                        "lpop" if arr.len() > 1 => {
                                            let key = arr[1].clone();
                                            let mut lock = loc_store.lock().unwrap();

                                            match lock.get_mut(&key) {
                                                Some(val) => {
                                                    let vec = val.value.as_vec_mut().unwrap();

                                                    if arr.len() > 2 {
                                                        let amount = arr[2]
                                                            .as_str()
                                                            .unwrap()
                                                            .parse::<usize>()
                                                            .unwrap()
                                                            .min(vec.len());
                                                        RESPValue::Array(Some(
                                                            vec.splice(0..amount, []).collect(),
                                                        ))
                                                    } else {
                                                        if vec.is_empty() {
                                                            RESPValue::BulkString(None)
                                                        } else {
                                                            vec.remove(0)
                                                        }
                                                    }
                                                }
                                                None => RESPValue::BulkString(None),
                                            }
                                        }
                                        "llen" if arr.len() > 1 => {
                                            let key = arr[1].clone();
                                            let lock = loc_store.lock().unwrap();

                                            let vec_len = match lock.get(&key) {
                                                Some(val) => val.value.as_vec().unwrap().len(),
                                                None => 0,
                                            };

                                            RESPValue::Integer(vec_len as i64)
                                        }
                                        "lrange" if arr.len() > 3 => {
                                            let key = arr[1].clone();
                                            let lock = loc_store.lock().unwrap();
                                            match lock.get(&key) {
                                                Some(val) => {
                                                    let vec = val.value.as_vec().unwrap();

                                                    let start: usize = {
                                                        let s: i64 = arr[2]
                                                            .as_str()
                                                            .unwrap()
                                                            .parse()
                                                            .unwrap();

                                                        if s < 0 {
                                                            (vec.len() as i64 + s).max(0) as usize
                                                        } else {
                                                            s as usize
                                                        }
                                                    };

                                                    let stop: usize = {
                                                        let s = arr[3]
                                                            .as_str()
                                                            .unwrap()
                                                            .parse::<i64>()
                                                            .unwrap()
                                                            .min((vec.len() - 1) as i64);

                                                        if s < 0 {
                                                            (vec.len() as i64 + s).max(0) as usize
                                                        } else {
                                                            s as usize
                                                        }
                                                    };

                                                    if start > stop || start >= vec.len() {
                                                        vec![].into()
                                                    } else {
                                                        vec[start..=stop].to_vec().into()
                                                    }
                                                }
                                                None => vec![].into(),
                                            }
                                        }
                                        _ => RESPValue::SimpleError(format!(
                                            "ERR unknown command '{}' (or insufficient arguments)",
                                            cmd
                                        )),
                                    };

                                    let output = encode(&response);

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
