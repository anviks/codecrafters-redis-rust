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

fn as_str(v: &RESPValue) -> Option<&str> {
    match v {
        RESPValue::BulkString(Some(s)) => Some(s),
        _ => None,
    }
}

fn as_vec(v: RESPValue) -> Option<Vec<RESPValue>> {
    match v {
        RESPValue::Array(Some(vec)) => Some(vec),
        _ => None,
    }
}

fn parse_expiry(args: &[RESPValue]) -> Option<Instant> {
    if args.len() > 1
        && let Some(str) = as_str(&args[0])
        && let Some(int) = as_str(&args[1])
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
                                    && let Some(cmd) = as_str(&arr[0])
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

                                            let mut vec = match lock.remove(&key) {
                                                Some(val) => as_vec(val.value).unwrap(),
                                                None => vec![],
                                            };
                                            vec.extend_from_slice(&arr[2..]);
                                            let vec_len = vec.len();

                                            lock.insert(
                                                key,
                                                Value {
                                                    value: RESPValue::Array(Some(vec)),
                                                    expires_at: None,
                                                },
                                            );

                                            RESPValue::Integer(vec_len as i64)
                                        }
                                        "lpush" if arr.len() > 2 => {
                                            let key = arr[1].clone();
                                            let mut lock = loc_store.lock().unwrap();

                                            let vec = match lock.remove(&key) {
                                                Some(val) => as_vec(val.value).unwrap(),
                                                None => vec![],
                                            };
                                            let mut vec2 = arr[2..].to_vec();
                                            vec2.reverse();
                                            vec2.extend(vec);
                                            let vec_len = vec2.len();

                                            lock.insert(
                                                key,
                                                Value {
                                                    value: RESPValue::Array(Some(vec2)),
                                                    expires_at: None,
                                                },
                                            );

                                            RESPValue::Integer(vec_len as i64)
                                        }
                                        "llen" if arr.len() > 1 => {
                                            let key = arr[1].clone();
                                            let lock = loc_store.lock().unwrap();

                                            let vec_len = match lock.get(&key) {
                                                Some(val) => as_vec(val.value.clone()).unwrap().len(),
                                                None => 0,
                                            };

                                            RESPValue::Integer(vec_len as i64)
                                        }
                                        "lrange" if arr.len() > 3 => {
                                            let key = arr[1].clone();
                                            let lock = loc_store.lock().unwrap();
                                            match lock.get(&key) {
                                                Some(val) => {
                                                    let vec = as_vec(val.value.clone()).unwrap();

                                                    let start: usize = {
                                                        let s: i64 = as_str(&arr[2])
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
                                                        let s = as_str(&arr[3])
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
                                                        RESPValue::Array(Some(vec![]))
                                                    } else {
                                                        RESPValue::Array(Some(
                                                            vec[start..=stop].to_vec(),
                                                        ))
                                                    }
                                                }
                                                None => RESPValue::Array(Some(vec![])),
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
