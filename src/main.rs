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
                            Ok(_) => {
                                let parsed = decode(&buf);
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
