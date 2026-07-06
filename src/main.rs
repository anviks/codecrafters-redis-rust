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
                                {
                                    if let RESPValue::BulkString(command) = &arr[0]
                                        && let Some(cmd) = command
                                        && arr.len() > 1
                                    {
                                        let response = match cmd.to_lowercase().as_str() {
                                            "echo" => arr[1].clone(),
                                            "get" => {
                                                let key = &arr[1];
                                                let value = {
                                                    let mut lock = loc_store.lock().unwrap();

                                                    match lock.get(key) {
                                                        Some(val)
                                                            if val
                                                                .expires_at
                                                                .map_or(false, |inst| {
                                                                    Instant::now() >= inst
                                                                }) =>
                                                        {
                                                            lock.remove(key);
                                                            RESPValue::BulkString(None)
                                                        }
                                                        Some(val) => val.value.clone(),
                                                        None => RESPValue::BulkString(None),
                                                    }
                                                };
                                                value
                                            }
                                            "set" => {
                                                let key = arr[1].clone();
                                                let value = {
                                                    let val = arr[2].clone();

                                                    let expiry = if arr.len() > 4
                                                        && let RESPValue::BulkString(str) = &arr[3]
                                                        && let Some(s) = str
                                                        && let RESPValue::BulkString(int) = &arr[4]
                                                        && let Some(i) = int
                                                    {
                                                        match s.to_lowercase().as_str() {
                                                            "ex" => Some(Instant::now().add(
                                                                Duration::from_secs(
                                                                    i.parse().unwrap(),
                                                                ),
                                                            )),
                                                            "px" => Some(Instant::now().add(
                                                                Duration::from_millis(
                                                                    i.parse().unwrap(),
                                                                ),
                                                            )),
                                                            _ => None,
                                                        }
                                                    } else {
                                                        None
                                                    };

                                                    Value {
                                                        value: val,
                                                        expires_at: expiry,
                                                    }
                                                };

                                                loc_store.lock().unwrap().insert(key, value);
                                                RESPValue::SimpleString("OK".to_string())
                                            }
                                            _ => RESPValue::SimpleError(format!(
                                                "ERR unknown comand '{}'",
                                                cmd
                                            )),
                                        };

                                        let output = encode(&response);

                                        if let Err(_) = stream.write_all(&output).await {
                                            break;
                                        }

                                        continue;
                                    }
                                }
                                if let Err(_) = stream.write_all(b"+PONG\r\n").await {
                                    break;
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
