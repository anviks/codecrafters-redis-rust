use crate::resp::{RESPValue, decode, encode};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
mod resp;

#[tokio::main]
async fn main() {
    let store: Arc<Mutex<HashMap<RESPValue, RESPValue>>> = Arc::new(Mutex::new(HashMap::new()));
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
                                if let RESPValue::Array(arr) = parsed {
                                    if let RESPValue::BulkString(cmd) = &arr[0]
                                        && arr.len() > 1
                                        && let RESPValue::BulkString(value) = &arr[1]
                                    {
                                        match cmd.to_lowercase().as_str() {
                                            "echo" => {
                                                let output = encode(&arr[1]);

                                                if let Err(_) = stream.write_all(&output).await {
                                                    break;
                                                }

                                                continue;
                                            }
                                            "get" => {
                                                let key = &arr[1];
                                                let value = {
                                                    let lock = loc_store.lock().unwrap();
                                                    lock.get(key)
                                                        .cloned()
                                                        .unwrap_or(RESPValue::Null)
                                                };
                                                let output = encode(&value);

                                                if let Err(_) = stream.write_all(&output).await {
                                                    break;
                                                }

                                                continue;
                                            }
                                            "set" => {
                                                let key = arr[1].clone();
                                                let value = arr[2].clone();
                                                loc_store.lock().unwrap().insert(key, value);
                                                let output = encode(&RESPValue::SimpleString(
                                                    "OK".to_string(),
                                                ));

                                                if let Err(_) = stream.write_all(&output).await {
                                                    break;
                                                }

                                                continue;
                                            }
                                            _ => {}
                                        }
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
