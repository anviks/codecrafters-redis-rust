use crate::{
    commands::execute_command,
    connection::{Conn, Flow},
    resp::{RESPValue, array, encode, try_decode},
    store::{SharedStore, Store},
};
use clap::Parser;
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
mod commands;
mod connection;
mod resp;
mod store;
mod stream;

async fn communicate(stream: &mut TcpStream, message: &RESPValue) {
    stream.write_all(&encode(message)).await.ok();

    let mut buf = [0; 512];
    match stream.read(&mut buf).await {
        Ok(0) => {}
        Ok(n) => {
            let parsed = try_decode(&buf[..n]);
            println!("{parsed:?}");
        }
        Err(_) => {}
    }
}

fn parse_command(frame: RESPValue) -> Option<(String, Vec<RESPValue>)> {
    if let RESPValue::Array(array) = frame
        && let Some(arr) = array
        && !arr.is_empty()
        && let Some(command) = arr[0].as_str()
    {
        Some((command.to_lowercase(), arr))
    } else {
        None
    }
}

async fn handle_client(mut conn: Conn, store: SharedStore, is_replica: bool) {
    loop {
        let Some(frame) = conn.read_frame().await else {
            break;
        };

        let Some((cmd, argv)) = parse_command(frame) else {
            continue;
        };

        match conn.dispatch(cmd, argv, &store, is_replica).await {
            Flow::Reply(respvalue) => {
                if conn.stream.write_all(&encode(&respvalue)).await.is_err() {
                    break;
                }
            }
            Flow::Silent => {}
            Flow::Close => return,
        }
    }
}

async fn handle_master(mut conn: Conn, store: SharedStore, is_replica: bool) {
    loop {
        let Some(frame) = conn.read_frame().await else {
            break;
        };

        let Some((cmd, argv)) = parse_command(frame) else {
            continue;
        };

        execute_command(&cmd, &argv, &store, is_replica).await.ok();
    }
}

#[derive(Parser, Clone, Debug)]
struct Args {
    #[arg(long, default_value_t = 6379)]
    port: u16,

    #[arg(long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let is_replica = args.replicaof.is_some();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port))
        .await
        .unwrap();

    let store: SharedStore = Arc::new(Mutex::new(Store::new()));

    let master_addr = args.replicaof.as_ref().map(|s| s.replace(" ", ":"));
    if let Some(addr) = master_addr {
        let mut stream = TcpStream::connect(addr).await.unwrap();

        communicate(
            &mut stream,
            &array(vec![RESPValue::BulkString(Some(b"PING".to_vec()))]),
        )
        .await;

        communicate(
            &mut stream,
            &array(vec![
                "REPLCONF".to_string(),
                "listening-port".to_string(),
                args.port.to_string(),
            ]),
        )
        .await;

        communicate(
            &mut stream,
            &array(vec![
                "REPLCONF".to_string(),
                "capa".to_string(),
                "psync2".to_string(),
            ]),
        )
        .await;

        communicate(
            &mut stream,
            &array(vec!["PSYNC".to_string(), "?".to_string(), "-1".to_string()]),
        )
        .await;

        let mut conn = Conn::new(stream);
        conn.read_rdb().await;

        tokio::spawn(handle_master(conn, Arc::clone(&store), is_replica));
    }

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let loc_store = Arc::clone(&store);
                let conn = Conn::new(stream);
                tokio::spawn(handle_client(conn, loc_store, is_replica));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
