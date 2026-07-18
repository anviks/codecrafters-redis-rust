use crate::{
    commands::execute_command,
    connection::Connection,
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

async fn communicate(conn: &mut Connection, message: &RESPValue) {
    conn.stream.write_all(&encode(message)).await.ok();
    conn.read_frame().await;
}

fn parse_command(frame: RESPValue) -> Option<(String, Vec<RESPValue>)> {
    if let Some(arr) = frame.as_vec()
        && !arr.is_empty()
        && let Some(command) = arr[0].as_str()
    {
        Some((command.to_lowercase(), arr.clone()))
    } else {
        None
    }
}

async fn handle_client(mut conn: Connection, store: SharedStore, is_replica: bool) {
    loop {
        let Some((frame, _)) = conn.read_frame().await else {
            break;
        };

        let Some((cmd, argv)) = parse_command(frame) else {
            continue;
        };

        match conn.dispatch(cmd, argv, &store, is_replica).await {
            Some(respvalue) => {
                if conn.stream.write_all(&encode(&respvalue)).await.is_err() {
                    break;
                }
            }
            None => {}
        }
    }
}

async fn handle_master(mut conn: Connection, store: SharedStore, is_replica: bool) {
    let mut offset = 0;
    loop {
        let Some((frame, consumed)) = conn.read_frame().await else {
            break;
        };

        offset += consumed;

        if let Some((cmd, argv)) = parse_command(frame) {
            if cmd == "replconf"
                && argv
                    .get(1)
                    .and_then(|a| a.as_str())
                    .is_some_and(|s| s.eq_ignore_ascii_case("getack"))
            {
                let reply = array(vec![
                    "REPLCONF".to_string(),
                    "ACK".to_string(),
                    offset.to_string(),
                ]);
                conn.stream.write_all(&encode(&reply)).await.ok();
            } else {
                execute_command(&cmd, &argv, &store, is_replica).await.ok();
            }
        };
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
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut conn = Connection::new(stream);

        communicate(
            &mut conn,
            &array(vec![RESPValue::BulkString(Some(b"PING".to_vec()))]),
        )
        .await;

        communicate(
            &mut conn,
            &array(vec![
                "REPLCONF".to_string(),
                "listening-port".to_string(),
                args.port.to_string(),
            ]),
        )
        .await;

        communicate(
            &mut conn,
            &array(vec![
                "REPLCONF".to_string(),
                "capa".to_string(),
                "psync2".to_string(),
            ]),
        )
        .await;

        communicate(
            &mut conn,
            &array(vec!["PSYNC".to_string(), "?".to_string(), "-1".to_string()]),
        )
        .await;

        conn.read_rdb().await;

        tokio::spawn(handle_master(conn, Arc::clone(&store), is_replica));
    }

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let loc_store = Arc::clone(&store);
                let conn = Connection::new(stream);
                tokio::spawn(handle_client(conn, loc_store, is_replica));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
