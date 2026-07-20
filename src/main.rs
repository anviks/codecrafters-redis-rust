use crate::{
    commands::execute_command,
    common::{Config, SharedConfig},
    connection::Connection,
    rdb::parse_rdb,
    resp::{RESPValue, array, encode, resp_result},
    store::{SharedStore, Store},
};
use clap::Parser;
use std::{
    fs,
    path::PathBuf,
    process::exit,
    sync::{Arc, Mutex},
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
mod commands;
mod common;
mod connection;
mod rdb;
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

async fn handle_client(mut conn: Connection, store: SharedStore, config: SharedConfig) {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    conn.subscription_sender = Some(sender);

    loop {
        tokio::select! {
            frame = conn.read_frame() => {
                let Some((frame, _)) = frame else {
                    break;
                };

                let Some((cmd, argv)) = parse_command(frame) else {
                    continue;
                };

                match conn.dispatch(cmd, argv, &store, &config).await.transpose() {
                    Some(respvalue) => {
                        if conn
                            .stream
                            .write_all(&encode(&resp_result(respvalue)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    None => {}
                }
            }
            sub_msg = receiver.recv() => {
                match sub_msg {
                    Some(bytes) => {
                        if conn.stream.write_all(&bytes).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }

    let mut lock = store.lock().unwrap();
    for channel in &conn.subscribed_channels {
        if let Some(subs) = lock.channel_subscriptions.get_mut(channel) {
            subs.remove(&conn.id);
        }
    }
}

async fn handle_master(mut conn: Connection, store: SharedStore, config: SharedConfig) {
    let mut offset = 0;
    loop {
        let Some((frame, consumed)) = conn.read_frame().await else {
            break;
        };

        if let Some((cmd, argv)) = parse_command(frame) {
            if cmd == "replconf"
                && argv
                    .get(1)
                    .and_then(|a| a.as_str())
                    .is_some_and(|s| s.eq_ignore_ascii_case("getack"))
            {
                let reply = array(vec!["REPLCONF", "ACK", offset.to_string().as_str()]);
                conn.stream.write_all(&encode(&reply)).await.ok();
            } else {
                execute_command(&cmd, &argv, &store, &config).await.ok();
            }
        };

        offset += consumed;
    }
}

#[derive(Parser, Clone, Debug)]
struct Args {
    #[arg(long, default_value_t = 6379)]
    port: u16,

    #[arg(long)]
    replicaof: Option<String>,

    #[arg(long, default_value_t = String::from("."))]
    dir: String,

    #[arg(long, default_value_t = String::from("dump.rdb"))]
    dbfilename: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let config = Arc::new(Config {
        is_replica: args.replicaof.is_some(),
        dir: args.dir,
        dbfilename: args.dbfilename,
    });

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port))
        .await
        .unwrap();

    let store: SharedStore = Arc::new(Mutex::new(Store::new()));

    let master_addr = args.replicaof.as_ref().map(|s| s.replace(" ", ":"));
    if let Some(addr) = master_addr {
        let stream = TcpStream::connect(addr).await.unwrap();
        let conn_id = store.lock().unwrap().get_next_connection_id();
        let mut conn = Connection::new(stream, conn_id);

        communicate(
            &mut conn,
            &array(vec![RESPValue::BulkString(Some(b"PING".to_vec()))]),
        )
        .await;

        communicate(
            &mut conn,
            &array(vec![
                "REPLCONF",
                "listening-port",
                args.port.to_string().as_str(),
            ]),
        )
        .await;

        communicate(&mut conn, &array(vec!["REPLCONF", "capa", "psync2"])).await;
        communicate(&mut conn, &array(vec!["PSYNC", "?", "-1"])).await;

        conn.read_rdb().await;

        tokio::spawn(handle_master(conn, Arc::clone(&store), Arc::clone(&config)));
    } else {
        let mut path = PathBuf::from(&config.dir);
        if !path.exists() {
            if let Err(e) = fs::create_dir_all(&path) {
                eprintln!("{e}");
                exit(1);
            }
        }

        path.push(&config.dbfilename);

        if path.exists() {
            let rdb = fs::read(path).unwrap_or_else(|e| {
                eprintln!("{e}");
                exit(1);
            });

            store.lock().unwrap().entries = parse_rdb(&rdb);
        }
    }

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let conn_id = store.lock().unwrap().get_next_connection_id();
                tokio::spawn(handle_client(
                    Connection::new(stream, conn_id),
                    Arc::clone(&store),
                    Arc::clone(&config),
                ));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
