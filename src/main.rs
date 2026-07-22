use crate::{
    commands::execute_command,
    common::{Config, SharedConfig},
    connection::Connection,
    rdb::parse_rdb,
    resp::{RESPValue, array, encode, resp_result, try_decode},
    store::{SharedStore, Store},
};
use clap::Parser;
use std::{
    fs::{self, OpenOptions},
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
mod coordinates;
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
                execute_command(&cmd, &argv, &store, &config, &None)
                    .await
                    .ok();
            }
        };

        offset += consumed;
    }
}

fn parse_yes_no(s: &str) -> Result<bool, String> {
    match s.to_lowercase().as_str() {
        "yes" => Ok(true),
        "no" => Ok(false),
        _ => Err(format!("expected 'yes' or 'no', got '{s}'")),
    }
}

fn resolve_path(path: &str) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| PathBuf::from(path))
}

#[derive(Parser, Clone, Debug)]
struct Args {
    #[arg(long, default_value_t = 6379)]
    port: u16,

    #[arg(long)]
    replicaof: Option<String>,

    #[arg(long, default_value = ".")]
    dir: String,

    #[arg(long, default_value = "dump.rdb")]
    dbfilename: String,

    #[arg(long, default_value = "no", action = clap::ArgAction::Set, value_parser = parse_yes_no)]
    appendonly: bool,

    #[arg(long, default_value = "appendonlydir")]
    appenddirname: String,

    #[arg(long, default_value = "appendonly.aof")]
    appendfilename: String,

    #[arg(long, default_value = "everysec")]
    appendfsync: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let dir = resolve_path(&args.dir);
    let aof_dir_path = dir.join(&args.appenddirname);
    let aof_filename = args.appendfilename.clone() + ".1.incr.aof";
    let appendfilepath = aof_dir_path.join(&aof_filename);
    let manifest_filepath = aof_dir_path.join(args.appendfilename.clone() + ".manifest");

    let config = Arc::new(Config {
        is_replica: args.replicaof.is_some(),
        dir,
        dbfilename: args.dbfilename,
        appendonly: args.appendonly,
        appenddirname: args.appenddirname,
        appendfilename: args.appendfilename,
        appendfilepath: appendfilepath,
        appendfsync: args.appendfsync,
    });

    if !config.dir.exists() {
        if let Err(e) = fs::create_dir_all(&config.dir) {
            eprintln!("{e}");
            exit(1);
        }
    }

    let store: SharedStore = Arc::new(Mutex::new(Store::new()));

    if config.appendonly {
        if manifest_filepath.exists() {
            for line in fs::read_to_string(manifest_filepath).unwrap().split('\n') {
                if let ["file", filename, "seq", _, "type", "i"] =
                    line.split(' ').collect::<Vec<&str>>()[..]
                {
                    let command_bytes =
                        fs::read(aof_dir_path.join(filename)).expect("AOF file doesn't exist");

                    let mut offset = 0;
                    loop {
                        match try_decode(&command_bytes[offset..]).expect("Corrupted AOF file") {
                            Some((resp, consumed)) => {
                                if let Some((cmd, argv)) = parse_command(resp) {
                                    execute_command(&cmd, &argv, &store, &config, &None)
                                        .await
                                        .ok();
                                }
                                offset += consumed;
                            }
                            None => break,
                        }
                    }
                    break;
                };
            }
        } else if let Err(e) = fs::create_dir_all(&aof_dir_path)
            .and_then(|_| {
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&config.appendfilepath)
            })
            .and_then(|_| {
                fs::write(
                    manifest_filepath,
                    format!("file {} seq 1 type i\n", aof_filename),
                )
            })
        {
            eprintln!("{e}");
            exit(1);
        }
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port))
        .await
        .unwrap();

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
        let path = config.dir.join(&config.dbfilename);

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
                let mut conn = Connection::new(stream, conn_id);
                let lock = store.lock().unwrap();
                if let Some(passwords) = lock.users.get(&b"default".to_vec())
                    && passwords.is_empty()
                {
                    conn.username = Some(b"default".to_vec());
                }
                tokio::spawn(handle_client(conn, Arc::clone(&store), Arc::clone(&config)));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
