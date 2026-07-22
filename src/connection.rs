use crate::{
    SharedConfig,
    commands::{arg_bytes, arg_str, arg_uint, execute_command},
    common::CmdError,
    resp::{RESPValue, array, array_of, encode, resp_result, try_decode},
    store::{Replica, SharedStore},
};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
};

fn is_write_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "set" | "del" | "incr" | "rpush" | "lpush" | "lpop" | "xadd"
    )
}

pub(crate) struct Connection {
    pub(crate) id: u64,
    pub(crate) stream: TcpStream,
    buf: Vec<u8>,
    cmd_queue: Vec<(String, Vec<RESPValue>)>,
    in_transaction: bool,
    pub(crate) subscribed_channels: HashSet<Vec<u8>>,
    pub(crate) subscription_sender: Option<mpsc::UnboundedSender<Vec<u8>>>,
    pub(crate) username: Option<Vec<u8>>,
}

impl Connection {
    pub(crate) fn new(stream: TcpStream, id: u64) -> Self {
        Self {
            id,
            stream,
            buf: vec![],
            cmd_queue: vec![],
            in_transaction: false,
            subscribed_channels: HashSet::new(),
            subscription_sender: None,
            username: None,
        }
    }

    pub(crate) async fn read_frame(&mut self) -> Option<(RESPValue, usize)> {
        let mut chunk = [0; 512];

        loop {
            match try_decode(&self.buf) {
                Ok(Some((parsed, consumed))) => {
                    self.buf.drain(..consumed);
                    println!("Received something from some stream: {parsed}");
                    return Some((parsed, consumed));
                }
                Ok(None) => {}
                Err(err) => {
                    let output = encode(&RESPValue::SimpleError(err.to_string()));
                    eprintln!("Error when trying to read frame: {}", err);
                    self.stream.write_all(&output).await.ok();
                    return None;
                }
            }

            match self.stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
            }
        }
    }

    pub(crate) async fn read_rdb(&mut self) -> Option<()> {
        let mut chunk = [0; 512];

        loop {
            // find end of "$<len>\r\n" header
            if let Some(nl) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let len: usize = std::str::from_utf8(&self.buf[1..nl]).ok()?.parse().ok()?;
                let start = nl + 2;
                if self.buf.len() >= start + len {
                    self.buf.drain(..start + len); // throw header + payload away
                    return Some(());
                }
            }

            match self.stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
            }
        }
    }

    pub(crate) async fn dispatch(
        &mut self,
        cmd: String,
        argv: Vec<RESPValue>,
        store: &SharedStore,
        config: &SharedConfig,
    ) -> Result<Option<RESPValue>, CmdError> {
        let in_sub_mode = !self.subscribed_channels.is_empty();
        if in_sub_mode
            && ![
                "subscribe",
                "unsubscribe",
                "psubscribe",
                "punsubscribe",
                "ping",
                "quit",
            ]
            .contains(&cmd.as_str())
        {
            return Err(CmdError::NotSubModeCmd(cmd));
        }

        match cmd.as_str() {
            "auth" => {
                let username = arg_bytes(&argv, 1)?;
                let password = arg_bytes(&argv, 2)?;

                let lock = store.lock().unwrap();
                if let Some(passwords) = lock.users.get(username)
                    && (passwords.is_empty()
                        || passwords.contains(&Sha256::digest(password).into()))
                {
                    self.username = Some(username.clone());
                    Ok(Some(RESPValue::SimpleString("OK".to_string())))
                } else {
                    Err(CmdError::WrongPass)
                }
            }
            "ping" => {
                if in_sub_mode {
                    Ok(Some(array(vec!["pong", ""])))
                } else {
                    Ok(Some(RESPValue::SimpleString("PONG".to_string())))
                }
            }
            "subscribe" => {
                let key = arg_bytes(&argv, 1)?;
                self.subscribed_channels.insert(key.clone());

                store
                    .lock()
                    .unwrap()
                    .channel_subscriptions
                    .entry(key.clone())
                    .or_default()
                    .insert(
                        self.id,
                        self.subscription_sender.clone().expect(
                            "subscription_sender should be set when Connection::dispatch is called",
                        ),
                    );

                Ok(Some(array_of(vec![
                    "subscribe".into(),
                    key.clone().into(),
                    (self.subscribed_channels.len() as i64).into(),
                ])))
            }
            "unsubscribe" => {
                let key = arg_bytes(&argv, 1)?;
                self.subscribed_channels.remove(key);

                if let Some(subs) = store.lock().unwrap().channel_subscriptions.get_mut(key) {
                    subs.remove(&self.id);
                }

                Ok(Some(array_of(vec![
                    "unsubscribe".to_string().into(),
                    key.clone().into(),
                    (self.subscribed_channels.len() as i64).into(),
                ])))
            }
            "exec" => {
                if !self.in_transaction {
                    Err(CmdError::ExecWithoutMulti)
                } else {
                    let mut results = vec![];
                    for (cmd, argv) in &self.cmd_queue {
                        results.push(resp_result(
                            execute_command(cmd, argv, store, config, &self.username).await,
                        ));
                    }

                    self.cmd_queue.clear();
                    self.in_transaction = false;

                    Ok(Some(array(results)))
                }
            }
            "multi" => {
                if self.in_transaction {
                    Err(CmdError::NestedMulti)
                } else {
                    self.in_transaction = true;
                    Ok(Some(RESPValue::SimpleString("OK".to_string())))
                }
            }
            "discard" => {
                if !self.in_transaction {
                    Err(CmdError::DiscardWithoutMulti)
                } else {
                    self.in_transaction = false;
                    self.cmd_queue.clear();
                    Ok(Some(RESPValue::SimpleString("OK".to_string())))
                }
            }
            "psync" => {
                let resp = RESPValue::SimpleString(
                    "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".to_string(),
                );
                let output = encode(&resp);
                self.stream.write_all(&output).await.ok();
                let empty_rdb = fs::read("empty.rdb").unwrap();
                let mut output = format!("${}\r\n", empty_rdb.len()).as_bytes().to_vec();
                output.extend(empty_rdb);
                self.stream.write_all(&output).await.ok();

                let (sender, mut receiver) = mpsc::unbounded_channel();
                let ack_offset = Arc::new(AtomicU64::new(0));
                store.lock().unwrap().replicas.push(Replica {
                    sender,
                    offset: Arc::clone(&ack_offset),
                });

                // "Permanently" park this task here to handle communication to and from a specific replica
                loop {
                    tokio::select! {
                        // outbound: a propagated command to forward
                        maybe_bytes = receiver.recv() => {
                            match maybe_bytes {
                                Some(bytes) => if self.stream.write_all(&bytes).await.is_err() { break; },
                                None => break,
                            }
                        }
                        // inbound: the replica sent us something
                        frame = self.read_frame() => {
                            match frame {
                                Some((resp, _)) => {
                                    if let Some(arr) = resp.as_vec()
                                        && let Ok(s0) = arg_str(arr, 0)
                                        && s0.eq_ignore_ascii_case("replconf")
                                        && let Ok(s1) = arg_str(arr, 1)
                                        && s1.eq_ignore_ascii_case("ack")
                                        && let Ok(offset) = arg_uint(arr, 2)
                                    {
                                        ack_offset.store(offset, Ordering::Relaxed);
                                    }
                                },
                                None => break,
                            }
                        }
                    }
                }

                return Ok(None);
            }
            _ if self.in_transaction => {
                self.cmd_queue.push((cmd, argv));
                Ok(Some(RESPValue::SimpleString("QUEUED".to_string())))
            }
            _ => {
                let result = execute_command(&cmd, &argv, store, config, &self.username).await;
                if is_write_command(&cmd) {
                    let encoded = encode(&RESPValue::Array(Some(argv.clone())));
                    let mut store = store.lock().unwrap();
                    store.master_offset += encoded.len() as u64;
                    for replica in &store.replicas {
                        replica.sender.send(encoded.clone()).ok();
                    }
                }
                result.map(|resp| Some(resp))
            }
        }
    }
}
