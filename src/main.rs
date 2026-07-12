use crate::{
    resp::{CmdError, RESPValue, array, array_of, decode, encode, resp_result},
    stream::{Stream, StreamEntry, StreamId},
};
use clap::Parser;
use std::{
    collections::{HashMap, VecDeque},
    ops::Add,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
    u64,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream},
    sync::oneshot,
};
mod resp;
mod stream;

#[derive(Clone, Debug)]
enum Data {
    String(String),
    List(VecDeque<String>),
    Stream(Stream),
}

impl Data {
    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Data::String(s) => Some(s),
            _ => None,
        }
    }

    pub(crate) fn as_vec(&self) -> Option<&VecDeque<String>> {
        match self {
            Data::List(vec) => Some(vec),
            _ => None,
        }
    }

    pub(crate) fn as_vec_mut(&mut self) -> Option<&mut VecDeque<String>> {
        match self {
            Data::List(vec) => Some(vec),
            _ => None,
        }
    }

    pub(crate) fn as_stream(&self) -> Option<&Stream> {
        match self {
            Data::Stream(stream) => Some(stream),
            _ => None,
        }
    }

    pub(crate) fn as_stream_mut(&mut self) -> Option<&mut Stream> {
        match self {
            Data::Stream(stream) => Some(stream),
            _ => None,
        }
    }

    pub(crate) fn try_str(&self) -> Result<&str, CmdError> {
        self.as_str().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_vec(&self) -> Result<&VecDeque<String>, CmdError> {
        self.as_vec().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_vec_mut(&mut self) -> Result<&mut VecDeque<String>, CmdError> {
        self.as_vec_mut().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_stream(&self) -> Result<&Stream, CmdError> {
        self.as_stream().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_stream_mut(&mut self) -> Result<&mut Stream, CmdError> {
        self.as_stream_mut().ok_or(CmdError::WrongType)
    }

    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            Data::String(_) => "string",
            Data::List(_) => "list",
            Data::Stream(_) => "stream",
        }
    }
}

#[derive(Clone, Debug)]
struct Value {
    data: Data,
    expires_at: Option<Instant>,
}

fn arg(arr: &[RESPValue], i: usize) -> Result<&str, CmdError> {
    let arg = arr.get(i).ok_or(CmdError::WrongArgs)?;
    arg.try_str()
}

fn arg_int(arr: &[RESPValue], i: usize) -> Result<i64, CmdError> {
    arg(arr, i)?.parse().map_err(|_| CmdError::NotInt)
}

fn arg_uint(arr: &[RESPValue], i: usize) -> Result<u64, CmdError> {
    arg(arr, i)?.parse().map_err(|_| CmdError::NotUint)
}

fn arg_double(arr: &[RESPValue], i: usize) -> Result<f64, CmdError> {
    arg(arr, i)?.parse().map_err(|_| CmdError::NotDouble)
}

fn to_strings(arr: &[RESPValue]) -> Result<Vec<String>, CmdError> {
    arr.iter()
        .map(|resp| resp.try_str().map(str::to_string))
        .collect()
}

fn parse_expiry(args: &[RESPValue]) -> Option<Instant> {
    let str = arg(args, 0).ok()?;
    let uint = arg_uint(args, 1).ok()?;

    match str.to_lowercase().as_str() {
        "ex" => Some(Instant::now().add(Duration::from_secs(uint))),
        "px" => Some(Instant::now().add(Duration::from_millis(uint))),
        _ => None,
    }
}

fn cmd_lpop(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let mut lock = store.lock().unwrap();

    match lock.entries.get_mut(key) {
        Some(val) => {
            let vec = val.data.try_vec_mut()?;

            if arr.len() > 2 {
                let amount = (arg_int(&arr, 2)? as usize).min(vec.len());
                Ok(array(vec.drain(..amount)))
            } else {
                Ok(if vec.is_empty() {
                    RESPValue::BulkString(None)
                } else {
                    vec.pop_front().unwrap().into()
                })
            }
        }
        None => Ok(RESPValue::BulkString(None)),
    }
}

fn cmd_lpush(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let mut lock = store.lock().unwrap();

    let vec_len = match lock.entries.get_mut(key) {
        Some(val) => {
            let vec = val.data.try_vec_mut()?;
            for v in to_strings(&arr[2..])? {
                vec.push_front(v);
            }
            vec.len()
        }
        None => {
            let mut vec = to_strings(&arr[2..])?;
            vec.reverse();
            let len = vec.len();
            lock.entries.insert(
                key.to_string(),
                Value {
                    data: Data::List(vec.into()),
                    expires_at: None,
                },
            );
            len
        }
    };

    Ok(RESPValue::Integer(vec_len as i64))
}

fn cmd_llen(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let lock = store.lock().unwrap();

    let vec_len = match lock.entries.get(key) {
        Some(val) => val.data.try_vec()?.len(),
        None => 0,
    };

    Ok(RESPValue::Integer(vec_len as i64))
}

fn cmd_lrange(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let lock = store.lock().unwrap();
    match lock.entries.get(key) {
        Some(val) => {
            let vec = val.data.try_vec()?;
            if vec.is_empty() {
                return Ok(array_of(vec![]));
            }

            let start: usize = {
                let s = arg_int(&arr, 2)?;

                if s < 0 {
                    (vec.len() as i64 + s).max(0) as usize
                } else {
                    s as usize
                }
            };

            let stop: usize = {
                let s = arg_int(&arr, 3)?.min((vec.len() - 1) as i64);

                if s < 0 {
                    (vec.len() as i64 + s).max(0) as usize
                } else {
                    s as usize
                }
            };

            if start > stop || start >= vec.len() {
                Ok(array_of(vec![]))
            } else {
                Ok(array(
                    vec.range(start..=stop).into_iter().map(|s| s.clone()),
                ))
            }
        }
        None => Ok(array_of(vec![])),
    }
}

fn cmd_get(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let mut lock = store.lock().unwrap();
    match lock.entries.get(key) {
        Some(val) if val.expires_at.map_or(false, |inst| Instant::now() >= inst) => {
            lock.entries.remove(key);
            Ok(RESPValue::BulkString(None))
        }
        Some(val) => match &val.data {
            Data::String(s) => Ok(s.clone().into()),
            _ => Err(CmdError::WrongType),
        },
        None => Ok(RESPValue::BulkString(None)),
    }
}

fn cmd_set(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let value = Value {
        data: Data::String(arg(&arr, 2)?.to_string()),
        expires_at: parse_expiry(&arr[3..]),
    };

    store.lock().unwrap().entries.insert(key.to_string(), value);
    Ok(RESPValue::SimpleString("OK".to_string()))
}

fn cmd_rpush(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let mut values: VecDeque<String> = to_strings(&arr[2..])?.into();
    let mut sent_values = 0;
    let mut lock = store.lock().unwrap();

    if let Some(waiters) = lock.blpop_waiters.get_mut(key) {
        while let Some(val) = values.pop_front() {
            let Some(w) = waiters.pop_front() else {
                values.push_front(val);
                break;
            };

            if let Err(returned) = w.send(val) {
                values.push_front(returned);
                continue;
            }

            sent_values += 1;
        }
    };

    let vec_len = match lock.entries.get_mut(key) {
        Some(val) => {
            let vec = val.data.try_vec_mut()?;
            vec.extend(values);
            vec.len()
        }
        None => {
            let len = values.len();
            lock.entries.insert(
                key.to_string(),
                Value {
                    data: Data::List(VecDeque::from(values)),
                    expires_at: None,
                },
            );
            len
        }
    };

    Ok(RESPValue::Integer((vec_len + sent_values) as i64))
}

async fn cmd_blpop(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let timeout = arg_double(&arr, 2)?;

    let receiver = {
        let mut lock = store.lock().unwrap();

        if let Some(val) = lock.entries.get_mut(key) {
            let vec = val.data.try_vec_mut()?;
            if let Some(v) = vec.pop_front() {
                return Ok(array(vec![key.to_string(), v]));
            }
        }

        let (sender, receiver) = oneshot::channel();
        lock.blpop_waiters
            .entry(key.to_string())
            .or_default()
            .push_back(sender);
        receiver
    };

    if timeout > 0.0 {
        let duration = Duration::from_secs_f64(timeout);
        Ok(match tokio::time::timeout(duration, receiver).await {
            Ok(Ok(value)) => array(vec![key.to_string(), value]),
            _ => RESPValue::Array(None),
        })
    } else {
        Ok(receiver
            .await
            .map(|value| array(vec![key.to_string(), value]))
            .unwrap_or(RESPValue::Array(None)))
    }
}

fn cmd_type(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    Ok(match store.lock().unwrap().entries.get(key) {
        Some(Value {
            data: value,
            expires_at: _,
        }) => RESPValue::SimpleString(value.type_name().to_string()),
        None => RESPValue::SimpleString("none".to_string()),
    })
}

fn cmd_xadd(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let entry_id = arg(&arr, 2)?;
    arg(&arr, 3)?;

    let mut fields = vec![];
    for i in (3..arr.len()).step_by(2) {
        let field = arg(&arr, i)?.to_string();
        let value = arg(&arr, i + 1)?.to_string();
        fields.push((field, value));
    }

    let mut lock = store.lock().unwrap();
    let val = lock.entries.entry(key.to_string()).or_insert(Value {
        data: Data::Stream(Stream {
            entries: vec![],
            last_id: StreamId { ms: 0, seq: 0 },
        }),
        expires_at: None,
    });
    let stream = val.data.try_stream_mut()?;

    let id = stream.last_id.next_from_str(entry_id)?;

    stream.entries.push(StreamEntry { id, fields });
    stream.last_id = id;

    if let Some(waiter_ids) = lock.xread_waiters_by_key.get(key) {
        for w_id in waiter_ids.clone() {
            if let Some(waiter) = lock.xread_waiters.remove(&w_id) {
                waiter.send(()).ok();
            }
        }
    }

    Ok(id.to_string().into())
}

fn cmd_xrange(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let start = {
        let s = arg(&arr, 2)?;
        if s == "-" {
            StreamId { ms: 0, seq: 0 }
        } else if s.contains("-") {
            s.parse()?
        } else {
            StreamId {
                ms: arg_uint(&arr, 2)?,
                seq: 0,
            }
        }
    };
    let end = {
        let s = arg(&arr, 3)?;
        if s == "+" {
            StreamId {
                ms: u64::MAX,
                seq: u64::MAX,
            }
        } else if s.contains("-") {
            s.parse()?
        } else {
            StreamId {
                ms: arg_uint(&arr, 3)?,
                seq: u64::MAX,
            }
        }
    };

    let lock = store.lock().unwrap();
    match lock.entries.get(key) {
        Some(value) => {
            let stream = value.data.try_stream()?;
            let entries = array(
                stream
                    .entries
                    .iter()
                    .filter(|e| start <= e.id && e.id <= end)
                    .map(|e| {
                        array(vec![
                            e.id.to_string().into(),
                            array(
                                e.fields
                                    .iter()
                                    .flat_map(|(k, v)| vec![k.clone(), v.clone()]),
                            ),
                        ])
                    }),
            );
            Ok(entries)
        }
        None => Ok(RESPValue::BulkString(None)),
    }
}

fn filter_stream_entries(
    lock: &Store,
    stream_keys: &Vec<String>,
    stream_ids: &Vec<StreamId>,
) -> Result<Vec<RESPValue>, CmdError> {
    let mut result = vec![];
    for (key, id) in stream_keys.iter().zip(stream_ids) {
        match lock.entries.get(key) {
            Some(value) => {
                let stream = value.data.try_stream()?;
                let filtered: Vec<RESPValue> = stream
                    .entries
                    .iter()
                    .filter(|e| *id < e.id)
                    .map(|e| {
                        array(vec![
                            e.id.to_string().into(),
                            array(
                                e.fields
                                    .iter()
                                    .flat_map(|(k, v)| vec![k.clone(), v.clone()]),
                            ),
                        ])
                    })
                    .collect();
                if filtered.is_empty() {
                    continue;
                }
                result.push(array(vec![key.clone().into(), array(filtered)]));
            }
            None => continue,
        };
    }

    Ok(result)
}

async fn xread_worker(
    store: &SharedStore,
    stream_keys: &Vec<String>,
    stream_ids: &Vec<StreamId>,
) -> Result<RESPValue, CmdError> {
    loop {
        let receiver = {
            let mut lock = store.lock().unwrap();
            let result = filter_stream_entries(&lock, stream_keys, stream_ids)?;
            if !result.is_empty() {
                return Ok(array(result));
            }

            let (sender, receiver) = oneshot::channel();
            let waiter_id = lock.add_xread_waiter(sender);

            for key in stream_keys {
                lock.add_key_for_xread_waiter(key.clone(), waiter_id);
            }

            receiver
        };

        receiver.await.ok();
    }
}

async fn cmd_xread(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let mut block_arg = None;
    let mut i = 1;
    loop {
        let argument = arg(&arr, i)?;
        i += 1;
        match argument.to_lowercase().as_str() {
            "streams" => {
                break;
            }
            "block" => {
                block_arg = Some(arg_uint(&arr, i)?);
                i += 1;
            }
            _ => {}
        }
    }

    let arg_count = arr.len() - i;
    if arg_count == 0 || arg_count % 2 == 1 {
        return Err(CmdError::WrongArgs);
    }
    let pair_count = arg_count / 2;
    let stream_keys = arr[i..i + pair_count]
        .iter()
        .map(|k| k.try_str().map(str::to_string))
        .collect::<Result<Vec<String>, CmdError>>()?;
    let stream_ids = arr[i + pair_count..]
        .iter()
        .enumerate()
        .map(|(index, id)| {
            id.try_str().and_then(|s| {
                if s == "$" {
                    let lock = store.lock().unwrap();
                    match lock.entries.get(&stream_keys[index]) {
                        Some(val) => Ok(val.data.try_stream()?.last_id),
                        None => Ok(StreamId { ms: 0, seq: 0 }),
                    }
                } else {
                    s.parse()
                }
            })
        })
        .collect::<Result<Vec<StreamId>, CmdError>>()?;

    let block_ms = {
        let lock = store.lock().unwrap();
        let result = filter_stream_entries(&lock, &stream_keys, &stream_ids)?;

        if !result.is_empty() {
            return Ok(array(result));
        }

        let Some(block_ms) = block_arg else {
            return Ok(array(result));
        };

        block_ms
    };

    if block_ms > 0 {
        let duration = Duration::from_millis(block_ms);
        Ok(
            match tokio::time::timeout(duration, xread_worker(store, &stream_keys, &stream_ids))
                .await
            {
                Ok(Ok(value)) => value,
                _ => RESPValue::Array(None),
            },
        )
    } else {
        Ok(xread_worker(store, &stream_keys, &stream_ids)
            .await
            .unwrap_or(RESPValue::Array(None)))
    }
}

fn cmd_incr(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg(&arr, 1)?;
    let mut lock = store.lock().unwrap();

    let value = lock.entries.entry(key.to_string()).or_insert(Value {
        data: Data::String("0".to_string()),
        expires_at: None,
    });

    let new_num: i64 = value
        .data
        .try_str()?
        .parse::<i64>()
        .map_err(|_| CmdError::NotInt)?
        + 1;
    value.data = Data::String(new_num.to_string());

    Ok(RESPValue::Integer(new_num))
}

fn cmd_info(arr: &[RESPValue], store: &SharedStore, args: &Args) -> Result<RESPValue, CmdError> {
    let mut sections = HashMap::new();
    let role = if args.replicaof.is_some() {
        "slave"
    } else {
        "master"
    };
    sections.insert(
        "replication".to_string(),
        format!(
            "# Replication\nrole:{}\nconnected_slaves:0\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\n",
            role
        ),
    );

    if let Ok(s) = arg(&arr, 1) {
        match sections.get(&s.to_lowercase()) {
            Some(section) => Ok(section.to_string().into()),
            None => todo!(),
        }
    } else {
        Ok(sections
            .values()
            .map(String::clone)
            .collect::<Vec<String>>()
            .join("\n\n")
            .into())
    }
}

async fn execute_command(
    command: &str,
    arr: &[RESPValue],
    store: &SharedStore,
    args: &Args,
) -> Result<RESPValue, CmdError> {
    match command {
        "ping" => Ok(RESPValue::SimpleString("PONG".to_string())),
        "echo" if arr.len() > 1 => Ok(arr[1].clone()),
        "get" => cmd_get(&arr, &store),
        "set" => cmd_set(&arr, &store),
        "rpush" => cmd_rpush(&arr, &store),
        "lpush" => cmd_lpush(&arr, &store),
        "lpop" => cmd_lpop(&arr, &store),
        "llen" => cmd_llen(&arr, &store),
        "lrange" => cmd_lrange(&arr, &store),
        "blpop" => cmd_blpop(&arr, &store).await,
        "type" => cmd_type(&arr, &store),
        "xadd" => cmd_xadd(&arr, &store),
        "xrange" => cmd_xrange(&arr, &store),
        "xread" => cmd_xread(&arr, &store).await,
        "incr" => cmd_incr(&arr, &store),
        "info" => cmd_info(&arr, &store, &args),
        "replconf" => Ok(RESPValue::SimpleString("OK".to_string())),
        _ => Err(CmdError::Unknown),
    }
}

async fn communicate(stream: &mut TcpStream, message: &RESPValue) {
    stream.write_all(&encode(message)).await.ok();

    let mut buf = [0; 512];
    match stream.read(&mut buf).await {
        Ok(0) => {}
        Ok(n) => {
            let parsed = decode(&buf[..n]);
            println!("{parsed:?}");
        }
        Err(_) => {}
    }
}

struct Store {
    entries: HashMap<String, Value>,
    blpop_waiters: HashMap<String, VecDeque<oneshot::Sender<String>>>,
    xread_waiters: HashMap<u64, oneshot::Sender<()>>,
    xread_waiters_by_key: HashMap<String, VecDeque<u64>>,
    next_id: u64,
}

impl Store {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            blpop_waiters: HashMap::new(),
            xread_waiters: HashMap::new(),
            xread_waiters_by_key: HashMap::new(),
            next_id: 1,
        }
    }

    fn add_xread_waiter(&mut self, waiter: oneshot::Sender<()>) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.xread_waiters.insert(id, waiter);
        id
    }

    fn add_key_for_xread_waiter(&mut self, key: String, waiter_id: u64) {
        self.xread_waiters_by_key
            .entry(key)
            .or_default()
            .push_back(waiter_id);
    }
}

type SharedStore = Arc<Mutex<Store>>;

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

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port))
        .await
        .unwrap();
    let master_addr = args.replicaof.as_ref().map(|s| s.replace(" ", ":"));
    if let Some(addr) = master_addr {
        let mut stream = TcpStream::connect(addr).await.unwrap();

        communicate(
            &mut stream,
            &array(vec![RESPValue::BulkString(Some("PING".to_string()))]),
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
    }

    let store: SharedStore = Arc::new(Mutex::new(Store::new()));

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let loc_store = Arc::clone(&store);
                let args = args.clone();
                tokio::spawn(async move {
                    let mut cmd_queue: Vec<(String, Vec<RESPValue>)> = vec![];
                    let mut in_transaction = false;

                    loop {
                        let mut buf = [0; 512];
                        match stream.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                let parsed = decode(&buf[..n]);
                                println!("{parsed:?}");

                                let (cmd, argv) = {
                                    if let RESPValue::Array(array) = parsed
                                        && let Some(arr) = array
                                        && !arr.is_empty()
                                        && let Some(command) = arr[0].as_str()
                                    {
                                        (command.to_lowercase(), arr)
                                    } else {
                                        continue;
                                    }
                                };

                                let result: Result<RESPValue, CmdError> = match cmd.as_str() {
                                    "exec" => {
                                        if !in_transaction {
                                            Err(CmdError::ExecWithoutMulti)
                                        } else {
                                            let mut results = vec![];
                                            for (cmd, argv) in &cmd_queue {
                                                results.push(resp_result(
                                                    execute_command(cmd, argv, &loc_store, &args)
                                                        .await,
                                                ));
                                            }

                                            cmd_queue.clear();
                                            in_transaction = false;

                                            Ok(array(results))
                                        }
                                    }
                                    "multi" => {
                                        if in_transaction {
                                            Err(CmdError::NestedMulti)
                                        } else {
                                            in_transaction = true;
                                            Ok(RESPValue::SimpleString("OK".to_string()))
                                        }
                                    }
                                    "discard" => {
                                        if !in_transaction {
                                            Err(CmdError::DiscardWithoutMulti)
                                        } else {
                                            in_transaction = false;
                                            cmd_queue.clear();
                                            Ok(RESPValue::SimpleString("OK".to_string()))
                                        }
                                    }
                                    "psync" => {
                                        let resp = RESPValue::SimpleString(
                                            "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"
                                                .to_string(),
                                        );
                                        let output = encode(&resp);
                                        stream.write_all(&output).await.ok();
                                        stream.write_all("$0\r\n".as_bytes()).await.ok();
                                        continue;
                                    }
                                    _ if in_transaction => {
                                        cmd_queue.push((cmd, argv));
                                        Ok(RESPValue::SimpleString("QUEUED".to_string()))
                                    }
                                    _ => execute_command(&cmd, &argv, &loc_store, &args).await,
                                };

                                let output = encode(&resp_result(result));
                                if stream.write_all(&output).await.is_err() {
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
