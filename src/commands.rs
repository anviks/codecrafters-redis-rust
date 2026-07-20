use crate::{
    SharedConfig,
    resp::{CmdError, RESPValue, array, array_of, encode},
    store::{Data, SharedStore, Store, Value},
    stream::{Stream, StreamEntry, StreamId},
};
use std::{
    collections::{HashMap, VecDeque},
    sync::atomic::Ordering,
    time::{Duration, SystemTime},
    u64,
};
use tokio::sync::oneshot;

fn to_bytes(arr: &[RESPValue]) -> Result<Vec<Vec<u8>>, CmdError> {
    arr.iter()
        .map(|resp| resp.try_bytes().map(Vec::clone))
        .collect()
}

pub(crate) fn arg(arr: &[RESPValue], i: usize) -> Result<&RESPValue, CmdError> {
    arr.get(i).ok_or(CmdError::WrongArgs)
}

pub(crate) fn arg_bytes(arr: &[RESPValue], i: usize) -> Result<&Vec<u8>, CmdError> {
    arg(arr, i)?.try_bytes()
}

pub(crate) fn arg_str(arr: &[RESPValue], i: usize) -> Result<&str, CmdError> {
    str::from_utf8(arg_bytes(arr, i)?).map_err(|_| CmdError::Syntax)
}

pub(crate) fn arg_int(arr: &[RESPValue], i: usize) -> Result<i64, CmdError> {
    str::from_utf8(arg_bytes(arr, i)?)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CmdError::NotInt)
}

pub(crate) fn arg_uint(arr: &[RESPValue], i: usize) -> Result<u64, CmdError> {
    str::from_utf8(arg_bytes(arr, i)?)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CmdError::NotUint)
}

pub(crate) fn arg_double(arr: &[RESPValue], i: usize) -> Result<f64, CmdError> {
    str::from_utf8(arg_bytes(arr, i)?)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CmdError::NotDouble)
}

fn parse_expiry(args: &[RESPValue]) -> Result<Option<SystemTime>, CmdError> {
    if args.is_empty() {
        return Ok(None);
    }

    let str = arg_str(args, 0)?;
    let uint = arg_uint(args, 1)?;

    match str.to_lowercase().as_str() {
        "ex" => Ok(Some(SystemTime::now() + Duration::from_secs(uint))),
        "px" => Ok(Some(SystemTime::now() + Duration::from_millis(uint))),
        _ => Err(CmdError::Syntax),
    }
}

fn cmd_lpop(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg_bytes(&arr, 1)?;
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
    let key = arg_bytes(&arr, 1)?;
    let mut lock = store.lock().unwrap();

    let vec_len = match lock.entries.get_mut(key) {
        Some(val) => {
            let vec = val.data.try_vec_mut()?;
            for v in to_bytes(&arr[2..])? {
                vec.push_front(v);
            }
            vec.len()
        }
        None => {
            let mut vec = to_bytes(&arr[2..])?;
            vec.reverse();
            let len = vec.len();
            lock.entries.insert(
                key.clone(),
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
    let key = arg_bytes(&arr, 1)?;
    let lock = store.lock().unwrap();

    let vec_len = match lock.entries.get(key) {
        Some(val) => val.data.try_vec()?.len(),
        None => 0,
    };

    Ok(RESPValue::Integer(vec_len as i64))
}

fn cmd_lrange(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg_bytes(&arr, 1)?;
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
                Ok(array(vec.range(start..=stop).map(|s| s.clone())))
            }
        }
        None => Ok(array_of(vec![])),
    }
}

fn cmd_get(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg_bytes(&arr, 1)?;
    let mut lock = store.lock().unwrap();
    match lock.entries.get(key) {
        Some(val)
            if val
                .expires_at
                .map_or(false, |inst| SystemTime::now() >= inst) =>
        {
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
    let key = arg_bytes(&arr, 1)?;
    let value = Value {
        data: Data::String(arg_bytes(&arr, 2)?.clone()),
        expires_at: parse_expiry(&arr[3..])?,
    };

    store.lock().unwrap().entries.insert(key.clone(), value);
    Ok(RESPValue::SimpleString("OK".to_string()))
}

fn cmd_rpush(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg_bytes(&arr, 1)?;
    let mut values: VecDeque<Vec<u8>> = to_bytes(&arr[2..])?.into();
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
                key.clone(),
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
    let key = arg_bytes(&arr, 1)?;
    let timeout = arg_double(&arr, 2)?;

    let receiver = {
        let mut lock = store.lock().unwrap();

        if let Some(val) = lock.entries.get_mut(key) {
            let vec = val.data.try_vec_mut()?;
            if let Some(v) = vec.pop_front() {
                return Ok(array(vec![key.clone(), v]));
            }
        }

        let (sender, receiver) = oneshot::channel();
        lock.blpop_waiters
            .entry(key.clone())
            .or_default()
            .push_back(sender);
        receiver
    };

    if timeout > 0.0 {
        let duration = Duration::from_secs_f64(timeout);
        Ok(match tokio::time::timeout(duration, receiver).await {
            Ok(Ok(value)) => array(vec![key.clone(), value]),
            _ => RESPValue::Array(None),
        })
    } else {
        Ok(receiver
            .await
            .map(|value| array(vec![key.clone(), value]))
            .unwrap_or(RESPValue::Array(None)))
    }
}

fn cmd_type(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg_bytes(&arr, 1)?;
    Ok(match store.lock().unwrap().entries.get(key) {
        Some(Value {
            data: value,
            expires_at: _,
        }) => RESPValue::SimpleString(value.type_name().to_string()),
        None => RESPValue::SimpleString("none".to_string()),
    })
}

fn cmd_xadd(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let key = arg_bytes(&arr, 1)?;
    let entry_id = arg_str(&arr, 2)?;
    arg(&arr, 3)?;

    let mut fields = vec![];
    for i in (3..arr.len()).step_by(2) {
        let field = arg_bytes(&arr, i)?.clone();
        let value = arg_bytes(&arr, i + 1)?.clone();
        fields.push((field, value));
    }

    let mut lock = store.lock().unwrap();
    let val = lock.entries.entry(key.clone()).or_insert(Value {
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
    let key = arg_bytes(&arr, 1)?;
    let start = {
        let s = arg_str(&arr, 2)?;
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
        let s = arg_str(&arr, 3)?;
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
    stream_keys: &Vec<Vec<u8>>,
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
    stream_keys: &Vec<Vec<u8>>,
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
        let Ok(s) = str::from_utf8(arg_bytes(&arr, i)?) else {
            break;
        };
        i += 1;
        match s.to_lowercase().as_str() {
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
        .map(|k| k.try_bytes().cloned())
        .collect::<Result<Vec<Vec<u8>>, CmdError>>()?;
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
    let key = arg_bytes(&arr, 1)?;
    let mut lock = store.lock().unwrap();

    let value = lock.entries.entry(key.clone()).or_insert(Value {
        data: Data::String(vec![b'0']),
        expires_at: None,
    });

    let new_num: i64 = value
        .data
        .try_str()?
        .parse::<i64>()
        .map_err(|_| CmdError::NotInt)?
        + 1;
    value.data = Data::String(new_num.to_string().into_bytes());

    Ok(RESPValue::Integer(new_num))
}

fn cmd_info(
    arr: &[RESPValue],
    store: &SharedStore,
    config: &SharedConfig,
) -> Result<RESPValue, CmdError> {
    let mut sections = HashMap::new();
    let role = if config.is_replica { "slave" } else { "master" };
    sections.insert(
        "replication".to_string(),
        format!(
            "# Replication\nrole:{}\nconnected_slaves:0\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\n",
            role
        ),
    );

    if let Ok(s) = arg_str(&arr, 1) {
        match sections.get(&s.to_lowercase()) {
            Some(section) => Ok(section.to_string().into()),
            None => Ok("".to_string().into()),
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

async fn cmd_wait(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let numreplicas = arg_uint(arr, 1)?;
    let timeout = arg_uint(arr, 2)?;

    let msg = encode(&array(vec![
        "REPLCONF".to_string(),
        "GETACK".to_string(),
        "*".to_string(),
    ]));

    let target = {
        let lock = store.lock().unwrap();
        let t = lock.master_offset;
        if t == 0 {
            return Ok(RESPValue::Integer(lock.replicas.len() as i64));
        }
        t
    };

    let mut up_to_date = 0;
    tokio::time::timeout(Duration::from_millis(timeout), async {
        loop {
            up_to_date = 0;

            {
                let lock = store.lock().unwrap();
                for replica in &lock.replicas {
                    if replica.offset.load(Ordering::Relaxed) >= target {
                        up_to_date += 1;
                    }
                }
            }

            if up_to_date >= numreplicas {
                return;
            }

            {
                let mut lock = store.lock().unwrap();
                lock.master_offset += msg.len() as u64;
                for replica in &lock.replicas {
                    replica.sender.send(msg.clone()).ok();
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .ok();

    Ok(RESPValue::Integer(up_to_date as i64))
}

fn cmd_config(
    arr: &[RESPValue],
    store: &SharedStore,
    config: &SharedConfig,
) -> Result<RESPValue, CmdError> {
    let subcommand = arg_str(arr, 1)?;
    if !subcommand.eq_ignore_ascii_case("GET") {
        return Err(CmdError::Syntax);
    }
    let key = arg_str(arr, 2)?;

    let value = match key.to_lowercase().as_str() {
        "dir" => &config.dir,
        "dbfilename" => &config.dbfilename,
        _ => return Ok(RESPValue::BulkString(None)),
    };

    Ok(array(vec![key.to_string(), value.clone()]))
}

fn cmd_keys(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    Ok(array(store.lock().unwrap().entries.keys().cloned()))
}

fn cmd_publish(arr: &[RESPValue], store: &SharedStore) -> Result<RESPValue, CmdError> {
    let channel_name = arg_bytes(arr, 1)?;
    let message = arg_bytes(arr, 2)?;

    let sub_count = match store
        .lock()
        .unwrap()
        .channel_subscriptions
        .get_mut(channel_name)
    {
        Some(subscribers) => {
            let mut sent = 0;
            let mut to_remove = vec![];

            for (id, sub) in subscribers.iter() {
                match sub.send(encode(&array(vec![
                    b"message".to_vec(),
                    channel_name.clone(),
                    message.clone(),
                ]))) {
                    Ok(_) => sent += 1,
                    Err(_) => to_remove.push(*id),
                };
            }

            for id in to_remove {
                subscribers.remove(&id);
            }

            sent
        }
        None => 0,
    };

    Ok(sub_count.into())
}

pub(crate) async fn execute_command(
    command: &str,
    arr: &[RESPValue],
    store: &SharedStore,
    config: &SharedConfig,
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
        "info" => cmd_info(&arr, &store, &config),
        "replconf" => Ok(RESPValue::SimpleString("OK".to_string())),
        "wait" => cmd_wait(&arr, &store).await,
        "config" => cmd_config(&arr, &store, &config),
        "keys" => cmd_keys(&arr, &store),
        "publish" => cmd_publish(&arr, &store),
        _ => Err(CmdError::Unknown),
    }
}
