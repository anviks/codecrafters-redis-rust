use std::{
    collections::HashMap,
    time::{Duration, UNIX_EPOCH},
};

use crate::store::{Data, Value};

fn read_length(b: &[u8]) -> (usize, usize) {
    let indicator = b[0] as usize;
    match indicator >> 6 & 0b11 {
        0b00 => (indicator, 1),
        0b01 => {
            let length = (indicator & 0b0011_1111) << 8 | b[1] as usize;
            (length, 2)
        }
        0b10 => {
            let (length, consumed) = match indicator & 0b0011_1111 {
                0b0000_0000 => (u32::from_be_bytes(b[1..5].try_into().unwrap()) as usize, 5),
                0b0000_0001 => (u64::from_be_bytes(b[1..9].try_into().unwrap()) as usize, 9),
                _ => unimplemented!(),
            };
            (length, consumed)
        }
        0b11 => panic!("Not a length"),
        _ => unreachable!(),
    }
}

fn read_value(b: &[u8]) -> (Vec<u8>, usize) {
    let indicator = b[0] as usize;
    match indicator >> 6 & 0b11 {
        0b00 | 0b01 | 0b10 => {
            let (length, consumed) = read_length(b);
            let val = b[consumed..consumed + length].to_vec();
            (val, consumed + length)
        }
        0b11 => match indicator & 0b11 {
            0b00 => {
                let val = i8::from_le_bytes([b[1]]);
                (val.to_string().as_bytes().to_vec(), 2)
            }
            0b01 => {
                let val = i16::from_le_bytes([b[1], b[2]]);
                (val.to_string().as_bytes().to_vec(), 3)
            }
            0b10 => {
                let val = i32::from_le_bytes(b[1..5].try_into().unwrap());
                (val.to_string().as_bytes().to_vec(), 5)
            }
            0b11 => unimplemented!(),
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

pub(crate) fn parse_rdb(rdb: &[u8]) -> HashMap<Vec<u8>, Value> {
    let mut metadata: HashMap<String, String> = HashMap::new();
    let mut entries: HashMap<Vec<u8>, Value> = HashMap::new();

    assert_eq!(&rdb[..7], b"REDIS00", "Not an RDB file");
    assert_eq!(
        rdb.iter().filter(|b| **b == 0xFE).count(),
        1,
        "Multiple databases are not supported"
    );
    // Skip REDIS00XX
    let mut i = 9;

    while i < rdb.len() && rdb[i] != 0xFE {
        assert_eq!(rdb[i], 0xFA); // AUX
        i += 1;
        let (key, j) = read_value(&rdb[i..]);
        i += j;
        let (val, j) = read_value(&rdb[i..]);
        i += j;

        metadata.insert(
            String::from_utf8(key).unwrap(),
            String::from_utf8(val).unwrap(),
        );
    }

    assert_eq!(rdb[i], 0xFE); // SELECTDB
    assert_eq!(rdb[i + 1], 0x00); // DB number/index
    assert_eq!(rdb[i + 2], 0xFB); // RESIZEDB
    i += 3;

    // I don't care about the hash table sizes
    i += read_length(&rdb[i..]).1;
    i += read_length(&rdb[i..]).1;

    let mut pending_expiry = None;
    while i < rdb.len() && rdb[i] != 0xFF {
        let val_type = rdb[i];
        i += 1;

        match val_type {
            0x00 => {
                let (key, j) = read_value(&rdb[i..]);
                i += j;
                let (val, j) = read_value(&rdb[i..]);
                i += j;
                entries.insert(
                    key,
                    Value {
                        data: Data::String(val),
                        expires_at: pending_expiry,
                    },
                );
                pending_expiry = None;
            }
            0xFD => {
                let exp_s = u32::from_le_bytes(rdb[i..i + 4].try_into().unwrap());
                i += 4;
                pending_expiry = Some(UNIX_EPOCH + Duration::from_secs(exp_s as u64));
            }
            0xFC => {
                let exp_ms = u64::from_le_bytes(rdb[i..i + 8].try_into().unwrap());
                i += 8;
                pending_expiry = Some(UNIX_EPOCH + Duration::from_millis(exp_ms));
            }
            other => unimplemented!("Unhandled type/opcode {:#x} at {i}", other),
        }
    }

    entries
}
