use std::collections::VecDeque;

use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum CmdError {
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    #[error("value is not an integer or out of range")]
    NotInt,
    #[error("value is not an integer or out of range")]
    NotUint,
    #[error("value is not a double or out of range")]
    NotDouble,
    #[error("ERR wrong number of arguments for command")]
    WrongArgs,
    #[error("ERR unknown command")]
    Unknown,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RESPValue {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Option<Vec<RESPValue>>),
    // Null,
    // Boolean(bool),
    // Double(f64),
}

impl std::hash::Hash for RESPValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
    }
}

impl Eq for RESPValue {}

impl From<String> for RESPValue {
    fn from(value: String) -> Self {
        RESPValue::BulkString(Some(value))
    }
}

impl From<i64> for RESPValue {
    fn from(value: i64) -> Self {
        RESPValue::Integer(value)
    }
}

impl From<Vec<RESPValue>> for RESPValue {
    fn from(value: Vec<RESPValue>) -> Self {
        RESPValue::Array(Some(value))
    }
}

impl From<VecDeque<RESPValue>> for RESPValue {
    fn from(value: VecDeque<RESPValue>) -> Self {
        value.into()
    }
}

impl RESPValue {
    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            RESPValue::BulkString(Some(s)) => Some(s),
            _ => None,
        }
    }

    pub(crate) fn as_vec(&self) -> Option<&Vec<RESPValue>> {
        match self {
            RESPValue::Array(Some(vec)) => Some(vec),
            _ => None,
        }
    }

    pub(crate) fn as_vec_mut(&mut self) -> Option<&mut Vec<RESPValue>> {
        match self {
            RESPValue::Array(Some(vec)) => Some(vec),
            _ => None,
        }
    }

    pub(crate) fn try_str(&self) -> Result<&str, CmdError> {
        self.as_str().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_vec(&self) -> Result<&Vec<RESPValue>, CmdError> {
        self.as_vec().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_vec_mut(&mut self) -> Result<&mut Vec<RESPValue>, CmdError> {
        self.as_vec_mut().ok_or(CmdError::WrongType)
    }
}

fn read_until_crlf<'a>(input: &'a [u8], i: &mut usize) -> &'a [u8] {
    let start = *i;
    while input[*i] != b'\r' {
        *i += 1;
    }
    let slice = &input[start..*i];
    *i += 2; // skip \r\n
    slice
}

fn decode_value(input: &[u8], i: &mut usize) -> RESPValue {
    let type_byte = input[*i];
    *i += 1;

    return match type_byte {
        b'+' => RESPValue::SimpleString(
            str::from_utf8(read_until_crlf(input, i))
                .unwrap()
                .to_string(),
        ),
        b'-' => RESPValue::SimpleError(
            str::from_utf8(read_until_crlf(input, i))
                .unwrap()
                .to_string(),
        ),
        b':' => RESPValue::Integer(
            str::from_utf8(read_until_crlf(input, i))
                .unwrap()
                .parse()
                .unwrap(),
        ),
        b'$' => {
            let count: i32 = str::from_utf8(read_until_crlf(input, i))
                .unwrap()
                .parse()
                .unwrap();
            if count == -1 {
                return RESPValue::BulkString(None);
            }
            let slice = &input[*i..*i + count as usize];
            *i += count as usize + 2;
            str::from_utf8(slice).unwrap().to_string().into()
        }
        b'*' => {
            let count: i32 = str::from_utf8(read_until_crlf(input, i))
                .unwrap()
                .parse()
                .unwrap();
            if count == -1 {
                return RESPValue::Array(None);
            }
            RESPValue::Array(Some((0..count).map(|_| decode_value(input, i)).collect()))
        }
        // b'_' => {
        //     read_until_crlf(input, i);
        //     RESPValue::Null
        // }
        // b'#' => match read_until_crlf(input, i) {
        //     [b't'] => RESPValue::Boolean(true),
        //     [b'f'] => RESPValue::Boolean(false),
        //     b => panic!("Invalid boolean byte sequence: {:?}", b),
        // },
        // b',' => RESPValue::Double(
        //     str::from_utf8(read_until_crlf(input, i))
        //         .unwrap()
        //         .parse()
        //         .unwrap(),
        // ),
        _ => panic!("Unexpected byte: {}", type_byte),
    };
}

pub fn encode(input: &RESPValue) -> Vec<u8> {
    match input {
        RESPValue::SimpleString(str) => format!("+{str}\r\n").bytes().collect(),
        RESPValue::SimpleError(err) => format!("-{err}\r\n").bytes().collect(),
        RESPValue::Integer(int) => format!(":{int}\r\n").bytes().collect(),
        RESPValue::BulkString(str) => match str {
            Some(s) => format!("${}\r\n{}\r\n", s.len(), s).bytes().collect(),
            None => b"$-1\r\n".to_vec(),
        },
        RESPValue::Array(respvalues) => match respvalues {
            Some(vals) => {
                let mut result = format!("*{}\r\n", vals.len()).as_bytes().to_vec();
                for val in vals {
                    result.extend(encode(val));
                }
                result
            }
            None => b"*-1\r\n".to_vec(),
        }, // RESPValue::Null => "_\r\n".bytes().collect(),
           // RESPValue::Boolean(bool) => format!("#{}\r\n", bool.to_string().chars().nth(0).unwrap())
           //     .bytes()
           //     .collect(),
           // RESPValue::Double(_) => todo!(),
    }
}

pub fn decode(input: &[u8]) -> RESPValue {
    let mut i = 0;
    decode_value(input, &mut i)
}
