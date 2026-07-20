use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum CmdError {
    #[error(
        "ERR Can't execute '{0}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
    )]
    NotSubModeCmd(String),

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("ERR value is not an integer or out of range")]
    NotInt,

    #[error("ERR value is not an integer or out of range")]
    NotUint,

    #[error("ERR value is not a double or out of range")]
    NotDouble,

    #[error("Invalid stream ID specified as stream command argument")]
    InvalidStreamId,

    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    BadStreamId,

    #[error("ERR The ID specified in XADD must be greater than 0-0")]
    ZeroStreamId,

    #[error("ERR EXEC without MULTI")]
    ExecWithoutMulti,

    #[error("ERR DISCARD without MULTI")]
    DiscardWithoutMulti,

    #[error("ERR MULTI calls can not be nested")]
    NestedMulti,

    #[error("ERR wrong number of arguments for command")]
    WrongArgs,

    #[error("ERR syntax error")]
    Syntax,

    #[error("ERR unknown command")]
    Unknown,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RESPValue {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RESPValue>>),
}

impl std::fmt::Display for RESPValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            RESPValue::SimpleString(s) => write!(f, "{}", s),
            RESPValue::SimpleError(e) => write!(f, "{}", e),
            RESPValue::Integer(i) => write!(f, "{}", i),
            RESPValue::BulkString(items) => match items {
                Some(vec) => write!(f, "{}", String::from_utf8_lossy(vec)),
                None => write!(f, "None"),
            },
            RESPValue::Array(respvalues) => match respvalues {
                Some(vec) => {
                    let items: Vec<String> = vec.iter().map(|r| r.to_string()).collect();
                    write!(f, "[{}]", items.join(", "))
                }
                None => write!(f, "None"),
            },
        }
    }
}

impl std::hash::Hash for RESPValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
    }
}

impl Eq for RESPValue {}

impl From<String> for RESPValue {
    fn from(value: String) -> Self {
        RESPValue::BulkString(Some(value.into_bytes()))
    }
}

impl From<&str> for RESPValue {
    fn from(value: &str) -> Self {
        RESPValue::BulkString(Some(value.as_bytes().to_vec()))
    }
}

impl From<&[u8]> for RESPValue {
    fn from(value: &[u8]) -> Self {
        RESPValue::BulkString(Some(value.to_vec()))
    }
}

impl From<Vec<u8>> for RESPValue {
    fn from(value: Vec<u8>) -> Self {
        RESPValue::BulkString(Some(value))
    }
}

impl From<i64> for RESPValue {
    fn from(value: i64) -> Self {
        RESPValue::Integer(value)
    }
}

pub(crate) fn array<I>(items: I) -> RESPValue
where
    I: IntoIterator,
    I::Item: Into<RESPValue>,
{
    RESPValue::Array(Some(items.into_iter().map(Into::into).collect()))
}

pub(crate) fn array_of(items: Vec<RESPValue>) -> RESPValue {
    RESPValue::Array(Some(items))
}

pub(crate) fn resp_result(result: Result<RESPValue, CmdError>) -> RESPValue {
    match result {
        Ok(v) => v,
        Err(e) => RESPValue::SimpleError(e.to_string()),
    }
}

impl RESPValue {
    pub(crate) fn as_vec(&self) -> Option<&Vec<RESPValue>> {
        match self {
            RESPValue::Array(Some(v)) => Some(v),
            _ => None,
        }
    }

    pub(crate) fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            RESPValue::BulkString(Some(s)) => Some(s),
            _ => None,
        }
    }

    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            RESPValue::BulkString(Some(s)) => str::from_utf8(s).ok(),
            _ => None,
        }
    }

    pub(crate) fn try_vec(&self) -> Result<&Vec<RESPValue>, CmdError> {
        self.as_vec().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_bytes(&self) -> Result<&Vec<u8>, CmdError> {
        self.as_bytes().ok_or(CmdError::WrongType)
    }

    pub(crate) fn try_str(&self) -> Result<&str, CmdError> {
        self.as_str().ok_or(CmdError::WrongType)
    }
}

fn read_until_crlf<'a>(input: &'a [u8], i: &mut usize) -> Option<&'a [u8]> {
    let start = *i;
    while *i < input.len() && input[*i] != b'\r' {
        *i += 1;
    }
    // \r and \n need to be present
    if *i + 1 >= input.len() {
        return None;
    }
    let slice = &input[start..*i];
    *i += 2; // skip \r\n
    Some(slice)
}

fn decode_value(input: &[u8], i: &mut usize) -> Result<Option<RESPValue>, CmdError> {
    if input.len() <= *i {
        return Ok(None);
    }
    let type_byte = input[*i];
    *i += 1;

    let s = match read_until_crlf(input, i) {
        Some(slice) => str::from_utf8(slice).map_err(|_| CmdError::Syntax)?,
        None => return Ok(None),
    };

    return match type_byte {
        b'+' => Ok(Some(RESPValue::SimpleString(s.to_string()))),
        b'-' => Ok(Some(RESPValue::SimpleError(s.to_string()))),
        b':' => Ok(Some(RESPValue::Integer(
            s.parse().map_err(|_| CmdError::Syntax)?,
        ))),
        b'$' => {
            let count: i32 = s.parse().map_err(|_| CmdError::Syntax)?;
            if count == -1 {
                return Ok(Some(RESPValue::BulkString(None)));
            }
            if count < 0 {
                return Err(CmdError::Syntax);
            }
            let count = count as usize;
            if input.len() < *i + count + 2 {
                return Ok(None);
            }
            let slice = &input[*i..*i + count];
            *i += count + 2;
            Ok(Some(slice.into()))
        }
        b'*' => {
            let count: i32 = s.parse().map_err(|_| CmdError::Syntax)?;
            if count == -1 {
                return Ok(Some(RESPValue::Array(None)));
            }
            let mut elems = vec![];
            for _ in 0..count {
                match decode_value(input, i)? {
                    Some(r) => elems.push(r),
                    None => return Ok(None),
                }
            }
            Ok(Some(RESPValue::Array(Some(elems))))
        }
        _ => Err(CmdError::Syntax),
    };
}

pub fn encode(input: &RESPValue) -> Vec<u8> {
    match input {
        RESPValue::SimpleString(str) => format!("+{str}\r\n").bytes().collect(),
        RESPValue::SimpleError(err) => format!("-{err}\r\n").bytes().collect(),
        RESPValue::Integer(int) => format!(":{int}\r\n").bytes().collect(),
        RESPValue::BulkString(str) => match str {
            Some(s) => {
                let mut res: Vec<u8> = format!("${}\r\n", s.len()).bytes().collect();
                res.extend(s);
                res.extend("\r\n".bytes());
                res
            }
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
        },
    }
}

pub fn try_decode(input: &[u8]) -> Result<Option<(RESPValue, usize)>, CmdError> {
    let mut i = 0;
    let result = decode_value(input, &mut i);
    result.map(|res| res.map(|r| (r, i)))
}
