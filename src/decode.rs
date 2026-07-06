#[derive(Debug)]
pub(crate) enum RESPValue {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RESPValue>),
    Null,
    Boolean(bool),
    Double(f64),
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
                return RESPValue::Null;
            }
            let slice = &input[*i..*i + count as usize];
            *i += count as usize + 2;
            RESPValue::BulkString(str::from_utf8(slice).unwrap().to_string())
        }
        b'*' => {
            let count: i32 = str::from_utf8(read_until_crlf(input, i))
                .unwrap()
                .parse()
                .unwrap();
            if count == -1 {
                return RESPValue::Null;
            }
            RESPValue::Array((0..count).map(|_| decode_value(input, i)).collect())
        }
        b'_' => {
            read_until_crlf(input, i);
            RESPValue::Null
        }
        b'#' => match read_until_crlf(input, i) {
            [b't'] => RESPValue::Boolean(true),
            [b'f'] => RESPValue::Boolean(false),
            b => panic!("Invalid boolean byte sequence: {:?}", b),
        },
        b',' => RESPValue::Double(
            str::from_utf8(read_until_crlf(input, i))
                .unwrap()
                .parse()
                .unwrap(),
        ),
        _ => panic!("Unexpected byte: {}", type_byte),
    };
}

pub fn decode(input: &[u8]) -> RESPValue {
    let mut i = 0;
    decode_value(input, &mut i)
}
