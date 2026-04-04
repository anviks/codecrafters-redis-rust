use std::char::from_digit;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Debug)]
enum RESPValue {
    SimpleString(String),
    Error(String),
    Integer(i32),
    BulkString(String),
    Array(Vec<RESPValue>),
    Null,
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

fn parse_resp(input: &[u8], i: &mut usize) -> RESPValue {
    let type_byte = input[*i];
    *i += 1;

    return match type_byte {
        b'+' => RESPValue::SimpleString(str::from_utf8(read_until_crlf(input, i)).unwrap().to_string()),
        b'-' => RESPValue::Error(str::from_utf8(read_until_crlf(input, i)).unwrap().to_string()),
        b':' => RESPValue::Integer(str::from_utf8(read_until_crlf(input, i)).unwrap().parse().unwrap()),
        b'$' => {
            let count: i32 = str::from_utf8(read_until_crlf(input, i)).unwrap().parse().unwrap();
            if count == -1 { return RESPValue::Null; }
            let slice = &input[*i..*i + count as usize];
            *i += count as usize + 2;
            RESPValue::BulkString(str::from_utf8(slice).unwrap().to_string())
        }
        b'*' => {
            let count: i32 = str::from_utf8(read_until_crlf(input, i)).unwrap().parse().unwrap();
            if count == -1 { return RESPValue::Null; }
            RESPValue::Array((0..count).map(|_| parse_resp(input, i)).collect())
        }
        _ => panic!("unexpected byte: {}", type_byte),
    };
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                tokio::spawn(async move {
                    loop {
                        let mut buf = [0; 512];
                        match stream.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(_) => {
                                let mut i: usize = 0;
                                let parsed = parse_resp(&buf, &mut i);
                                println!("{:?}", parsed);
                                if let RESPValue::Array(arr) = parsed {
                                    if let RESPValue::BulkString(cmd) = &arr[0]
                                        && arr.len() > 1
                                        && let RESPValue::BulkString(value) = &arr[1]
                                    {
                                        if cmd.to_lowercase() == "echo" {
                                            let mut output = "$".to_owned();
                                            output.push_str(&value.len().to_string());
                                            output.push_str("\r\n");
                                            output.push_str(value);
                                            output.push_str("\r\n");

                                            if let Err(_) =
                                                stream.write_all(output.as_bytes()).await
                                            {
                                                break;
                                            }

                                            continue;
                                        }
                                    }
                                }
                                if let Err(_) = stream.write_all(b"+PONG\r\n").await {
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
