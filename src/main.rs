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

fn parse_resp(input: &[u8], i: &mut usize) -> RESPValue {
    let type_byte = input[*i];
    *i += 1;

    return match type_byte {
        b'+' => {
            let mut str = vec![];
            while (input[*i]) != b'\r' {
                str.push(input[*i]);
                *i += 1;
            }
            *i += 2; // skip \r\n
            RESPValue::SimpleString(String::from_utf8(str).unwrap())
        }
        b'-' => {
            let mut str = vec![];
            while (input[*i]) != b'\r' {
                str.push(input[*i]);
                *i += 1;
            }
            *i += 2; // skip \r\n
            RESPValue::Error(String::from_utf8(str).unwrap())
        }
        b':' => {
            let mut int = vec![];
            while (input[*i]) != b'\r' {
                int.push(input[*i]);
                *i += 1;
            }
            *i += 2; // skip \r\n
            RESPValue::Integer(str::from_utf8(&int).unwrap().parse().unwrap())
        }
        b'$' => {
            let mut len_bytes = vec![];
            while input[*i] != b'\r' {
                len_bytes.push(input[*i]);
                *i += 1;
            }
            *i += 2; // skip \r\n
            let count: i32 = str::from_utf8(&len_bytes).unwrap().parse().unwrap();
            if count == -1 {
                return RESPValue::Null;
            }
            let mut str = vec![];
            for _ in 0..count {
                str.push(input[*i]);
                *i += 1;
            }
            *i += 2; // skip \r\n
            RESPValue::BulkString(String::from_utf8(str).unwrap())
        }
        b'*' => {
            let mut len_bytes = vec![];
            while input[*i] != b'\r' {
                len_bytes.push(input[*i]);
                *i += 1;
            }
            *i += 2; // skip \r\n
            let count: i32 = str::from_utf8(&len_bytes).unwrap().parse().unwrap();
            if count == -1 {
                return RESPValue::Null;
            }
            let mut items = vec![];
            for _ in 0..count {
                items.push(parse_resp(input, i));
            }
            *i += 2; // skip \r\n
            RESPValue::Array(items)
        }
        _ => panic!("unexpected byte: {}", type_byte),
    }

    // while i < input.len() {
    //     if byte == b':' {}
    //     if byte == b'*' {
    //         result.push(RESPValue::Array(vec![]));
    //     }

    //     i += 1;
    // }

    // println!("{:?}", String::from_utf8_lossy(&input));
    // println!("{:?}", &input);

    // return vec![];
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
                                    if let RESPValue::BulkString(cmd) = &arr[0] && let RESPValue::BulkString(value) = &arr[1] {
                                        if cmd.to_lowercase() == "echo" {
                                            let mut output = "+".to_owned();
                                            output.push_str(value);
                                            output.push_str("\r\n");

                                            if let Err(_) = stream.write_all(output.as_bytes()).await {
                                                break;
                                            }
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
