use std::char::from_digit;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

enum RESPValue {
    String(String),
    Number(i32),
    Array(Vec<RESPValue>),
}

fn parse_resp(input: &[u8]) -> Vec<RESPValue> {
    let result: Vec<RESPValue> = vec![];
    println!("{:?}", String::from_utf8_lossy(&input));

    return vec![];
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
                                parse_resp(&buf);
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
