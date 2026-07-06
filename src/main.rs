use crate::decode::{RESPValue, decode};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
mod decode;

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
                                let parsed = decode(&buf);
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
