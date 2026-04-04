use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => loop {
                let mut buf = [0; 512];
                match stream.read(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => stream.write_all(b"+PONG\r\n").unwrap(),
                    Err(_) => break,
                }
            },
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
