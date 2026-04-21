#![allow(unused_imports)]
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{io::ErrorKind, vec};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_stream::{Stream, StreamExt};
use tokio_util::codec::FramedRead;

use crate::resp_parser::{RedisValueRef, RespParser};

mod resp_parser;

#[tokio::main]
async fn main() {
    // bind to :6379
    let listener: TcpListener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let data: HashMap<String, (String, Option<Instant>)> = HashMap::new();
    let arc: Arc<Mutex<HashMap<String, (String, Option<Instant>)>>> = Arc::new(data.into());

    // main loop
    loop {
        // accepte new incoming connections
        let (stream, _): (TcpStream, _) = listener.accept().await.unwrap();

        let arc_clone = arc.clone();

        tokio::spawn(async move {
            handle_connection(stream, arc_clone).await;
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    arc: Arc<Mutex<HashMap<String, (String, Option<Instant>)>>>,
) {
    // diviser le stream en 2 partie, read et write pour le borrow checkekr
    let (read_stream, mut write_stream) = stream.split();

    // lire le stream avec tokio, il le lit, puis le passe auto danss le decoder RESP. Il en sort un RedisValueRef
    let mut frame = FramedRead::new(read_stream, RespParser);

    // loop sur plusieurs commands
    loop {
        // si il existe un prochain mot
        match frame.next().await {
            Some(Ok(value)) => {
                let response: &[u8] = &handle_command(value, &arc);

                // envoyer la réponse
                let result: Result<(), std::io::Error> = write_stream.write_all(response).await;

                match result {
                    Ok(_) => {}
                    Err(_) => {
                        eprintln!("Erreur lors de l'envoie de la réponse");
                        return;
                    }
                }
            }
            Some(Err(_)) => {
                eprintln!("Erreur lors de la lecture de l'input");
                return;
            }
            None => {
                return;
            }
        }
    }
}

fn handle_command(
    value: RedisValueRef,
    arc: &Arc<Mutex<HashMap<String, (String, Option<Instant>)>>>,
) -> Vec<u8> {
    match value {
        RedisValueRef::String(bytes) => match &bytes.to_ascii_uppercase()[..] {
            b"PING" => b"+PONG\r\n".to_vec(),
            _ => todo!(),
        },
        RedisValueRef::Error(_bytes) => {
            return b"+ERROR\r\n".to_vec();
        }
        RedisValueRef::Int(_) => {
            return b"+INT\r\n".to_vec();
        }
        RedisValueRef::Array(elements) => {
            if elements.is_empty() {
                return b"-ERR empty array\r\n".to_vec();
            }

            let command = &elements[0];
            match command {
                RedisValueRef::String(bytes) => match &bytes.to_ascii_uppercase()[..] {
                    b"PING" => b"+PONG\r\n".to_vec(),
                    b"ECHO" => {
                        if elements.len() < 2 {
                            return b"-ERR wrong number of arguments for ECHO cmd \r\n".to_vec();
                        }
                        if let RedisValueRef::String(arg) = &elements[1] {
                            format!("${}\r\n{}\r\n", arg.len(), String::from_utf8_lossy(arg))
                                .into_bytes()
                        } else {
                            b"-ERR ECHO argument must be a string\r\n".to_vec()
                        }
                    }
                    b"SET" => {
                        let mut experity: Option<Instant> = None;

                        if elements.len() == 5 {
                            if let RedisValueRef::String(option) = &elements[3] {
                                match &option.to_ascii_uppercase()[..] {
                                    b"PX" => {
                                        if let RedisValueRef::String(time) = &elements[4] {
                                            let ms: u64 =
                                                String::from_utf8_lossy(time).parse().unwrap();
                                            experity =
                                                Some(Instant::now() + Duration::from_millis(ms));
                                        } else {
                                            return b"-ERR wrong argument for PX".to_vec();
                                        }
                                    }
                                    b"EX" => {
                                        if let RedisValueRef::String(time) = &elements[4] {
                                            let ms: u64 =
                                                String::from_utf8_lossy(time).parse().unwrap();
                                            experity =
                                                Some(Instant::now() + Duration::from_secs(ms));
                                        } else {
                                            return b"-ERR wrong argument for EX".to_vec();
                                        }
                                    }
                                    _ => {
                                        return b"-ERR wrong timeout argument for SET".to_vec();
                                    }
                                }
                            }
                        }

                        if elements.len() < 3 {
                            return b"-ERR wrong number of arguments for SET cmd \r\n".to_vec();
                        }
                        if let RedisValueRef::String(key) = &elements[1]
                            && let RedisValueRef::String(value) = &elements[2]
                        {
                            let mut store = arc.lock().unwrap();
                            store.insert(
                                String::from_utf8_lossy(key).to_string(),
                                (String::from_utf8_lossy(value).to_string(), experity),
                            );
                            return b"+OK\r\n".to_vec();
                        } else {
                            b"-ERR SET argument must be a string\r\n".to_vec()
                        }
                    }
                    b"GET" => {
                        if elements.len() < 2 {
                            return b"-ERR wrong number of arguments for GET cmd \r\n".to_vec();
                        }
                        if let RedisValueRef::String(key) = &elements[1] {
                            let store = arc.lock().unwrap();
                            let r = store.get(&String::from_utf8_lossy(key).to_string());
                            match r {
                                Some(result) => match result.1 {
                                    Some(r) => {
                                        if r < Instant::now() {
                                            return b"$-1\r\n".to_vec();
                                        } else {
                                            format!("${}\r\n{}\r\n", result.0.len(), result.0)
                                                .into_bytes()
                                        }
                                    }
                                    None => format!("${}\r\n{}\r\n", result.0.len(), result.0)
                                        .into_bytes(),
                                },
                                None => {
                                    return b"$-1\r\n".to_vec();
                                }
                            }
                        } else {
                            b"-ERR GET argument must be a string\r\n".to_vec()
                        }
                    }
                    _ => todo!(),
                },
                _ => b"-ERR command must be a STRING\r\n".to_vec(),
            }
        }
        RedisValueRef::NullArray => {
            return b"+NULLARRAY\r\n".to_vec();
        }
        RedisValueRef::NullBulkString => {
            return b"+NullBulkString\r\n".to_vec();
        }
    }
}
