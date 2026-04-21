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

type Store = Arc<Mutex<HashMap<String, (String, Option<Instant>)>>>;

#[tokio::main]
async fn main() {
    // bind to :6379
    let listener: TcpListener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let data: HashMap<String, (String, Option<Instant>)> = HashMap::new();
    let arc: Store = Arc::new(data.into());

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

async fn handle_connection(mut stream: TcpStream, arc: Store) {
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

fn handle_command(value: RedisValueRef, arc: &Store) -> Vec<u8> {
    match value {
        RedisValueRef::String(bytes) => match &bytes.to_ascii_uppercase()[..] {
            b"PING" => cmd_ping(),
            _ => resp_error("not implemented"),
        },
        RedisValueRef::Error(_bytes) => {
            return resp_error("ERROR not implemented");
        }
        RedisValueRef::Int(_) => {
            return resp_error("INT not implemented");
        }
        RedisValueRef::Array(elements) => {
            if elements.is_empty() {
                return resp_error("empty array");
            }

            let command = &elements[0];
            match command {
                RedisValueRef::String(bytes) => match &bytes.to_ascii_uppercase()[..] {
                    b"PING" => cmd_ping(),
                    b"ECHO" => cmd_echo(&elements),
                    b"SET" => cmd_set(&elements, arc),
                    b"GET" => cmd_get(&elements, arc),
                    _ => resp_error("command not supported"),
                },
                _ => resp_error("command must be a STRING"),
            }
        }
        RedisValueRef::NullArray => {
            return resp_error("NULLARRAY not implemented");
        }
        RedisValueRef::NullBulkString => {
            return resp_error("NullBulkString not implemented");
        }
    }
}

fn cmd_ping() -> Vec<u8> {
    return resp_simple("PONG");
}

fn cmd_echo(elements: &[RedisValueRef]) -> Vec<u8> {
    if elements.len() < 2 {
        return resp_error("wrong number of arguments for ECHO cmd");
    }
    if let RedisValueRef::String(arg) = &elements[1] {
        return resp_bulk(&String::from_utf8_lossy(arg).to_string());
    } else {
        return resp_error("ECHO argument must be a string");
    }
}

fn cmd_set(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
    let mut experity: Option<Instant> = None;

    if elements.len() == 5 {
        if let RedisValueRef::String(option) = &elements[3] {
            match &option.to_ascii_uppercase()[..] {
                b"PX" => {
                    if let RedisValueRef::String(time) = &elements[4] {
                        let ms: u64 = String::from_utf8_lossy(time).parse().unwrap();
                        experity = Some(Instant::now() + Duration::from_millis(ms));
                    } else {
                        return resp_error("wrong argument for PX");
                    }
                }
                b"EX" => {
                    if let RedisValueRef::String(time) = &elements[4] {
                        let ms: u64 = String::from_utf8_lossy(time).parse().unwrap();
                        experity = Some(Instant::now() + Duration::from_secs(ms));
                    } else {
                        return resp_error("wrong argument for EX");
                    }
                }
                _ => {
                    return resp_error("wrong timeout argument for SET");
                }
            }
        }
    }

    if elements.len() < 3 {
        return resp_error("wrong number of arguments for SET cmd");
    }
    if let RedisValueRef::String(key) = &elements[1]
        && let RedisValueRef::String(value) = &elements[2]
    {
        let mut store = arc.lock().unwrap();
        store.insert(
            String::from_utf8_lossy(key).to_string(),
            (String::from_utf8_lossy(value).to_string(), experity),
        );
        return resp_simple("OK");
    } else {
        return resp_error("SET argument must be a string");
    }
}

fn cmd_get(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
    if elements.len() < 2 {
        return resp_error("wrong number of arguments for GET cmd");
    }
    if let RedisValueRef::String(key) = &elements[1] {
        let store = arc.lock().unwrap();
        let r = store.get(&String::from_utf8_lossy(key).to_string());
        match r {
            Some(result) => match result.1 {
                Some(r) => {
                    if r < Instant::now() {
                        return resp_null_bulk();
                    } else {
                        return resp_bulk(&result.0);
                    }
                }
                None => return resp_bulk(&result.0),
            },
            None => {
                return resp_null_bulk();
            }
        }
    } else {
        return resp_error("GET argument must be a string");
    }
}

fn resp_error(msg: &str) -> Vec<u8> {
    format!("-ERR {}\r\n", msg).into_bytes()
}

fn resp_simple(msg: &str) -> Vec<u8> {
    format!("+{}\r\n", msg).into_bytes()
}

fn resp_bulk(s: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
}

fn resp_null_bulk() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}
