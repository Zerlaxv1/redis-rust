use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use crate::resp_parser::{RedisValueRef, RespParser};

mod resp_parser;

enum RedisValue {
    String(String, Option<Instant>),
    List(Vec<String>),
}

type Store = Arc<Mutex<HashMap<String, RedisValue>>>;

#[tokio::main]
async fn main() {
    // bind to :6379
    let listener: TcpListener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let data: HashMap<String, RedisValue> = HashMap::new();
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
                    b"RPUSH" => cmd_rpush(&elements, arc),
                    b"LRANGE" => cmd_lrange(&elements, arc),
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

        let value: RedisValue =
            RedisValue::String(String::from_utf8_lossy(value).to_string(), experity);

        store.insert(String::from_utf8_lossy(key).to_string(), value);
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
        let key_string = String::from_utf8_lossy(key).to_string();
        let r = store.get(&key_string);
        match r {
            Some(result) => {
                if let RedisValue::String(value, timeout) = result {
                    match timeout {
                        Some(timeout) => {
                            if timeout < &Instant::now() {
                                return resp_null_bulk();
                            } else {
                                return resp_bulk(&value);
                            }
                        }

                        None => return resp_bulk(&value),
                    }
                } else {
                    return resp_error(&format!("{} is not a String", key_string));
                }
            }
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

fn resp_int(i: usize) -> Vec<u8> {
    format!(":{}\r\n", i).into_bytes()
}

fn resp_array(array: &[String]) -> Vec<u8> {
    // exemple : ["a", "b", "c"]
    //
    //*3\r\n
    // $1\r\n
    // a\r\n
    // $1\r\n
    // b\r\n
    // $1\r\n
    // c\r\n
    let mut result = format!("*{}\r\n", array.len()).into_bytes();
    for s in array {
        result.extend(resp_bulk(s));
    }
    result
}

fn cmd_rpush(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
    if elements.len() < 3 {
        return resp_error("wrong arguments for RPUSH");
    }
    if let RedisValueRef::String(key) = &elements[1] {
        let mut store = arc.lock().unwrap();
        let key_string = String::from_utf8_lossy(key).to_string();

        let entry = store.entry(key_string).or_insert(RedisValue::List(vec![]));

        match entry {
            RedisValue::List(liste) => {
                for value in &elements[2..] {
                    if let RedisValueRef::String(v) = value {
                        liste.push(String::from_utf8_lossy(v).to_string());
                    }
                }
                return resp_int(liste.len());
            }
            _ => return resp_error("not a list"),
        }
    } else {
        return resp_error("list argument is not a string for RPUSH");
    };
}

fn cmd_lrange(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
    if elements.len() != 4 {
        return resp_error("wrong number of arguments for LRANGE");
    }

    if let RedisValueRef::String(liste) = &elements[1]
        && let RedisValueRef::String(start) = &elements[2]
        && let RedisValueRef::String(end) = &elements[3]
    {
        let store = arc.lock().unwrap();
        let key_string = String::from_utf8_lossy(liste).to_string();

        let entry = store.get(&key_string);

        match entry {
            Some(liste) => {
                if let RedisValue::List(liste) = liste {
                    let start_i: i32 = String::from_utf8_lossy(start).parse().unwrap();
                    let start = resolve_index(start_i, liste.len());

                    let end_i: i32 = String::from_utf8_lossy(end).parse().unwrap();
                    let end: usize = resolve_index(end_i, liste.len());

                    if start >= liste.len() || start > end {
                        return resp_array(&[]);
                    }

                    if end >= liste.len() {
                        return resp_array(&liste[start..liste.len()]);
                    }
                    return resp_array(&liste[start..=end]);
                } else {
                    return resp_error("not a list");
                }
            }
            None => return resp_array(&[]),
        }
    } else {
        return resp_error("arguments are not strings");
    }
}

fn resolve_index(i: i32, len: usize) -> usize {
    if i < 0 {
        (len as i32 + i).max(0) as usize
    } else {
        (i as usize).min(len)
    }
}
