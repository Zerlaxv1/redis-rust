use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, oneshot};
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
    Stream(Vec<(String, Vec<(String, String)>)>),
}

struct Server {
    data: HashMap<String, RedisValue>,
    waiters: HashMap<String, VecDeque<oneshot::Sender<String>>>,
}

type Store = Arc<Mutex<Server>>;

#[tokio::main]
async fn main() {
    let listener: TcpListener = find_listener(6379).await;

    let data: HashMap<String, RedisValue> = HashMap::new();
    let waiters: HashMap<String, VecDeque<oneshot::Sender<String>>> = HashMap::new();
    let server: Server = Server { data, waiters };
    let arc: Store = Arc::new(Mutex::new(server));

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

async fn find_listener(start_port: u16) -> TcpListener {
    let args: Vec<String> = std::env::args().collect();
    let mut address: String;

    if args.len() == 2 {
        address = args[1].clone();
        TcpListener::bind(address).await.unwrap()
    } else {
        for port in start_port..20000 {
            address = format!("127.0.0.1:{}", port);
            let listener = TcpListener::bind(&address).await;
            match listener {
                Ok(listener) => {
                    println!("Server Listening on {}", address);
                    return listener;
                }
                Err(_) => {
                    continue;
                }
            };
        }
        panic!("impossible de trouver un port disponible !")
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
                let response: &[u8] = &handle_command(value, &arc).await;

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

async fn handle_command(value: RedisValueRef, arc: &Store) -> Vec<u8> {
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
                    b"SET" => cmd_set(&elements, arc).await,
                    b"GET" => cmd_get(&elements, arc).await,
                    b"RPUSH" => cmd_rpush(&elements, arc).await,
                    b"LPUSH" => cmd_lpush(&elements, arc).await,
                    b"LRANGE" => cmd_lrange(&elements, arc).await,
                    b"LLEN" => cmd_llen(&elements, arc).await,
                    b"LPOP" => cmd_lpop(&elements, arc).await,
                    b"BLPOP" => cmd_blpop(&elements, arc).await,
                    b"TYPE" => cmd_type(&elements, arc).await,
                    b"XADD" => cmd_xadd(&elements, arc).await,
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

async fn cmd_set(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
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
        let mut store = arc.lock().await;

        let value: RedisValue =
            RedisValue::String(String::from_utf8_lossy(value).to_string(), experity);

        store
            .data
            .insert(String::from_utf8_lossy(key).to_string(), value);
        return resp_simple("OK");
    } else {
        return resp_error("SET argument must be a string");
    }
}

async fn cmd_get(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
    if elements.len() < 2 {
        return resp_error("wrong number of arguments for GET cmd");
    }
    if let RedisValueRef::String(key) = &elements[1] {
        let store = arc.lock().await;
        let key_string = String::from_utf8_lossy(key).to_string();
        let r = store.data.get(&key_string);
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

fn resp_null_array() -> Vec<u8> {
    b"*-1\r\n".to_vec()
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

async fn cmd_rpush(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
    push(elements, arc, false).await
}

async fn cmd_lpush(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
    push(elements, arc, true).await
}

async fn push(elements: &[RedisValueRef], arc: &Store, front: bool) -> Vec<u8> {
    if elements.len() < 3 {
        return resp_error("wrong arguments for RPUSH");
    }
    if let RedisValueRef::String(key) = &elements[1] {
        let mut store = arc.lock().await;
        let key_string = String::from_utf8_lossy(key).to_string();

        let len = {
            let entry = store
                .data
                .entry(key_string.clone())
                .or_insert(RedisValue::List(vec![]));

            match entry {
                RedisValue::List(liste) => {
                    for value in &elements[2..] {
                        if let RedisValueRef::String(v) = value {
                            let element: String = String::from_utf8_lossy(v).to_string();
                            if front {
                                liste.insert(0, element);
                            } else {
                                liste.push(element);
                            }
                        }
                    }
                    liste.len()
                }
                _ => return resp_error("not a list"),
            }
        };

        let entry = store.waiters.get_mut(&key_string);
        match entry {
            Some(entry) => {
                let tx = entry.pop_front();
                match tx {
                    Some(tx) => {
                        let liste = store.data.get_mut(&key_string);
                        match liste {
                            Some(liste) => {
                                if let RedisValue::List(liste) = liste {
                                    // remove l'index 0 (fais le job de blpop) puis on le notifie du succes
                                    let val: String = liste.remove(0);
                                    let _ = tx.send(val);
                                } else {
                                    // not a list
                                }
                            }
                            None => {
                                // Liste non existante
                            }
                        };
                    }
                    None => {
                        // Il n'y a pas de waiters
                    }
                }
            }
            None => {
                // Il n'y a pas de liste, pas normal je pense
            }
        }

        return resp_int(len);
    } else {
        return resp_error("list argument is not a string for RPUSH");
    };
}

async fn cmd_lrange(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
    if elements.len() != 4 {
        return resp_error("wrong number of arguments for LRANGE");
    }

    if let RedisValueRef::String(liste) = &elements[1]
        && let RedisValueRef::String(start) = &elements[2]
        && let RedisValueRef::String(end) = &elements[3]
    {
        let store = arc.lock().await;
        let key_string = String::from_utf8_lossy(liste).to_string();

        let entry = store.data.get(&key_string);

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

async fn cmd_llen(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
    if elements.len() != 2 {
        return resp_error("wrong number of arguments for LLEN");
    }
    if let RedisValueRef::String(liste) = &elements[1] {
        let store = arc.lock().await;
        let liste = store.data.get(&String::from_utf8_lossy(liste).to_string());

        match liste {
            Some(liste) => {
                if let RedisValue::List(liste) = liste {
                    return resp_int(liste.len());
                } else {
                    return resp_error("not a Liste");
                }
            }
            None => {
                return resp_int(0);
            }
        }
    } else {
        return resp_error("not a String");
    }
}

async fn cmd_lpop(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
    if elements.len() < 2 || elements.len() > 3 {
        return resp_error("wrong number of arguments for LPOP");
    }
    if let RedisValueRef::String(liste) = &elements[1] {
        let mut store = arc.lock().await;
        let liste = store
            .data
            .get_mut(&String::from_utf8_lossy(liste).to_string());

        if elements.len() == 3 {
            if let RedisValueRef::String(number) = &elements[2] {
                match liste {
                    Some(liste) => {
                        if let RedisValue::List(liste) = liste {
                            let number: usize = String::from_utf8_lossy(number).parse().unwrap();
                            let removed: Vec<String> =
                                liste.drain(0..number.min(liste.len())).collect();
                            return resp_array(&removed);
                        } else {
                            return resp_null_bulk();
                        }
                    }
                    None => {
                        return resp_null_bulk();
                    }
                }
            } else {
                return resp_error("arg 2 is not a string");
            }
        } else {
            match liste {
                Some(liste) => {
                    if let RedisValue::List(liste) = liste {
                        let removed = liste.remove(0);
                        return resp_bulk(&removed);
                    } else {
                        return resp_null_bulk();
                    }
                }
                None => {
                    return resp_null_bulk();
                }
            }
        }
    } else {
        return resp_error("arg 1 is not a string");
    }
}

async fn cmd_blpop(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
    if elements.len() != 3 {
        return resp_error("wrong number of arguments for BLPOP");
    }
    if let RedisValueRef::String(liste) = &elements[1]
        && let RedisValueRef::String(timeout) = &elements[2]
    {
        let mut store = arc.lock().await;

        let liste_string = String::from_utf8_lossy(&liste).to_string();
        let liste = store.data.get_mut(&liste_string);

        match liste {
            Some(liste) => {
                if let RedisValue::List(liste) = liste {
                    if liste.len() == 0 {
                        let (tx, rx) = oneshot::channel();

                        let timeout_secs: f64 = String::from_utf8_lossy(timeout).parse().unwrap();

                        store
                            .waiters
                            .entry(liste_string.clone())
                            .or_insert(VecDeque::new())
                            .push_back(tx);

                        drop(store);

                        if timeout_secs == 0f64 {
                            match rx.await {
                                Ok(value) => resp_array(&[liste_string, value]),
                                Err(_) => resp_null_array(),
                            }
                        } else {
                            match tokio::time::timeout(Duration::from_secs_f64(timeout_secs), rx)
                                .await
                            {
                                Ok(Ok(value)) => resp_array(&[liste_string, value]),
                                _ => resp_null_array(),
                            }
                        }
                    } else {
                        let removed = liste.remove(0);
                        return resp_array(&[liste_string, removed]);
                    }
                } else {
                    return resp_null_bulk();
                }
            }
            None => {
                let (tx, rx) = oneshot::channel();

                let timeout_secs: f64 = String::from_utf8_lossy(timeout).parse().unwrap();

                store
                    .waiters
                    .entry(liste_string.clone())
                    .or_insert(VecDeque::new())
                    .push_back(tx);

                drop(store);

                if timeout_secs == 0f64 {
                    match rx.await {
                        Ok(value) => resp_array(&[liste_string, value]),
                        Err(_) => resp_null_array(),
                    }
                } else {
                    match tokio::time::timeout(Duration::from_secs_f64(timeout_secs), rx).await {
                        Ok(Ok(value)) => resp_array(&[liste_string, value]),
                        _ => resp_null_array(),
                    }
                }
            }
        }
    } else {
        resp_error("not a string")
    }
}

async fn cmd_type(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
    if elements.len() != 2 {
        return resp_error("wrong number of arguments for TYPE");
    }
    if let RedisValueRef::String(key) = &elements[1] {
        let store = arc.lock().await;
        let key_string = String::from_utf8_lossy(&key).to_string();
        let key = store.data.get(&key_string);

        match key {
            Some(key) => match key {
                RedisValue::String(_, _) => {
                    return resp_simple("string");
                }
                RedisValue::List(_) => {
                    return resp_simple("list");
                }
                RedisValue::Stream(_) => {
                    return resp_simple("stream");
                }
            },
            None => resp_simple("none"),
        }
    } else {
        resp_error("not a string")
    }
}

async fn cmd_xadd(elements: &[RedisValueRef], arc: &Store) -> Vec<u8> {
    let elements_div_2 = elements.len() % 2 == 0;
    if elements_div_2 {
        // pas d'ID
        resp_error("not implemented !")
    } else {
        // avec ID
        if elements.len() < 5 {
            return resp_error("wrong number of arguments for XADD");
        }

        if let RedisValueRef::String(stream) = &elements[1] {
            let mut store = arc.lock().await;
            let stream_string = String::from_utf8_lossy(&stream).to_string();
            let entry = store
                .data
                .entry(stream_string)
                .or_insert(RedisValue::Stream(vec![]));
            if let RedisValue::Stream(stream) = entry {
                if let RedisValueRef::String(id) = &elements[2] {
                    let id_string: String = String::from_utf8_lossy(&id).to_string();
                    let id_split: Vec<&str> = id_string.split("-").collect();
                    let id: String;
                    if id_string.trim() == "*" {
                        // Fully auto-generated IDs
                        id = id_string;
                    } else {
                        let ms: u64 = id_split[0].parse().unwrap_or(0u64);
                        if id_split[1].trim() == "*" {
                            // Partially auto-generated IDs
                            let last_stream_element = stream.get(stream.len() - 1);
                            match last_stream_element {
                                Some(element) => {
                                    let element_split: Vec<&str> = element.0.split("-").collect();
                                    let element_ms: u64 = element_split[0].parse().unwrap_or(0u64);
                                    if ms < element_ms {
                                        return resp_error(
                                            "The ID specified in XADD is equal or smaller than the target stream top item",
                                        );
                                    } else if ms == element_ms {
                                        let element_seq: u64 =
                                            element_split[1].parse().unwrap_or(0u64);

                                        id = format!("{}-{}", ms, element_seq + 1);
                                    } else {
                                        id = format!("{}-{}", ms, 0);
                                    }
                                }
                                None => {
                                    if ms == 0 {
                                        id = format!("{}-{}", ms, 1);
                                    } else {
                                        id = format!("{}-{}", ms, 0);
                                    }
                                }
                            }
                        } else {
                            // Manual IDs
                            let seq: u64 = id_split[1].parse().unwrap_or(0u64);
                            id = id_string;

                            if ms == 0 && seq == 0 {
                                return resp_error(
                                    "The ID specified in XADD must be greater than 0-0",
                                );
                            };

                            let last_stream_element = stream.get(stream.len() - 1);
                            match last_stream_element {
                                Some(element) => {
                                    let element_split: Vec<&str> = element.0.split("-").collect();
                                    let element_ms: u64 = element_split[0].parse().unwrap_or(0u64);
                                    if ms < element_ms {
                                        return resp_error(
                                            "The ID specified in XADD is equal or smaller than the target stream top item",
                                        );
                                    }
                                    if ms == element_ms {
                                        let element_seq: u64 =
                                            element_split[1].parse().unwrap_or(0u64);
                                        if seq <= element_seq {
                                            return resp_error(
                                                "The ID specified in XADD is equal or smaller than the target stream top item",
                                            );
                                        }
                                    }
                                }
                                None => {}
                            }
                        }
                    }

                    let mut vec: Vec<(String, String)> = vec![];

                    for pairs in elements[3..].chunks(2) {
                        if let RedisValueRef::String(key) = &pairs[0]
                            && let RedisValueRef::String(val) = &pairs[1]
                        {
                            vec.push((
                                String::from_utf8_lossy(&key).to_string(),
                                String::from_utf8_lossy(&val).to_string(),
                            ))
                        }
                    }

                    let reponse = resp_bulk(&id);
                    stream.push((id, vec));

                    return reponse;
                } else {
                    resp_error("")
                }
            } else {
                resp_error("msg")
            }
        } else {
            resp_error("not a string")
        }
    }
}
