#![allow(unused_imports)]
use bytes::Bytes;
use std::collections::VecDeque;
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

    // main loop
    loop {
        // accepte new incoming connections
        let (stream, _): (TcpStream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_connection(stream).await;
        });
    }
}

async fn handle_connection(mut stream: TcpStream) {
    // diviser le stream en 2 partie, read et write pour le borrow checkekr
    let (read_stream, mut write_stream) = stream.split();

    // lire le stream avec tokio, il le lit, puis le passe auto danss le decoder RESP. Il en sort un RedisValueRef
    let mut frame = FramedRead::new(read_stream, RespParser);

    // loop sur plusieurs commands
    loop {
        // si il existe un prochain mot
        match frame.next().await {
            Some(Ok(value)) => {
                let response: &[u8] = &handle_command(value);

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

fn handle_command(value: RedisValueRef) -> Vec<u8> {
    match value {
        RedisValueRef::String(bytes) => match &bytes[..] {
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
                RedisValueRef::String(bytes) => match &bytes[..] {
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
                    b"SET" => b"test".to_vec(),
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
