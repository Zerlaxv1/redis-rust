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
    println!("{:?}", value);
    match value {
        RedisValueRef::String(bytes) => match *bytes == *b"PING" {
            true => b"+PONG\r\n".to_vec(),
            // faire un genre de switch ??
            false => todo!(),
        },
        RedisValueRef::Error(bytes) => {
            return b"+ERROR\r\n".to_vec();
        }
        RedisValueRef::Int(_) => {
            return b"+INT\r\n".to_vec();
        }
        RedisValueRef::Array(mut elements) => return handle_command(elements.remove(0)),
        RedisValueRef::NullArray => {
            return b"+NULLARRAY\r\n".to_vec();
        }
        RedisValueRef::NullBulkString => {
            return b"+NullBulkString\r\n".to_vec();
        }
    }
}
