#![allow(unused_imports)]
use std::{io::ErrorKind, vec};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() {
    // bind to :6379
    let listener: TcpListener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    // main loop
    loop {
        // accepte new incoming connections
        let (stream, _): (TcpStream,_) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_connection(stream).await;
        });
    }
}

async fn handle_connection(mut stream: TcpStream) -> bool {
    let buffer_response = b"+PONG\r\n";
    let mut buffer_input = [0; 512];

    // write input to buffer
    let input: Result<usize, std::io::Error> = stream.read(&mut buffer_input).await;

    // if not input, ignore
    match input {
        Ok(bytes) => {
            if bytes == 0 {
                return false;
            }

            // envoyer la réponse
            let result: Result<(), std::io::Error> = stream.write_all(buffer_response).await;

            match result {
                Ok(_) => {
                    return true;
                }
                Err(_) => {
                    eprintln!("Erreur lors de l'envoie de la réponse");
                    return false;
                }
            }
        }
        Err(_) => {
            eprintln!("Erreur lors de la lecture de l'input");
            return false;
        }
    }
}
