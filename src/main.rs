#![allow(unused_imports)]
use std::{
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    vec,
};

fn main() {
    // bind to :6379
    let listener: TcpListener = TcpListener::bind("127.0.0.1:6379").unwrap();

    // set non blocking
    listener.set_nonblocking(true).unwrap();

    let mut list_streams: Vec<TcpStream> = vec![];

    // main loop
    loop {
        // accepte new incoming connections
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("accepted new connection");
                    stream.set_nonblocking(true).unwrap();
                    list_streams.push(stream);
                }
                Err(e) => {
                    if e.kind() == ErrorKind::WouldBlock {
                        break;
                    } else {
                        eprint!("Erreur lors de la connection : {}", e)
                    }
                }
            }
        }

        // essaie de répondre, prob a modifier
        for stream in &mut list_streams {
            handle_connection(stream);
        }
    }
}

fn handle_connection(stream: &mut TcpStream) {
    let buffer_response = b"+PONG\r\n";
    let mut buffer_input = [0; 512];

    // write input to buffer
    let input = stream.read(&mut buffer_input);

    // if not input, ignore
    match input {
        Ok(bytes) => {
            if bytes == 0 {
                return;
            }

            // envoyer la réponse
            let result = stream.write_all(buffer_response);

            match result {
                Ok(_) => {
                    println!("Réponse envoyer avec succes !")
                }
                Err(_) => {
                    eprintln!("Erreur lors de l'envoie de la réponse")
                }
            }
        }
        Err(e) => {
            if e.kind() != ErrorKind::WouldBlock {
                eprintln!("Erreur lors de la lecture de l'input")
            }
        }
    }
}
