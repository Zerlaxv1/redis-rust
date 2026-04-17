#![allow(unused_imports)]
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener: TcpListener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let buffer = b"+PONG\r\n";

                // loop over potential multiple commands
                loop {
                    // try to use write_all
                    let result = stream.write(buffer);
                    match result {
                        Ok(_) => {
                            print!("Réponse envoyer avec succes !")
                        }
                        Err(_) => {
                            print!("Erreur lors de l'envoie de la réponse")
                        }
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
