#![allow(unused_imports)]
use std::{
    io::{Read, Write},
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
                let buffer_response = b"+PONG\r\n";
                let mut buffer_input = [0; 512];

                // loop over potential multiple commands
                loop {
                    // write input to buffer
                    let input = stream.read(&mut buffer_input);

                    // if not input, ignore
                    match input {
                        Ok(bytes) => {
                            if bytes == 0 {
                                return;
                            }
                        }
                        Err(_) => {
                            print!("Erreur lors de la lecture de l'input")
                        }
                    }

                    let result = stream.write_all(buffer_response);
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
