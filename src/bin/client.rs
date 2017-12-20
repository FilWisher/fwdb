#[macro_use]
extern crate serde_derive;
extern crate bincode;

use std::env;
use std::io::Read;
use std::os::unix::net::{UnixStream, UnixListener};
use std::thread;
use std::error::Error;

use bincode::{serialized_size, serialize_into, serialize, deserialize, deserialize_from, Infinite};

#[derive(Debug, Serialize, Deserialize)]
enum Cmd {
    Set(String, String),
    Get(String),
}

#[derive(Debug, Serialize, Deserialize)]
enum Response {
    Ok(String),
    Err(String),
}

fn usage() -> String {
    "lol".to_string()
}

fn get(mut socket: UnixStream, args: Vec<String>) -> Result<Response, String> {
    let key = try!(args.get(2)
        .ok_or("Not enough args to `get`".to_string())
        .map_err(|s| s.to_string()));
    try!(serialize_into(&mut socket, &Cmd::Get(key.clone()), Infinite)
         .map_err(|e| e.description().to_string()));
    let decoded: Result<Response, bincode::Error> = deserialize_from(&mut socket, Infinite);
    decoded.map_err(|e| e.description().to_string())
}

fn set(mut socket: UnixStream, args: Vec<String>) -> Result<Response, String> {
    let key = try!(args.get(2)
        .ok_or("Not enough args to `get`".to_string())
        .map_err(|s| s.to_string()));
    let value = try!(args.get(3)
        .ok_or("Not enough args to `get`".to_string())
        .map_err(|s| s.to_string()));
    try!(serialize_into(&mut socket, &Cmd::Set(key.clone(), value.clone()), Infinite)
         .map_err(|e| e.description().to_string()));
    let decoded: Result<Response, bincode::Error> = deserialize_from(&mut socket, Infinite);
    decoded.map_err(|e| e.description().to_string())
}

fn main() {

    let args: Vec<String> = env::args().collect();
    println!("thanks for the args: {:?}", args);

    let socket = match UnixStream::connect("fwdb.hello.sock") {
        Ok(sock) => sock,
        Err(e) => {
            println!("Couldn't connect: {:?}", e);
            return
        }
    };

    let res = match args.get(1).map(|s| s.as_ref()) {
        Some("set") => set(socket, args),
        Some("get") => get(socket, args),
        Some(_)     => Err(usage()),
        None        => Err(usage()),
    };

    match res {
        Ok(r) => println!("Nice! get: {:?}", r), 
        Err(e) => println!("You prick: {:?}", e), 
    }
}
