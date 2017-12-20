extern crate bincode;
extern crate fwdb;

use std::env;
use std::os::unix::net::UnixStream;
use std::error::Error;

use fwdb::*;

use bincode::{serialize_into, deserialize_from, Infinite};

fn usage() -> String {
    "lol".to_string()
}

fn get(mut socket: UnixStream, args: Vec<String>) -> Result<database::Response, String> {
    let key = try!(args.get(2)
        .ok_or("Not enough args to `get`".to_string())
        .map_err(|s| s.to_string()));
    try!(serialize_into(&mut socket, &database::Cmd::Get(key.clone()), Infinite)
         .map_err(|e| e.description().to_string()));
    let decoded: Result<database::Response, bincode::Error> = deserialize_from(&mut socket, Infinite);
    decoded.map_err(|e| e.description().to_string())
}

fn set(mut socket: UnixStream, args: Vec<String>) -> Result<database::Response, String> {
    let key = try!(args.get(2)
        .ok_or("Not enough args to `get`".to_string())
        .map_err(|s| s.to_string()));
    let value = try!(args.get(3)
        .ok_or("Not enough args to `get`".to_string())
        .map_err(|s| s.to_string()));
    try!(serialize_into(&mut socket, &database::Cmd::Set(key.clone(), value.clone()), Infinite)
         .map_err(|e| e.description().to_string()));
    let decoded: Result<database::Response, bincode::Error> = deserialize_from(&mut socket, Infinite);
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
