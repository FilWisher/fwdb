/* data structures:
 *  SSTable: segment file of kvs, sorted by k (written to disk)
 *      Contains an index-block at the end with an index of the first key
 *      in each block of the SSTable
 *  Memtable: in-memory balanced tree of k-vs, sorted by k (becomes SSTable when full)
 *  Hash table: hash table of keys to offsets in SSTable
 *  Write-ahead log: a sequential, on-disk log of writes (for recovery)
 *
 * algorithms:
 *  o Insert: insert k-v into balanced tree. If tree is greater than 
 *    threshold size, write it to SSTable and flush.
 *  o Read: read from k-v, if not present, read from segment file, if not present, read from older
 *    segment file.
 *  o Merge: periodically, the SSTable files are compacted together
 */

extern crate bincode;
extern crate fwdb;

use std::os::unix::net::{UnixStream, UnixListener};
use std::error::Error;

use bincode::{serialize_into, deserialize_from, Infinite};

use fwdb::*;

fn run_cmd(cmd: database::Cmd, db: &mut database::Database) -> database::Response {
    let res = match cmd {
        database::Cmd::Set(key, value) => db.set(&key, &value).map(|_| format!("set {}", key)),
        database::Cmd::Get(key) => db.get(&key),
    };

    match res {
        Ok(msg) => database::Response::Ok(msg),
        Err(e) => database::Response::Err(e.description().to_string())
    }
}

fn handle(mut stream: UnixStream, db: &mut database::Database) -> database::Result<()> {
    let decoded: database::Result<database::Cmd> = deserialize_from(&mut stream, Infinite)
        .map_err(|e| database::Error::from(e));
    match decoded {
        Ok(cmd) => {
            let res = run_cmd(cmd, db);
            serialize_into(&mut stream, &res, Infinite).map_err(|e| database::Error::from(e))
        }
        Err(e) => {
            serialize_into(&mut stream, &database::Response::Err(e.description().to_string()), Infinite)?;
            Err(e)
        }
    }
}

fn main() {

    // TODO: parse this config from a conf file
    let conf = database::DatabaseConfig{
        memtable_size: 200,
        block_size: 100,
        
        name: "hello",
        data_dir: "/var/db/",
    };
    let db = &mut database::Database::new(&conf).unwrap();

    let listener = UnixListener::bind("fwdb.hello.sock").unwrap();

    // TODO: while not concurrency-safe, only handle one connection at a time
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                match handle(stream, db) {
                    Ok(_) => println!("ok"),
                    Err(e) => println!("you prick: {:?}", e),
                }
            }
            Err(err) => {
                println!("Error receiving: {:?}", err);
                break;
            }
        } 
    }
}
