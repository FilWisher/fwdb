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

use bincode::Result as BincodeResult;
use bincode::{serialize_into, deserialize_from, Infinite};

use std::os::unix::net::{UnixStream, UnixListener};
use std::error::Error;

extern crate fwdb;

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

fn handle(mut stream: UnixStream, db: &mut database::Database) {
    let decoded: BincodeResult<database::Cmd> = deserialize_from(&mut stream, Infinite);
    match decoded {
        Ok(cmd) => {
            let res = run_cmd(cmd, db);
            serialize_into(&mut stream, &res, Infinite);
        }
        Err(e) => {
            println!("you prick: {:?}", e);
            serialize_into(&mut stream, &database::Response::Err(e.description().to_string()), Infinite);
        }
    }
}

fn main() {

    let conf = database::DatabaseConfig{
        memtable_size: 200,
        block_size: 100,
        
        name: "hello".to_string(),
        data_dir: "/var/db/".to_string(),
    };
    let db = &mut database::Database::new(&conf);

    let listener = UnixListener::bind("fwdb.hello.sock").unwrap();

    // TODO: while not concurrency-safe, only handle one connection at a time
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle(stream, db)
            }
            Err(err) => {
                println!("Error receiving: {:?}", err);
                break;
            }
        } 
    }
}
