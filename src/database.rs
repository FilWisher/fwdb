extern crate bincode;

use std::fmt;
use std::error::Error as StdError;
use std::io::Error as IOError;
use bincode::Error as BincodeError;
use bincode::Result as BincodeResult;
use std::fs::{File,OpenOptions};
use std::io::{SeekFrom,Seek,Write};
use std::result::Result as StdResult;

use std::collections::{BTreeMap};

use bincode::{serialized_size, serialize_into, serialize, deserialize_from, Infinite};

#[derive(Debug)]
pub enum Error {
    NotFound,
    IOError(IOError),
    BincodeError(BincodeError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Error::NotFound => f.write_str("NotFound"),
            &Error::IOError(_) => f.write_str("IOError(e)"),
            &Error::BincodeError(_) => f.write_str("BincodeError(e)"),
        }
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        match *self {
            Error::NotFound => "Key not found",
            Error::IOError(_) => "IO Error",
            Error::BincodeError(_) => "Error bincoding",
        }
    }
}

impl From<IOError> for Error {
    fn from(e: IOError) -> Self {
        Error::IOError(e)
    } 
}

impl From<BincodeError> for Error {
    fn from(e: BincodeError) -> Self {
        Error::BincodeError(e)
    }
}


pub type Result<T> = StdResult<T, Error>;

struct Memtable {
    table: BTreeMap<String, String>,
    // current length
    len: usize,
    block_size: usize,
}

/* SSTable format:
 *  <Block 1>
 *  ...
 *  <Block N>
 *  <IndexBlock>
 *  <IndexBlock size>
 *
 *  The IndexBlock is a list of IndexEntry structs. Each IndexEntry
 *  has an offset to a block and the key of the first entry in that
 *  block. We can use a binary search in the IndexBlock to find the
 *  offset of the block that will contain the key we're searching
 *  for (if it exists).
 */

// A list of index entries. binary search to find IndexEntry in which 
// k is likely to exist
#[derive(Serialize, Deserialize, Debug)]
struct IndexBlock {
    content: Vec<IndexEntry>
}

impl IndexBlock {
    fn new() -> Self {
        IndexBlock {
            content: Vec::new(),
        }
    }

    fn from_file(f: &mut File) -> self::Result<Self> {
        let mut idx_size: i64 = 0;
        let len = serialized_size(&idx_size) as i64;
        f.seek(SeekFrom::End(-len));
        idx_size = deserialize_from(f, Infinite).unwrap();
        f.seek(SeekFrom::End(-(len+idx_size)));
        let idx: IndexBlock = deserialize_from(f, Infinite).unwrap();
        Ok(idx)
    }

    #[allow(dead_code)]
    // binary search for the IndexEntry
    fn find_block(&self, k: String) -> Option<IndexEntry> {
        None 
    }

    fn insert(&mut self, s: String, off: u64) {
        self.content.push(IndexEntry {
            key: s,
            off: off,
        })
    }

    fn get_offset_for(&self, k: String) -> Option<u64> {
        // TODO: binary search for the right entry, don't linear search
        let iter = self.content.iter().take_while(|entry| k >= entry.key );
        let mut elm = None;
        for entry in iter  {
            elm = Some(entry.off);
        }
        elm
    }
}

// In-memory representation of on-disk SSTable
struct SSTable {
    idx: Option<IndexBlock>,
    file: Option<File>,
    filename: String,
}

impl SSTable {
    // TODO: should this be initialized on boot or done lazily?
    fn new(filename: String) -> Self {
        SSTable {
            idx: None,
            file: None,
            filename: filename.clone(),
        } 
    }
   
    // Attempt to read value from on-disk sstable. If file not open,
    // open it. If index-block not loaded into memory, load it.
    fn get(&mut self, key: String) -> self::Result<String> {
        if self.file.is_none() {
            let f = try!(File::open(&self.filename));
            self.file = Some(f);
        }
        match self.file {
            Some(ref mut f) => {
                if self.idx.is_none() {
                    let idx = try!(IndexBlock::from_file(f));
                    self.idx = Some(idx);
                }
                match &self.idx {
                    &Some(ref b) => {
                        match b.get_offset_for(key.clone()) {
                            None => Err(Error::NotFound),
                            Some(off) => {
                                Block::from_file(f, off)
                                    .and_then(|block| block.get(key)
                                              .ok_or(Error::NotFound))
                            }
                        }
                    },
                    &None => Err(Error::NotFound),
                }
            },
            None => Err(Error::NotFound),
        }
    }
}

// Index to the offset `off` of the block whose first key is `key`.
#[derive(Serialize, Deserialize, Debug)]
struct IndexEntry {
    key: String,
    off: u64,
}

// A block of KVPair structs, ordered on keys.
#[derive(Serialize,Deserialize, Debug)]
struct Block {
    len: usize,
    content: Vec<KVPair>,
}

impl Block {
    fn new() -> Self {
        Block {
            len: 0,
            content: Vec::new(),
        }
    }

    fn from_file(f: &mut File, off: u64) -> self::Result<Self> {
        f.seek(SeekFrom::Start(off));
        let b: Block = deserialize_from(f, Infinite).unwrap();
        Ok(b)
    }

    fn insert(&mut self, p: KVPair) {
        self.len += p.len();
        self.content.push(p);
    }

    fn first_key(&self) -> Option<String> {
        self.content.get(0).map(|kv| kv.k.clone())
    }

    // get value from block
    fn get(&self, key: String) -> Option<String> {
        self.content.binary_search_by(|kv| kv.k.cmp(&key))
            .ok()
            .and_then(|i| self.content.get(i))
            .and_then(|kv| Some(kv.v.clone()))
    }
}

impl Memtable {
    fn new(block_size: usize) -> Self {
        Memtable {
            table: BTreeMap::new(),
            len: 0,
            block_size: block_size,
        } 
    }

    fn insert(&mut self, k: &str, v: &str) -> Option<String> {
        // TODO: should actually use serialized_size
        self.len += k.len() + v.len();
        self.table.insert(k.to_string(), v.to_string())    
    }

    // Dump memtable to a Vec of Blocks of max BLOCK_SIZE len.
    fn to_blocks(&mut self) -> Vec<Block> {
        let mut blocks: Vec<Block> = Vec::new();
        let mut b = Block::new();
        for (key, value) in self.table.iter() {
            if b.len + key.len() + value.len() > self.block_size {
                blocks.push(b);
                b = Block::new();
            }
            b.insert(KVPair::new(key, value));
        }
        blocks.push(b);
        return blocks;
    }
}

pub struct Database<'a> {
    conf: &'a DatabaseConfig,
    logfile: Log,
    memtable: Memtable,
    sstables: Vec<SSTable>,
}

#[derive(Serialize,Deserialize, Debug)]
struct KVPair {
    k: String,
    v: String,
}

impl KVPair {
    fn new(k: &str, v: &str) -> Self {
        KVPair {
            k: k.to_string(),
            v: v.to_string(),
        }
    }

    fn len(&self) -> usize {
        self.k.len() + self.v.len()
    }
}

struct Log {
    file: File,
}

impl Log {
    fn new(name: String) -> Self {
        Log {
            file: OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(name.clone())
                .unwrap(),
        }
    }

    fn record(&mut self, key: &str, value: &str) {
        let encoded = serialize(&KVPair::new(key, value), Infinite).unwrap();
        self.file.write_all(&encoded);
        self.file.sync_data();
    }

    // TODO: call this on database initialization
    fn recover_memtable(&mut self, block_size: usize) -> self::Result<Memtable> {
        self.file.seek(SeekFrom::Start(0));
        let mut memtable = Memtable::new(block_size);
        loop {
            let decoded: BincodeResult<KVPair> = deserialize_from(&mut self.file, Infinite);
            match decoded {
                Err(_) => {
                    break;
                }
                Ok(d) => {
                    memtable.insert(&d.k, &d.v);
                }
            }
        }
        Ok(memtable)
    }
}

pub struct DatabaseConfig {
    pub memtable_size: usize,
    pub block_size: usize,

    pub name: String, 
    pub data_dir: String,

    /*
     * logfile: <data_dir>/<name>.log
     * sstables: <data_dir>/<name><n>.db
     */
}

impl<'a> Database<'a> {
    pub fn new(conf: &'a DatabaseConfig) -> Self {
        return Database {
            conf: conf,
            logfile: Log::new(format!("{}.log", conf.name.clone())),
            memtable: Memtable::new(conf.block_size),
            sstables: Vec::new(),
        }
    }

    // Set `key` to `value`
    pub fn set(&mut self, key: &str, value: &str) -> self::Result<()> {
        self.logfile.record(key, value);
        if self.memtable.len + KVPair::new(key, value).len() > self.conf.memtable_size {
            let filename = format!("{}.db", self.conf.name);
            let file = &mut try!(File::create(&filename));
            self.serialize_memtable(file);
            let s = SSTable::new(filename.clone());
            self.sstables.push(s);
            self.memtable = Memtable::new(self.conf.block_size);
        }
        self.memtable.insert(key, value);
        Ok(())
    }

    // Fetch `key` from database. Searches `memtable` and `sstables` stack.
    pub fn get(&mut self, key: &String) -> self::Result<String> {
        match self.memtable.table.get(key) {
            Some(v) => Ok(v.to_string()),
            None => {
                for sstable in &mut self.sstables {
                    match sstable.get(key.clone()) {
                        Err(_) => {
                            continue;
                        }
                        Ok(v) => {
                            return Ok(v);
                        }
                    }
                }
                return Err(Error::NotFound);
            }
        }
    }

    // TODO: attempt to recover memtable from logfile
    // TODO: attempt to recover sstables from <data_dir>/<name><n>.db
    fn recover(&mut self) {}

    // Serialize memtable to on-disk sstable.
    fn serialize_memtable(&mut self, file: &mut File) {
        let blocks = self.memtable.to_blocks();
        let mut idx = IndexBlock::new();
        let mut off = 0;
        for block in blocks.iter() {
            match block.first_key() {
                Some(k) => {
                    idx.insert(k, off);
                    let size = serialized_size(block);
                    off = off + size;
                    serialize_into(file, &block, Infinite);
                }
                None => continue
            }
        }
        let idx_size = serialized_size(&idx);
        serialize_into(file, &idx, Infinite);
        match serialize_into(file, &idx_size, Infinite) {
            Err(e) => Err(e.description().to_string()),
            _ => Ok(())
        };
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub enum Cmd {
    Set(String, String),
    Get(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok(String),
    Err(String),
}
