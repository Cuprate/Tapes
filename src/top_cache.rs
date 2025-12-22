use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{BufWriter, Write};
use std::mem::MaybeUninit;
use std::sync::RwLock;
use bytes::BytesMut;
use crate::ring_buffer::RingBuffer;

pub enum Persistence {
    Buffer,
    SyncData,
    SyncAll,
}

pub struct AppendTransaction {
    modified_tapes: HashMap<&'static str, TapeAppender>
}

impl AppendTransaction {
    pub fn append(&mut self, tape: &Tape, bytes: &[u8]) -> io::Result<()> {
        let tape_appender = self.modified_tapes.entry(tape.name).or_insert_with(|| {
            let file = tape.file.try_clone().expect("TODO");
            TapeAppender { file: BufWriter::new(file), bytes_written: 0 }
        });
        
        tape_appender.bytes_written += bytes.len();
        tape_appender.file.write_all(bytes)
    }
    
    pub fn commit(&mut self, persistence: Persistence) -> io::Result<()> {
        for tape_appender in self.modified_tapes.values_mut() {
            tape_appender.file.flush()?;
            
            match persistence {
                Persistence::Buffer => (),
                Persistence::SyncData=> tape_appender.file.get_ref().sync_data()?,
                Persistence::SyncAll => tape_appender.file.get_ref().sync_all()?,
            }
        }
        Ok(())
    }
}

pub struct TopCache {
    ring_buffer: RingBuffer,
    offset: usize,
}

pub struct TapeAppender {
    file: BufWriter<File>,
    bytes_written: usize,
}

pub struct Tape {
    name: &'static str,
    file: File,
    top_cache: RwLock<TopCache>
}



