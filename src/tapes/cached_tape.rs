use std::{
    cmp::{Ordering, max},
    io,
    sync::Arc,
};

use parking_lot::RwLock;

use crate::{
    Persistence,
    metadata::TapeMetadata,
    traits::{BlobTape, BlobTapeWriter, OpenConfig},
};

mod ring_buffer;
use ring_buffer::RingBuffer;

/// Open options for a [`CachedBlobTape`].
pub struct CachedTapeOpenOptions<B: BlobTape> {
    /// The inner tapes' config.
    pub inner: B::OpenConfig,
    /// The size of the top cache in bytes, this amount of data from the top of the tape will be cached in memory.
    pub top_cache_size: u64,
}

impl<B: BlobTape> OpenConfig for CachedTapeOpenOptions<B> {
    fn start_index(&self) -> u64 {
        self.inner.start_index()
    }
}

impl<B: BlobTape<OpenConfig: Clone>> Clone for CachedTapeOpenOptions<B> {
    fn clone(&self) -> Self {
        CachedTapeOpenOptions {
            inner: self.inner.clone(),
            top_cache_size: self.top_cache_size,
        }
    }
}

/// A wrapper for a [`BlobTape`] that caches bytes from the top of the tape in memory to speed
/// up access and reduce disk I/O.
pub struct CachedBlobTape<B: BlobTape> {
    tape: B,
    cache: Arc<RwLock<RingBuffer>>,
}

impl<B: BlobTape + 'static> BlobTape for CachedBlobTape<B> {
    type OpenConfig = CachedTapeOpenOptions<B>;
    type Writer = CachedBlobTapeWriter<B>;

    fn name(&self) -> &'static str {
        self.tape.name()
    }

    fn open(
        name: &'static str,
        tape_metadata: Option<TapeMetadata>,
        current_epoch: u64,
        config: Self::OpenConfig,
    ) -> io::Result<Self> {
        let tape = B::open(name, tape_metadata, current_epoch, config.inner)?;

        let metadata = tape_metadata.unwrap_or_default();
        let start = max(
            metadata.len.saturating_sub(config.top_cache_size),
            metadata.start,
        );

        let mut ring_buffer = RingBuffer::new(config.top_cache_size as usize, 0);
        let buf = ring_buffer.reset((metadata.len - start) as usize, start as usize);

        tape.read_bytes(start, buf)?;

        let cache = Arc::new(RwLock::new(ring_buffer));

        Ok(Self { tape, cache })
    }

    fn read_bytes(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let top_cache = self.cache.read();
        let cached_offset = top_cache.cache_start_idx() as u64;

        let mut last_byte_needed_offset = offset + buf.len() as u64;

        if last_byte_needed_offset > cached_offset {
            let read_start = offset.saturating_sub(cached_offset);
            let buf_to_fill = &mut buf[(cached_offset.saturating_sub(offset)) as usize..];

            top_cache.fill(read_start as usize, buf_to_fill);
            last_byte_needed_offset -= buf_to_fill.len() as u64;
        }

        if last_byte_needed_offset != offset {
            self.tape.read_bytes(
                offset,
                &mut buf[0..(last_byte_needed_offset - offset) as usize],
            )?;
        }

        Ok(())
    }

    fn writer(&self, len: u64) -> io::Result<Self::Writer> {
        Ok(CachedBlobTapeWriter {
            ring_buffer: self.cache.clone(),
            tape_writer: self.tape.writer(len)?,
            bytes_to_flush: 0,
            len,
            is_truncation: None,
        })
    }

    fn delete(self) -> io::Result<()> {
        self.tape.delete()
    }
}

/// A writer that writes to a [`CachedBlobTape`].
///
/// The cache is flushed to disk when it is full, or on commit.
///
/// This should not be used directly.
pub struct CachedBlobTapeWriter<B: BlobTape> {
    pub(crate) ring_buffer: Arc<RwLock<RingBuffer>>,
    pub(crate) bytes_to_flush: usize,
    pub(crate) tape_writer: B::Writer,
    pub(crate) len: u64,
    is_truncation: Option<bool>,
}

impl<B: BlobTape> BlobTapeWriter for CachedBlobTapeWriter<B> {
    fn write_bytes(&mut self, buf: &[u8]) -> io::Result<u64> {
        assert!(self.is_truncation.is_none_or(|x| !x));
        self.is_truncation = Some(false);

        let mut ring_buffer = self.ring_buffer.write();
        ring_buffer.prepare_write(self.len as usize);
        let capacity = ring_buffer.capacity();

        // Writing enough data to completely fill the ring buffer.
        if buf.len() >= capacity {
            flush::<B>(
                &mut self.tape_writer,
                &ring_buffer,
                self.bytes_to_flush,
                Persistence::Buffer,
            )?;
            self.bytes_to_flush = 0;

            self.tape_writer.write_bytes(buf)?;

            ring_buffer.push(&buf[buf.len() - capacity..], buf.len() - capacity)
        }
        // Writing enough data to push data that hasn't been flushed to disk yet out of the ring buffer.
        else if self.bytes_to_flush + buf.len() > capacity {
            // Just flush everything that needs to be flushed to disk to reduce the number of flushes.
            flush::<B>(
                &mut self.tape_writer,
                &ring_buffer,
                self.bytes_to_flush,
                Persistence::Buffer,
            )?;
            self.bytes_to_flush = 0;

            self.bytes_to_flush += buf.len();
            ring_buffer.push(buf, 0)
        }
        // Writing data that won't push data that hasn't been flushed to disk yet out of the ring buffer.
        else {
            self.bytes_to_flush += buf.len();
            ring_buffer.push(buf, 0)
        }

        let old_len = self.len;
        self.len += buf.len() as u64;

        Ok(old_len)
    }

    fn flush(&mut self, persistence: Persistence) -> io::Result<()> {
        if self.is_truncation.is_some_and(|x| x) {
            self.ring_buffer.write().truncate(self.len as usize);
            return self.tape_writer.flush(persistence);
        }

        flush::<B>(
            &mut self.tape_writer,
            &self.ring_buffer.read(),
            self.bytes_to_flush,
            persistence,
        )
    }

    fn truncate(&mut self, new_len: u64) {
        assert!(self.is_truncation.is_none_or(|x| x));
        self.is_truncation = Some(true);

        self.len = new_len;
        self.tape_writer.truncate(new_len)
    }

    fn revert(&mut self, bytes: usize) {
        self.ring_buffer.write().pop(bytes);
    }

    fn len(&self) -> u64 {
        self.len
    }

    fn remove_old_files(
        &self,
        metadata: TapeMetadata,
        current_epoch: u64,
        oldest_reader_epoch: u64,
    ) -> io::Result<()> {
        self.tape_writer
            .remove_old_files(metadata, current_epoch, oldest_reader_epoch)
    }
}

fn flush<B: BlobTape>(
    tape_writer: &mut B::Writer,
    ring_buffer: &RingBuffer,
    bytes_to_flush: usize,
    persistence: Persistence,
) -> io::Result<()> {
    if bytes_to_flush != 0 {
        let (fist_slice, second_slice) = ring_buffer.as_slices();
        match bytes_to_flush.cmp(&second_slice.len()) {
            Ordering::Less | Ordering::Equal => {
                tape_writer.write_bytes(&second_slice[second_slice.len() - bytes_to_flush..])?;
            }
            Ordering::Greater => {
                let first_slice_top_needed = bytes_to_flush - second_slice.len();

                tape_writer
                    .write_bytes(&fist_slice[fist_slice.len() - first_slice_top_needed..])?;
                tape_writer.write_bytes(second_slice)?;
            }
        }
    }

    tape_writer.flush(persistence)
}
