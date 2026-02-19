use std::{
    cmp::{Ordering, min},
    fs::File,
    io,
    sync::Arc,
};

use parking_lot::RwLock;

use crate::Persistence;

/// A simple ring buffer to cache the top of a tape.
#[derive(Debug)]
pub struct RingBuffer {
    buf: Box<[u8]>,
    /// The length of the ring buffer, will not be more than `buf.len()`.
    len: usize,
    /// The index to start reading from in `buf`.
    start_idx: usize,

    /// The start of the cache as an index in the tape.
    // TODO: Is this structure a good place for this?
    cache_start_idx: usize,
}

impl RingBuffer {
    /// Create a new [`RingBuffer`] with a given capacity and start index in the tape.
    pub fn new(capacity: usize, cache_start_idx: usize) -> Self {
        Self {
            buf: vec![0; capacity].into_boxed_slice(),
            len: 0,
            start_idx: 0,
            cache_start_idx,
        }
    }

    /// Reset the [`RingBuffer`] to the given length and start index in the tape.
    ///
    /// # Returns
    ///
    /// This will return a mutable slice with the length `len` for data to be written to.
    pub fn reset(&mut self, len: usize, cache_start_idx: usize) -> &mut [u8] {
        self.len = len;
        self.cache_start_idx = cache_start_idx;
        self.start_idx = 0;
        &mut self.buf[..len]
    }

    /// Fully fill the given `buf` with data from the [`RingBuffer`], starting at the [`start`] index.
    pub fn fill(&self, mut start: usize, buf: &mut [u8]) {
        let (first_slice, second_slice) = self.as_slices();

        let bytes_to_copy = min(first_slice.len().saturating_sub(start), buf.len());
        if bytes_to_copy != 0 {
            buf[..bytes_to_copy].copy_from_slice(&first_slice[start..(start + bytes_to_copy)]);
            start += bytes_to_copy;
        }

        start = start.saturating_sub(first_slice.len());
        let leftover_len = buf.len() - bytes_to_copy;
        let second_bytes_to_copy = min(second_slice.len().saturating_sub(start), leftover_len);
        if second_bytes_to_copy != 0 {
            buf[bytes_to_copy..(bytes_to_copy + second_bytes_to_copy)]
                .copy_from_slice(&second_slice[start..(start + second_bytes_to_copy)]);
        }
    }

    /// Returns the capacity of the [`RingBuffer`]
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Returns a tuple of slices which represent the [`RingBuffer`], returned in the order the data
    /// is in the tape.
    pub fn as_slices(&self) -> (&[u8], &[u8]) {
        let end_idx = min(self.start_idx + self.len, self.buf.len());
        let first_slice = &self.buf[self.start_idx..end_idx];
        let remaining = self.len - first_slice.len();
        let second_slice = &self.buf[..remaining];
        (first_slice, second_slice)
    }

    /// Push some data to the tape, potentially overwriting old data.
    ///
    /// # Panics
    ///
    /// This will panic if the data cannot fit into the [`RingBuffer`].
    pub fn push(&mut self, data: &[u8], missed_bytes: usize) {
        let data_len = data.len();
        assert!(data_len <= self.buf.len(), "Data too large for buffer");

        let end_idx = (self.start_idx + self.len) % self.buf.len();
        let space_until_end = self.buf.len() - end_idx;

        if data_len <= space_until_end {
            // Data fits without wrapping
            self.buf[end_idx..end_idx + data_len].copy_from_slice(data);
        } else {
            // Data needs to wrap around
            self.buf[end_idx..].copy_from_slice(&data[..space_until_end]);
            self.buf[..data_len - space_until_end].copy_from_slice(&data[space_until_end..]);
        }

        let new_len = self.len + data_len;
        if new_len > self.buf.len() {
            // Buffer overflow: move start_idx forward by the overflow amount
            let overflow = new_len - self.buf.len();
            self.start_idx = (self.start_idx + (overflow)) % self.buf.len();
            self.cache_start_idx += overflow;
        }

        self.cache_start_idx += missed_bytes;
        self.len = min(new_len, self.buf.len());
    }

    /// The first index in the tape that is cached.
    pub fn cache_start_idx(&self) -> usize {
        self.cache_start_idx
    }

    /// Pop some bytes from the tape.
    pub fn pop(&mut self, amount: usize) {
        if amount > self.len {
            self.cache_start_idx -= amount - self.len;
        }

        self.len = self.len.saturating_sub(amount);
    }
}

/// A file writer that writes to a [`RingBuffer`] and flushes to disk periodically.
pub struct RingBufferFileWriter {
    pub(crate) ring_buffer: Arc<RwLock<RingBuffer>>,
    pub(crate) bytes_to_flush: usize,
    pub(crate) file: Arc<File>,
    pub(crate) len: u64,
}

impl RingBufferFileWriter {
    /// Flush the buffer to disk.
    pub fn flush(&mut self, persistence: Persistence) -> io::Result<()> {
        flush(
            &self.file,
            &self.ring_buffer.read(),
            self.bytes_to_flush,
            self.len,
            persistence,
        )
    }

    /// Write some data to the tape.
    pub fn write(&mut self, data: &[u8]) -> io::Result<u64> {
        let mut ring_buffer = self.ring_buffer.write();
        let capacity = ring_buffer.capacity();

        // Writing enough data to completely fill the ring buffer.
        if data.len() >= capacity {
            flush(
                &self.file,
                &ring_buffer,
                self.bytes_to_flush,
                self.len,
                Persistence::Buffer,
            )?;
            self.bytes_to_flush = 0;

            write_all_at(&self.file, data, self.len)?;

            ring_buffer.push(&data[data.len() - capacity..], data.len() - capacity)
        }
        // Writing enough data to push data that hasn't been flushed to disk yet out of the ring buffer.
        else if self.bytes_to_flush + data.len() > capacity {
            // Just flush everything that needs to be flushed to disk to reduce the number of flushes.
            flush(
                &self.file,
                &ring_buffer,
                self.bytes_to_flush,
                self.len,
                Persistence::Buffer,
            )?;
            self.bytes_to_flush = 0;

            self.bytes_to_flush += data.len();
            ring_buffer.push(data, 0)
        }
        // Writing data that won't push data that hasn't been flushed to disk yet out of the ring buffer.
        else {
            self.bytes_to_flush += data.len();
            ring_buffer.push(data, 0)
        }

        let old_len = self.len;
        self.len += data.len() as u64;

        Ok(old_len)
    }
}

fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;

        file.write_all_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;

        let n = file.seek_write(buf, offset)?;
        if n != buf.len() {
            return Err(io::Error::other("Failed to write all bytes to tape"));
        }

        Ok(())
    }
}

fn flush(
    file: &File,
    ring_buffer: &RingBuffer,
    mut bytes_to_flush: usize,
    tape_len: u64,
    persistence: Persistence,
) -> std::io::Result<()> {
    if bytes_to_flush == 0 {
        return Ok(());
    }

    let (fist_slice, second_slice) = ring_buffer.as_slices();
    match bytes_to_flush.cmp(&second_slice.len()) {
        Ordering::Less | Ordering::Equal => {
            write_all_at(
                file,
                &second_slice[second_slice.len() - bytes_to_flush..],
                tape_len - bytes_to_flush as u64,
            )?;
        }
        Ordering::Greater => {
            let first_slice_top_needed = bytes_to_flush - second_slice.len();
            write_all_at(
                file,
                &fist_slice[fist_slice.len() - first_slice_top_needed..],
                tape_len - bytes_to_flush as u64,
            )?;
            bytes_to_flush -= first_slice_top_needed;

            write_all_at(file, second_slice, tape_len - bytes_to_flush as u64)?;
        }
    }

    match persistence {
        Persistence::Buffer => Ok(()),
        Persistence::SyncData => file.sync_data(),
        Persistence::SyncAll => file.sync_all(),
    }
}
