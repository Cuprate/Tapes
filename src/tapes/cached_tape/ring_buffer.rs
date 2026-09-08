use std::cmp::min;

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
        assert!(start + buf.len() <= self.buf.len());

        let first_slice = &self.buf[self.start_idx..];
        let second_slice = &self.buf[..self.start_idx];

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

    pub(crate) fn truncate(&mut self, tape_len: usize) {
        self.len = self.len.min(tape_len.saturating_sub(self.cache_start_idx));
    }

    pub(crate) fn prepare_write(&mut self, tape_len: usize) {
        debug_assert!(self.len == 0 || self.cache_start_idx + self.len == tape_len);

        if self.len == 0 {
            self.cache_start_idx = tape_len;
        }
    }
}
