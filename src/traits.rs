use std::io;

use crate::{FixedSizedTape, Persistence, metadata::TapeMetadata};

/// A trait for a tape of bytes.
///
/// You should not use any functions or types from this trait directly. You should use the transaction
/// API.
///
/// This trait is sealed, only this crate provides implementations.
pub trait BlobTape: Sized {
    type OpenConfig: OpenConfig;

    type Writer: BlobTapeWriter + 'static;

    fn name(&self) -> &'static str;

    fn open(
        name: &'static str,
        tape_metadata: Option<TapeMetadata>,
        current_epoch: u64,
        config: Self::OpenConfig,
    ) -> io::Result<Self>;

    fn read_bytes(&self, offset: u64, buf: &mut [u8]) -> io::Result<()>;

    fn writer(&self, len: u64) -> io::Result<Self::Writer>;

    fn delete(self) -> io::Result<()>;
}

/// A trait for the configuration of a tape.
pub trait OpenConfig {
    /// The byte index to start the tape at, only used when creating a new tape.
    fn start_index(&self) -> u64;
}

/// An internal trait for a writer to a tape.
///
/// A single writer can only append _or_ truncate, you should not mix both in 1 writer instance.
pub trait BlobTapeWriter {
    fn write_bytes(&mut self, buf: &[u8]) -> io::Result<u64>;

    fn truncate(&mut self, new_len: u64);

    fn flush(&mut self, persistence: Persistence) -> io::Result<()>;

    fn revert(&mut self, _bytes: usize) {}

    fn len(&self) -> u64;

    fn remove_old_files(
        &self,
        metadata: TapeMetadata,
        current_epoch: u64,
        oldest_reader_epoch: u64,
    ) -> io::Result<()>;
}

/// A trait for reading from tapes.
pub trait TapesRead {
    /// Returns the length of a [`BlobTape`].
    ///
    /// This will not take into account the removed bytes and will be the total bytes written
    /// excluding those popped.
    fn blob_tape_len<B: BlobTape>(&self, tape: &B) -> Option<u64>;

    /// Gets the start index of a tape.
    ///
    /// This will be `0` for a tape that has not had its start index shifted.
    fn blob_tape_start<B: BlobTape>(&self, tape: &B) -> Option<u64>;

    /// Fills a mutable buffer with bytes from a tape, starting at the given `offset`.
    ///
    /// Will return an error if the read goes past the end of the tape.
    fn read_bytes<B: BlobTape>(
        &self,
        blob_tape: &B,
        offset: u64,
        buf: &mut [u8],
    ) -> io::Result<()> {
        let tape_len = self
            .blob_tape_len(blob_tape)
            .ok_or(io::Error::other("Tape not found"))?;

        let tape_start = self
            .blob_tape_start(blob_tape)
            .ok_or(io::Error::other("Tape not found"))?;

        read_bytes(blob_tape, tape_len, tape_start, offset, buf)
    }

    /// Gets the length of a fixed-sized tape in entries.
    fn fixed_sized_tape_len<B: BlobTape, E: bytemuck::Pod>(
        &self,
        tape: &FixedSizedTape<E, B>,
    ) -> Option<u64> {
        self.blob_tape_len(&tape.inner)
            .map(|bytes| bytes / size_of::<E>() as u64)
    }
    /// Reads an entry from a fixed-sized tape.
    ///
    /// Returns `None` if the read goes past the end of the tape, or before the start of the tape.
    fn read_entry<B: BlobTape, E: bytemuck::Pod>(
        &self,
        fixed_sized_tape: &FixedSizedTape<E, B>,
        index: u64,
    ) -> io::Result<Option<E>> {
        let mut entry = E::zeroed();
        let res = self.read_entries(fixed_sized_tape, index, core::slice::from_mut(&mut entry));

        if res
            .as_ref()
            .is_err_and(|e| e.kind() == io::ErrorKind::UnexpectedEof)
        {
            return Ok(None);
        }

        res?;

        Ok(Some(entry))
    }

    /// Fills a mutable buffer of entries with entries in the tape.
    ///
    /// # Errors
    ///
    /// Will return an error if the read is past the end of the tape or on any other I/O error
    /// when accessing the tape file.
    ///
    /// If there is an error, the state of the buffer is not guaranteed.
    fn read_entries<B: BlobTape, E: bytemuck::Pod>(
        &self,
        fixed_sized_tape: &FixedSizedTape<E, B>,
        offset: u64,
        buf: &mut [E],
    ) -> io::Result<()> {
        self.read_bytes(
            &fixed_sized_tape.inner,
            offset * size_of::<E>() as u64,
            bytemuck::cast_slice_mut(buf),
        )
    }

    /// Iterate from the given start value until the end of the tape.
    ///
    /// # Errors
    ///
    /// Will return an error if the start is past the end of the tape or on any other I/O error
    /// when accessing the tape file.
    fn iter_from<'b, B: BlobTape, E: bytemuck::Pod>(
        &'b self,
        fixed_sized_tape: &'b FixedSizedTape<E, B>,
        from: u64,
    ) -> io::Result<crate::tapes::fixed_sized_iter::Iter<'b, B, E, Self>> {
        let tape_len = self
            .fixed_sized_tape_len(fixed_sized_tape)
            .ok_or(io::Error::other("Tape not found"))?;

        if from > tape_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Read past end of tape",
            ));
        }

        crate::tapes::fixed_sized_iter::Iter::new(fixed_sized_tape, self, from, tape_len)
    }
}

/// A trait for truncating and popping tapes.
pub trait TapesTruncate: TapesRead {
    /// Truncates a blob tape to `new_len` bytes.
    ///
    /// Returns an error if the tape does not exist or if `new_len` is longer than the tape.
    ///
    /// If `new_len` is before the tape's start index, the tape is emptied and its start index
    /// moves to `new_len`.
    fn truncate_blob_tape<B: BlobTape>(&mut self, tape: &B, new_len: u64) -> io::Result<()>;

    /// Truncates a fixed-sized tape to `new_len` entries.
    ///
    /// Returns an error if the tape does not exist or if `new_len` is longer than the tape.
    ///
    /// If `new_len` is before the tape's start index, the tape is emptied and its start index
    /// moves to `new_len`.
    fn truncate_fixed_sized_tape<B: BlobTape, E: bytemuck::Pod>(
        &mut self,
        tape: &FixedSizedTape<E, B>,
        new_len: u64,
    ) -> io::Result<()> {
        self.truncate_blob_tape(&tape.inner, new_len * size_of::<E>() as u64)
    }

    /// Drops the last `numb_to_drop` entries from a fixed-sized tape.
    fn drop_fixed_sized_tape<B: BlobTape, E: bytemuck::Pod>(
        &mut self,
        tape: &FixedSizedTape<E, B>,
        numb_to_drop: u64,
    ) -> io::Result<()> {
        let Some(len) = self.fixed_sized_tape_len(tape) else {
            return Err(io::Error::other("Tape not found"));
        };
        let new_len = len.saturating_sub(numb_to_drop);

        self.truncate_fixed_sized_tape(tape, new_len)
    }

    /// Pops the last entry from a fixed-sized tape.
    ///
    /// Returns the index and entry of the popped entry, or `None` if the tape does not exist or is
    /// empty.
    fn pop_fixed_sized_tape<B: BlobTape, E: bytemuck::Pod>(
        &mut self,
        tape: &FixedSizedTape<E, B>,
    ) -> io::Result<Option<(u64, E)>> {
        let Some(len) = self.fixed_sized_tape_len(tape) else {
            return Ok(None);
        };

        if len == 0 {
            return Ok(None);
        }

        let Some(entry) = self.read_entry(tape, len - 1)? else {
            return Ok(None);
        };
        self.truncate_fixed_sized_tape(tape, len - 1)?;

        Ok(Some((len - 1, entry)))
    }
}

/// A trait for appending to tapes.
pub trait TapesAppend: TapesRead {
    /// Appends bytes to a tape.
    ///
    /// Returns the index at which the data was written.
    fn append_bytes<B: BlobTape>(&mut self, tape: &B, buf: &[u8]) -> io::Result<u64>;
    /// Appends entries to a fixed-sized tape.
    ///
    /// Returns the index of the first appended entry.
    fn append_entries<B: BlobTape, E: bytemuck::NoUninit>(
        &mut self,
        fixed_sized_tape: &FixedSizedTape<E, B>,
        entries: &[E],
    ) -> io::Result<u64> {
        self.append_bytes(&fixed_sized_tape.inner, bytemuck::cast_slice(entries))
            .map(|len| len / size_of::<E>() as u64)
    }

    /// Shift the start index of a tape.
    ///
    /// This will do nothing if `new_start` is less than the current start of the tape, and for a whole
    /// tape it will not free up disk space.
    fn shift_start_idx<B: BlobTape>(&mut self, tape: &B, new_start: u64) -> io::Result<()>;

    /// Shift the start index of a fixed size tape.
    ///
    /// This will do nothing if `new_start` is less than the current start of the tape, and for a whole
    /// tape it will not free up disk space.
    fn shift_start_idx_fixed<B: BlobTape, E: bytemuck::NoUninit>(
        &mut self,
        fixed_sized_tape: &FixedSizedTape<E, B>,
        new_start: u64,
    ) -> io::Result<()> {
        self.shift_start_idx(&fixed_sized_tape.inner, new_start * size_of::<E>() as u64)
    }
}

fn read_bytes<B: BlobTape>(
    blob_tape: &B,
    tape_len: u64,
    start_index: u64,
    offset: u64,
    buf: &mut [u8],
) -> io::Result<()> {
    if tape_len < offset + buf.len() as u64 || start_index > offset {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Read out of bounds",
        ));
    }

    blob_tape.read_bytes(offset, buf)
}
