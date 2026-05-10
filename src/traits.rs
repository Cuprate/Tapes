use std::fs::File;
use std::io;

use crate::{BlobTape, FixedSizedTape};

pub trait TapesRead {
    fn blob_tape_len(&self, tape: &BlobTape) -> Option<u64>;

    fn read_bytes(&self, blob_tape: &BlobTape, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let tape_len = self
            .blob_tape_len(blob_tape)
            .ok_or(io::Error::other("Tape not found"))?;

        read_bytes(blob_tape, tape_len, offset, buf)
    }

    fn fixed_sized_tape_len<E: bytemuck::Pod>(&self, tape: &FixedSizedTape<E>) -> Option<u64> {
        self.blob_tape_len(&tape.inner)
            .map(|bytes| bytes / size_of::<E>() as u64)
    }
    /// Reads an entry from a fixed-sized tape.
    ///
    /// Returns `None` if read goes past the end of the tape.
    fn read_entry<E: bytemuck::Pod>(
        &self,
        fixed_sized_tape: &FixedSizedTape<E>,
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
    fn read_entries<E: bytemuck::Pod>(
        &self,
        fixed_sized_tape: &FixedSizedTape<E>,
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
    fn iter_from<'b, E: bytemuck::Pod>(
        &'b self,
        fixed_sized_tape: &'b FixedSizedTape<E>,
        from: u64,
    ) -> io::Result<crate::tapes::fixed_sized_iter::Iter<'b, E, Self>> {
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

pub trait TapesTruncate: TapesRead {
    fn truncate_blob_tape(&mut self, tape: &BlobTape, new_len: u64);

    fn truncate_fixed_sized_tape<E: bytemuck::Pod>(
        &mut self,
        tape: &FixedSizedTape<E>,
        new_len: u64,
    ) {
        self.truncate_blob_tape(&tape.inner, new_len * size_of::<E>() as u64);
    }

    fn drop_fixed_sized_tape<E: bytemuck::Pod>(
        &mut self,
        tape: &FixedSizedTape<E>,
        numb_to_drop: u64,
    ) {
        let Some(len) = self.fixed_sized_tape_len(tape) else {
            return;
        };
        let new_len = len.saturating_sub(numb_to_drop);

        self.truncate_fixed_sized_tape(tape, new_len);
    }

    fn pop_fixed_sized_tape<E: bytemuck::Pod>(
        &mut self,
        tape: &FixedSizedTape<E>,
    ) -> io::Result<Option<(u64, E)>> {
        let Some(len) = self.fixed_sized_tape_len(tape) else {
            return Ok(None);
        };

        let Some(entry) = self.read_entry(tape, len - 1)? else {
            return Ok(None);
        };
        self.truncate_fixed_sized_tape(tape, len - 1);

        Ok(Some((len - 1, entry)))
    }
}

pub trait TapesAppend: TapesRead {
    fn append_bytes(&mut self, tape: &BlobTape, buf: &[u8]) -> io::Result<u64>;
    fn append_entries<E: bytemuck::NoUninit>(
        &mut self,
        fixed_sized_tape: &FixedSizedTape<E>,
        entries: &[E],
    ) -> io::Result<u64> {
        self.append_bytes(&fixed_sized_tape.inner, bytemuck::cast_slice(entries))
            .map(|len| len / size_of::<E>() as u64)
    }
}

fn read_bytes(blob_tape: &BlobTape, tape_len: u64, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    if tape_len < offset + buf.len() as u64 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Read past end of tape",
        ));
    }

    let top_cache = blob_tape.top_cache.read();
    let cached_offset = top_cache.cache_start_idx() as u64;

    let mut last_byte_needed_offset = offset + buf.len() as u64;

    if last_byte_needed_offset > cached_offset {
        let read_start = offset.saturating_sub(cached_offset);
        let buf_to_fill = &mut buf[(cached_offset.saturating_sub(offset)) as usize..];

        top_cache.fill(read_start as usize, buf_to_fill);
        last_byte_needed_offset -= buf_to_fill.len() as u64;
    }

    if last_byte_needed_offset != offset {
        read_exact_at(
            &blob_tape.file,
            &mut buf[0..(last_byte_needed_offset - offset) as usize],
            offset,
        )?;
    }

    Ok(())
}

pub(crate) fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;

        file.read_exact_at(buf, offset)
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;

        let mut buf = buf;
        let mut offset = offset;
        while !buf.is_empty() {
            match file.seek_read(buf, offset) {
                Ok(0) => {
                    break;
                }
                Ok(n) => {
                    buf = &mut buf[n..];
                    offset += n as u64;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if !buf.is_empty() {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill the whole buffer",
            ))
        } else {
            Ok(())
        }
    }
}
