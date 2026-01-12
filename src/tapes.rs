use std::{
    cmp::min,
    collections::HashMap,
    fs::{File, OpenOptions},
    io,
    marker::PhantomData,
    ops::Deref,
    os::unix::prelude::*,
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::RwLock;

use crate::{
    Persistence,
    metadata::{Metadata, MetadataGuard},
    ring_buffer::{RingBuffer, RingBufferFileWriter},
};

pub struct TapeOpenOptions {
    pub top_cache_size: u64,
    pub dir: PathBuf,
}

pub struct Tapes {
    metadata: Metadata,
}

impl Tapes {
    pub fn open(path: &Path) -> io::Result<Self> {
        let metadata = Metadata::open(path)?;

        Ok(Self { metadata })
    }

    pub fn appender(&self) -> TapesAppenderTransaction<'_> {
        TapesAppenderTransaction {
            metadata: &self.metadata,
            metadata_guard: self.metadata.metadata(),
            modified_tapes: HashMap::new(),
        }
    }

    pub fn reader(&self) -> TapesReadTransaction<'_> {
        TapesReadTransaction {
            metadata_guard: self.metadata.metadata(),
        }
    }
}

pub struct FixedSizedTape<E> {
    inner: BlobTape,
    phantom_data: PhantomData<E>,
}

pub struct BlobTape {
    name: &'static str,
    file: Arc<File>,
    top_cache: Arc<RwLock<RingBuffer>>,
}

pub struct TapesAppenderTransaction<'a> {
    metadata: &'a Metadata,
    metadata_guard: MetadataGuard<'a>,
    modified_tapes: HashMap<&'static str, RingBufferFileWriter>,
}

impl<'a> TapesAppenderTransaction<'a> {
    pub fn fixed_sized_tape_len<E: bytemuck::Pod>(&self, tape: &FixedSizedTape<E>) -> Option<u64> {
        self.blob_tape_len(&tape.inner)
            .map(|bytes| bytes / size_of::<E>() as u64)
    }

    pub fn blob_tape_len(&self, tape: &BlobTape) -> Option<u64> {
        self.modified_tapes
            .get(tape.name)
            .map(|tape| tape.len)
            .or_else(|| self.metadata_guard.get(tape.name).copied())
    }

    pub fn append_entry<E: bytemuck::NoUninit>(
        &mut self,
        fixed_sized_tape: &FixedSizedTape<E>,
        entries: &[E],
    ) -> io::Result<u64> {
        self.append_bytes(&fixed_sized_tape.inner, bytemuck::cast_slice(entries))
    }

    pub fn append_bytes(&mut self, blob_tape: &BlobTape, buf: &[u8]) -> io::Result<u64> {
        let tape = match self.modified_tapes.get_mut(blob_tape.name) {
            Some(tape) => tape,
            None => {
                let tape_len = *self
                    .metadata_guard
                    .get(blob_tape.name)
                    .ok_or(io::Error::other("Tape does not exist"))?;
                self.modified_tapes.insert(
                    blob_tape.name,
                    RingBufferFileWriter {
                        ring_buffer: blob_tape.top_cache.clone(),
                        bytes_to_flush: 0,
                        file: blob_tape.file.clone(),
                        len: tape_len,
                    },
                );

                self.modified_tapes.get_mut(blob_tape.name).unwrap()
            }
        };

        tape.write(buf)
    }

    pub fn open_fixed_sized_tape<E: bytemuck::NoUninit>(
        &mut self,
        name: &'static str,
        options: &TapeOpenOptions,
    ) -> io::Result<FixedSizedTape<E>> {
        let inner = self.open_blob_tape(name, options)?;

        if self
            .metadata_guard
            .get(name)
            .is_some_and(|len| *len as usize % size_of::<E>() != 0)
        {
            return Err(io::Error::other(
                "Tape size is not a multiple of entry size",
            ));
        }

        Ok(FixedSizedTape {
            inner,
            phantom_data: PhantomData,
        })
    }

    pub fn open_blob_tape(
        &mut self,
        name: &'static str,
        options: &TapeOpenOptions,
    ) -> io::Result<BlobTape> {
        match OpenOptions::new()
            .write(true)
            .read(true)
            .custom_flags(libc::POSIX_FADV_RANDOM)
            .open(options.dir.join(name))
        {
            Ok(file) => {
                let len = *self.metadata_guard.get(name).unwrap_or(&0);

                unsafe {
                    libc::posix_fadvise(
                        file.as_raw_fd(),
                        0,
                        0,
                        libc::POSIX_FADV_RANDOM | libc::POSIX_FADV_NOREUSE,
                    )
                };

                if file.metadata()?.len() < len {
                    return Err(io::Error::other("Tape file is too small"));
                }

                let mut ring_buffer = RingBuffer::new(options.top_cache_size as usize, 0);
                let buf = ring_buffer.reset(
                    min(len, options.top_cache_size) as usize,
                    len.saturating_sub(options.top_cache_size) as usize,
                );

                file.read_exact_at(buf, len - buf.len() as u64)?;

                let top_cache = Arc::new(RwLock::new(ring_buffer));
                let file = Arc::new(file);

                if self.metadata_guard.get(name).is_none() {
                    self.modified_tapes.insert(
                        name,
                        RingBufferFileWriter {
                            ring_buffer: top_cache.clone(),
                            bytes_to_flush: 0,
                            file: file.clone(),
                            len,
                        },
                    );
                }

                Ok(BlobTape {
                    name,
                    file,
                    top_cache,
                })
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if self.metadata_guard.get(name).is_some() {
                    return Err(io::Error::other(
                        "tape was in metadata but file was not found.",
                    ));
                }

                let file = Arc::new(
                    OpenOptions::new()
                        .write(true)
                        .read(true)
                        .create(true)
                        .open(options.dir.join(name))?,
                );

                unsafe {
                    libc::posix_fadvise(
                        file.as_raw_fd(),
                        0,
                        0,
                        libc::POSIX_FADV_RANDOM | libc::POSIX_FADV_NOREUSE,
                    )
                };

                let top_cache = Arc::new(RwLock::new(RingBuffer::new(
                    options.top_cache_size as usize,
                    0,
                )));

                self.modified_tapes.insert(
                    name,
                    RingBufferFileWriter {
                        ring_buffer: top_cache.clone(),
                        bytes_to_flush: 0,
                        file: file.clone(),
                        len: 0,
                    },
                );

                Ok(BlobTape {
                    name,
                    file,
                    top_cache,
                })
            }
            Err(e) => Err(e),
        }
    }

    pub fn read_entries<E: bytemuck::Pod>(
        &self,
        fixed_sized_tape: &FixedSizedTape<E>,
        offset: u64,
        entries: &mut [E],
    ) -> io::Result<()> {
        self.read_bytes(
            &fixed_sized_tape.inner,
            offset * size_of::<E>() as u64,
            bytemuck::cast_slice_mut(entries),
        )
    }

    pub fn read_bytes(&self, blob_tape: &BlobTape, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let tape_len = self
            .modified_tapes
            .get(&blob_tape.name)
            .map(|t| t.len)
            .or_else(|| self.metadata_guard.get(blob_tape.name).copied())
            .ok_or(io::Error::other("Tape not found"))?;

        read_bytes(blob_tape, tape_len, offset, buf)
    }

    pub fn commit(&mut self, persistence: Persistence) -> io::Result<()> {
        let mut new_metadata = self.metadata_guard.deref().clone();

        for (&name, tape) in &mut self.modified_tapes {
            tape.flush(persistence)?;

            new_metadata.insert(name.into(), tape.len);
        }

        self.metadata.update_metadata(new_metadata, false);

        Ok(())
    }
}

pub struct TapesTruncateTransaction<'a> {
    metadata_guard: MetadataGuard<'a>,
    modified_tapes: HashMap<&'static str, u64>,
}

impl<'a> TapesTruncateTransaction<'a> {
    pub fn drop_from_fixed_sized_tape<E>(&mut self, tape: &FixedSizedTape<E>, numb: u64) {
        let old_len = *self.metadata_guard.get(tape.inner.name).unwrap();
        assert!(old_len >= numb * size_of::<E>() as u64);

        self.modified_tapes.insert(tape.inner.name.into(), old_len - numb * size_of::<E>() as u64);
    }

    pub fn set_blob_tape_len(&mut self, tape: &BlobTape, new_len: u64) {
         let old_len = *self.metadata_guard.get(tape.name).unwrap();
        assert!(old_len >= new_len);

        self.modified_tapes.insert(tape.name.into(), new_len);
    }

    pub fn commit(&mut self, persistence: Persistence) -> io::Result<()> {
        todo!()
    }
}

pub struct TapesReadTransaction<'a> {
    metadata_guard: MetadataGuard<'a>,
}

impl<'a> TapesReadTransaction<'a> {
    pub fn fixed_sized_tape_len<E: bytemuck::Pod>(&self, tape: &FixedSizedTape<E>) -> Option<u64> {
        self.blob_tape_len(&tape.inner)
            .map(|bytes| bytes / size_of::<E>() as u64)
    }

    pub fn blob_tape_len(&self, tape: &BlobTape) -> Option<u64> {
        self.metadata_guard.get(tape.name).copied()
    }

    pub fn read_entry<E: bytemuck::Pod>(
        &self,
        fixed_sized_tape: &FixedSizedTape<E>,
        index: u64,
    ) -> io::Result<E> {
        let mut entry = E::zeroed();
        self.read_entries(fixed_sized_tape, index, core::slice::from_mut(&mut entry))?;

        Ok(entry)
    }

    pub fn read_entries<E: bytemuck::Pod>(
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

    pub fn read_bytes(&self, blob_tape: &BlobTape, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let tape_len = *self
            .metadata_guard
            .get(blob_tape.name)
            .ok_or(io::Error::other("Tape not found"))?;

        read_bytes(blob_tape, tape_len, offset, buf)
    }
}

pub fn read_bytes(
    blob_tape: &BlobTape,
    tape_len: u64,
    offset: u64,
    buf: &mut [u8],
) -> io::Result<()> {
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
        blob_tape.file.read_exact_at(
            &mut buf[0..(last_byte_needed_offset - offset) as usize],
            offset,
        )?;
    }

    Ok(())
}
