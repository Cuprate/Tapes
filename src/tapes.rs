use std::{
    cmp::min,
    collections::HashMap,
    fs::{File, OpenOptions},
    io,
    marker::PhantomData,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::RwLock;

use crate::{
    Persistence,
    metadata::{Metadata, MetadataGuard},
    ring_buffer::{RingBuffer, RingBufferFileWriter},
    traits::{TapesAppend, TapesRead, TapesTruncate, read_exact_at},
};

pub(crate) mod fixed_sized_iter;

/// Configuration options for opening a tape.
pub struct TapeOpenOptions {
    /// The size of the top cache in bytes, this amount of data from the top of the tape will be cached in memory.
    pub top_cache_size: u64,
    /// The directory to store the tapes.
    pub dir: PathBuf,
}

/// A handle to a fixed-sized tape.
///
/// Only a single handle to a tape should be opened.
pub struct FixedSizedTape<E> {
    pub(crate) inner: BlobTape,
    phantom_data: PhantomData<E>,
}

/// A handle to a blob tape.
///
/// Only a single handle to a tape should be opened.
pub struct BlobTape {
    name: &'static str,
    pub(crate) file: Arc<File>,
    pub top_cache: Arc<RwLock<RingBuffer>>,
}

/// A tapes database.
pub struct Tapes {
    metadata: Metadata,
}

impl Tapes {
    /// Open a tapes database, with metadata stored at `path`.
    pub fn open(path: &Path) -> io::Result<Self> {
        let metadata = Metadata::open(path)?;

        Ok(Self { metadata })
    }

    /// Starts an append transaction.
    pub fn append(&self) -> TapesAppendTransaction<'_> {
        TapesAppendTransaction {
            metadata: &self.metadata,
            metadata_guard: self.metadata.metadata(true),
            modified_tapes: HashMap::new(),
            committed: false,
        }
    }

    /// Starts a read transaction.
    pub fn reader(&self) -> TapesReadTransaction<'_> {
        TapesReadTransaction {
            metadata_guard: self.metadata.metadata(false),
        }
    }

    /// Starts a truncate transaction.
    pub fn truncate(&self) -> TapesTruncateTransaction<'_> {
        TapesTruncateTransaction {
            metadata: &self.metadata,
            metadata_guard: self.metadata.metadata(false),
            modified_tapes: HashMap::new(),
        }
    }
}

/// A tapes appender.
pub struct TapesAppendTransaction<'a> {
    metadata: &'a Metadata,
    metadata_guard: MetadataGuard<'a>,
    modified_tapes: HashMap<&'static str, RingBufferFileWriter>,
    committed: bool,
}

impl<'a> TapesAppendTransaction<'a> {
    /// Opens or creates a fixed-sized tape.
    pub fn open_fixed_sized_tape<E: bytemuck::NoUninit>(
        &mut self,
        name: &'static str,
        options: &TapeOpenOptions,
    ) -> io::Result<FixedSizedTape<E>> {
        let inner = self.open_blob_tape(name, options)?;

        if self
            .metadata_guard
            .get(name)
            .is_some_and(|len| !(*len as usize).is_multiple_of(size_of::<E>()))
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

    /// Opens or creates a blob tape.
    pub fn open_blob_tape(
        &mut self,
        name: &'static str,
        options: &TapeOpenOptions,
    ) -> io::Result<BlobTape> {
        match OpenOptions::new()
            .write(true)
            .read(true)
            .open(options.dir.join(name))
        {
            Ok(file) => {
                let len = *self.metadata_guard.get(name).unwrap_or(&0);

                if file.metadata()?.len() < len {
                    return Err(io::Error::other("Tape file is too small"));
                }

                let mut ring_buffer = RingBuffer::new(options.top_cache_size as usize, 0);
                let buf = ring_buffer.reset(
                    min(len, options.top_cache_size) as usize,
                    len.saturating_sub(options.top_cache_size) as usize,
                );

                read_exact_at(&file, buf, len - buf.len() as u64)?;

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
                        .truncate(true)
                        .open(options.dir.join(name))?,
                );

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

    /// Commit and consume this transaction.
    pub fn commit(mut self, persistence: Persistence) -> io::Result<()> {
        let mut new_metadata = self.metadata_guard.deref().clone();

        for (&name, tape) in &mut self.modified_tapes {
            tape.flush(persistence)?;

            new_metadata.insert(name.into(), tape.len);
        }

        self.metadata
            .update_metadata(new_metadata, false, persistence)?;
        self.committed = true;

        Ok(())
    }
}

impl Drop for TapesAppendTransaction<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }

        for (&name, tape) in &self.modified_tapes {
            let committed_len = self.metadata_guard.get(name).copied().unwrap_or(0);
            debug_assert!(tape.len >= committed_len);

            let appended = tape.len.saturating_sub(committed_len);
            tape.ring_buffer.write().pop(appended as usize);
        }
    }
}

impl TapesRead for TapesAppendTransaction<'_> {
    fn blob_tape_len(&self, tape: &BlobTape) -> Option<u64> {
        self.modified_tapes
            .get(tape.name)
            .map(|tape| tape.len)
            .or_else(|| self.metadata_guard.get(tape.name).copied())
    }
}

impl TapesAppend for TapesAppendTransaction<'_> {
    fn append_bytes(&mut self, blob_tape: &BlobTape, buf: &[u8]) -> io::Result<u64> {
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
                        ring_buffer: Arc::clone(&blob_tape.top_cache),
                        bytes_to_flush: 0,
                        file: Arc::clone(&blob_tape.file),
                        len: tape_len,
                    },
                );

                self.modified_tapes.get_mut(blob_tape.name).unwrap()
            }
        };

        tape.write(buf)
    }
}

pub struct TapesTruncateTransaction<'a> {
    metadata: &'a Metadata,
    metadata_guard: MetadataGuard<'a>,
    modified_tapes: HashMap<&'static str, TruncatedTape>,
}

struct TruncatedTape {
    new_len: u64,
    top_cache: Arc<RwLock<RingBuffer>>,
}

impl<'a> TapesTruncateTransaction<'a> {
    pub fn commit(self, persistence: Persistence) -> io::Result<()> {
        let mut new_metadata = self.metadata_guard.deref().clone();

        for (&name, tape) in &self.modified_tapes {
            new_metadata.insert(name.into(), tape.new_len);
        }

        self.metadata
            .update_metadata(new_metadata, true, persistence)?;

        for tape in self.modified_tapes.values() {
            tape.top_cache.write().truncate(tape.new_len as usize);
        }

        Ok(())
    }
}

impl TapesRead for TapesTruncateTransaction<'_> {
    fn blob_tape_len(&self, tape: &BlobTape) -> Option<u64> {
        self.modified_tapes
            .get(tape.name)
            .map(|tape| tape.new_len)
            .or_else(|| self.metadata_guard.get(tape.name).copied())
    }
}

impl TapesTruncate for TapesTruncateTransaction<'_> {
    fn truncate_blob_tape(&mut self, tape: &BlobTape, new_len: u64) {
        let old_len = self.blob_tape_len(tape).unwrap();
        assert!(old_len >= new_len);

        self.modified_tapes.insert(
            tape.name,
            TruncatedTape {
                new_len,
                top_cache: Arc::clone(&tape.top_cache),
            },
        );
    }
}

/// A tapes reader.
///
/// This will keep the view of the tapes consistent, while this is held, old data can't be overwritten,
/// so this should not be held for too long.
pub struct TapesReadTransaction<'a> {
    metadata_guard: MetadataGuard<'a>,
}

impl TapesRead for TapesReadTransaction<'_> {
    /// Returns the number of bytes in a blob tape.
    ///
    /// Returns `None` if the tape doesn't exist.
    fn blob_tape_len(&self, tape: &BlobTape) -> Option<u64> {
        self.metadata_guard.get(tape.name).copied()
    }
}
