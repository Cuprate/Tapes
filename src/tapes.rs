use std::{
    cmp::{max, min},
    collections::HashMap,
    io,
    marker::PhantomData,
    ops::Deref,
    path::Path,
    sync::Arc,
};

use crate::{
    Persistence,
    metadata::{Metadata, MetadataGuard, TapeMetadata},
    traits::{BlobTape, BlobTapeWriter, OpenConfig, TapesAppend, TapesRead, TapesTruncate},
};

mod cached_tape;
pub(crate) mod fixed_sized_iter;
mod rolling_tape;
mod whole_tape;

pub use cached_tape::{CachedBlobTape, CachedTapeOpenOptions};

pub use rolling_tape::{RollingBlobTape, RollingTapeOpenOptions};
pub use whole_tape::{WholeBlobTape, WholeTapeOpenOptions};

/// A handle to a fixed-sized tape.
///
/// Only a single handle to a tape should be opened.
pub struct FixedSizedTape<E, B: BlobTape> {
    pub(crate) inner: B,
    phantom_data: PhantomData<E>,
}

/// A tapes database.
pub struct Tapes {
    metadata: Arc<Metadata>,
}

impl Tapes {
    /// Open a tapes database, with metadata stored at `path`.
    pub fn open(path: &Path) -> io::Result<Self> {
        let metadata = Metadata::open(&path.join("tapes"))?;

        Ok(Self {
            metadata: Arc::new(metadata),
        })
    }

    /// Starts an append transaction.
    pub fn append(&self) -> TapesAppendTransaction {
        TapesAppendTransaction {
            metadata: Arc::clone(&self.metadata),
            metadata_guard: self.metadata.metadata(true),
            modified_tapes: HashMap::new(),
            committed: false,
        }
    }

    /// Starts a read transaction.
    pub fn reader(&self) -> TapesReadTransaction {
        TapesReadTransaction {
            metadata_guard: self.metadata.metadata(false),
        }
    }

    /// Starts a truncate transaction.
    pub fn truncate(&self) -> TapesTruncateTransaction {
        TapesTruncateTransaction {
            metadata: Arc::clone(&self.metadata),
            metadata_guard: self.metadata.metadata(false),
            modified_tapes: HashMap::new(),
        }
    }

    /// Deletes a tape.
    ///
    /// No other transactions or [`Tapes`] instances may be active, otherwise this returns an error.
    pub fn delete_tape<B: BlobTape>(&self, tape: B) -> io::Result<()> {
        if Arc::strong_count(&self.metadata) != 1 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "cannot delete a tape while a transaction is active",
            ));
        }

        let metadata_guard = self.metadata.metadata(false);

        if metadata_guard.contains_key(tape.name()) {
            let mut new_metadata = metadata_guard.clone();
            new_metadata.remove(tape.name());
            self.metadata
                .update_metadata(new_metadata, true, Persistence::SyncAll)?;
        }

        drop(metadata_guard);

        tape.delete()
    }
}

/// A tapes appender.
pub struct TapesAppendTransaction {
    metadata: Arc<Metadata>,
    metadata_guard: MetadataGuard,
    modified_tapes: HashMap<&'static str, (Box<dyn BlobTapeWriter>, u64)>,
    committed: bool,
}

impl TapesAppendTransaction {
    /// Checks if a tape exists.
    pub fn tape_exists(&self, name: &'static str) -> bool {
        self.metadata_guard.contains_key(name) || self.modified_tapes.contains_key(name)
    }

    /// Opens or creates a fixed-sized tape.
    pub fn open_fixed_sized_tape<E: bytemuck::NoUninit, B: BlobTape>(
        &mut self,
        name: &'static str,
        options: B::OpenConfig,
    ) -> io::Result<FixedSizedTape<E, B>> {
        let inner = self.open_blob_tape(name, options)?;

        if self
            .metadata_guard
            .get(name)
            .is_some_and(|metadata| !(metadata.len as usize).is_multiple_of(size_of::<E>()))
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
    pub fn open_blob_tape<B: BlobTape>(
        &mut self,
        name: &'static str,
        options: B::OpenConfig,
    ) -> io::Result<B> {
        let metadata = self.metadata_guard.get(name).copied();
        let start_index = metadata.map_or(options.start_index(), |m| m.start);
        let len = metadata.map_or(start_index, |m| m.len);

        let tape = B::open(name, metadata, self.metadata_guard.epoch, options)?;
        let w = tape.writer(len)?;

        self.modified_tapes.insert(name, (Box::new(w), start_index));

        Ok(tape)
    }

    /// Commit and consume this transaction.
    pub fn commit(mut self, persistence: Persistence) -> io::Result<()> {
        let mut new_metadata = self.metadata_guard.deref().clone();

        for (&name, (tape, start_index)) in &mut self.modified_tapes {
            tape.flush(persistence)?;

            let metadata = TapeMetadata {
                len: tape.len(),
                start: *start_index,
            };

            new_metadata.insert(name.into(), metadata);
        }

        self.metadata
            .update_metadata(new_metadata.clone(), false, persistence)?;
        self.committed = true;

        let oldest_reader = self
            .metadata
            .oldest_reader_excluding_reader(&self.metadata_guard)
            .unwrap_or(self.metadata_guard.epoch + 1);
        for (&name, (tape, _)) in &mut self.modified_tapes {
            tape.remove_old_files(
                *new_metadata.get(name).unwrap(),
                self.metadata_guard.epoch,
                oldest_reader,
            )?;
        }

        Ok(())
    }
}

impl Drop for TapesAppendTransaction {
    fn drop(&mut self) {
        if self.committed {
            return;
        }

        for (&name, (tape, _)) in &mut self.modified_tapes {
            let committed_len = self
                .metadata_guard
                .get(name)
                .copied()
                .unwrap_or_default()
                .len;
            debug_assert!(tape.len() >= committed_len);

            let appended = tape.len().saturating_sub(committed_len);
            tape.revert(appended as usize);
        }
    }
}

impl TapesRead for TapesAppendTransaction {
    fn blob_tape_len<B: BlobTape>(&self, tape: &B) -> Option<u64> {
        self.modified_tapes
            .get(tape.name())
            .map(|tape| tape.0.len())
            .or_else(|| {
                self.metadata_guard
                    .get(tape.name())
                    .map(|metadata| metadata.len)
            })
    }

    fn blob_tape_start<B: BlobTape>(&self, tape: &B) -> Option<u64> {
        self.modified_tapes
            .get(tape.name())
            .map(|tape| tape.1)
            .or_else(|| {
                self.metadata_guard
                    .get(tape.name())
                    .map(|metadata| metadata.start)
            })
    }
}

impl TapesAppend for TapesAppendTransaction {
    fn append_bytes<B: BlobTape>(&mut self, blob_tape: &B, buf: &[u8]) -> io::Result<u64> {
        let tape = match self.modified_tapes.get_mut(blob_tape.name()) {
            Some(tape) => tape,
            None => {
                let metadata = self
                    .metadata_guard
                    .get(blob_tape.name())
                    .ok_or(io::Error::other("Tape does not exist"))?;

                let w = blob_tape.writer(metadata.len)?;

                self.modified_tapes
                    .insert(blob_tape.name(), (Box::new(w), metadata.start));

                self.modified_tapes.get_mut(blob_tape.name()).unwrap()
            }
        };

        tape.0.write_bytes(buf)
    }

    fn shift_start_idx<B: BlobTape>(&mut self, blob_tape: &B, new_start: u64) -> io::Result<()> {
        let tape = match self.modified_tapes.get_mut(blob_tape.name()) {
            Some(tape) => tape,
            None => {
                let metadata = self
                    .metadata_guard
                    .get(blob_tape.name())
                    .ok_or(io::Error::other("Tape does not exist"))?;

                let w = blob_tape.writer(metadata.len)?;

                self.modified_tapes
                    .insert(blob_tape.name(), (Box::new(w), metadata.start));

                self.modified_tapes.get_mut(blob_tape.name()).unwrap()
            }
        };

        if tape.0.len() < new_start {
            return Err(io::Error::other(
                "Start index cannot be set past the length of the tape",
            ));
        }

        tape.1 = max(new_start, tape.1);

        Ok(())
    }
}

/// A tapes truncator.
pub struct TapesTruncateTransaction {
    metadata: Arc<Metadata>,
    metadata_guard: MetadataGuard,
    modified_tapes: HashMap<&'static str, Box<dyn BlobTapeWriter>>,
}

impl TapesTruncateTransaction {
    /// Commit and consume this transaction.
    pub fn commit(mut self, persistence: Persistence) -> io::Result<()> {
        let mut new_metadata = self.metadata_guard.deref().clone();

        for (&name, tape) in &self.modified_tapes {
            let start = min(
                new_metadata.get(name).map(|m| m.start).unwrap_or_default(),
                tape.len(),
            );

            new_metadata.insert(
                name.into(),
                TapeMetadata {
                    len: tape.len(),
                    start,
                },
            );
        }

        for tape in self.modified_tapes.values_mut() {
            tape.flush(persistence)?;
        }

        self.metadata
            .update_metadata(new_metadata, true, persistence)?;

        Ok(())
    }
}

impl TapesRead for TapesTruncateTransaction {
    fn blob_tape_len<B: BlobTape>(&self, tape: &B) -> Option<u64> {
        self.modified_tapes
            .get(tape.name())
            .map(|tape| tape.len())
            .or_else(|| {
                self.metadata_guard
                    .get(tape.name())
                    .map(|metadata| metadata.len)
            })
    }

    fn blob_tape_start<B: BlobTape>(&self, tape: &B) -> Option<u64> {
        self.metadata_guard
            .get(tape.name())
            .map(|metadata| metadata.start)
    }
}

impl TapesTruncate for TapesTruncateTransaction {
    fn truncate_blob_tape<B: BlobTape>(&mut self, tape: &B, new_len: u64) -> io::Result<()> {
        let Some(old_len) = self.blob_tape_len(tape) else {
            return Err(io::Error::other("Tape does not exist"));
        };

        if old_len < new_len {
            return Err(io::Error::other(
                "Cannot truncate a tape to a longer length",
            ));
        }

        let tape = match self.modified_tapes.get_mut(tape.name()) {
            Some(tape) => tape,
            None => {
                let tape_len = self
                    .metadata_guard
                    .get(tape.name())
                    .ok_or(io::Error::other("Tape does not exist"))?
                    .len;
                self.modified_tapes
                    .insert(tape.name(), Box::new(tape.writer(tape_len)?));

                self.modified_tapes.get_mut(tape.name()).unwrap()
            }
        };

        tape.truncate(new_len);

        Ok(())
    }
}

/// A tapes reader.
///
/// This will keep the view of the tapes consistent, while this is held, old data can't be overwritten,
/// so this should not be held for too long.
pub struct TapesReadTransaction {
    metadata_guard: MetadataGuard,
}

impl TapesRead for TapesReadTransaction {
    /// Returns the number of bytes in a blob tape.
    ///
    /// Returns `None` if the tape doesn't exist.
    fn blob_tape_len<B: BlobTape>(&self, tape: &B) -> Option<u64> {
        self.metadata_guard
            .get(tape.name())
            .map(|metadata| metadata.len)
    }

    fn blob_tape_start<B: BlobTape>(&self, tape: &B) -> Option<u64> {
        self.metadata_guard
            .get(tape.name())
            .map(|metadata| metadata.start)
    }
}
