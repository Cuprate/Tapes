use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, Write},
    ops::Deref,
    path::Path,
    sync::Arc,
};

use blake2::Digest;
use borsh::{BorshDeserialize, BorshSerialize};
use parking_lot::{Condvar, Mutex};
use slab::Slab;

use crate::Persistence;

pub type ActiveMetadata = HashMap<Box<str>, u64>;

pub struct MetadataGuard<'a> {
    active_metadata: Arc<ActiveMetadata>,
    metadata: &'a Metadata,
    reader_slot: usize,
}

impl<'a> Deref for MetadataGuard<'a> {
    type Target = ActiveMetadata;

    fn deref(&self) -> &Self::Target {
        &self.active_metadata
    }
}

impl<'a> Drop for MetadataGuard<'a> {
    fn drop(&mut self) {
        let mut metadata_inner = self.metadata.inner.lock();

        metadata_inner.reader_epochs.remove(self.reader_slot);

        // If there is a writer waiting and all readers have caught up to the current epoch we can notify the writer.
        if metadata_inner.writer_waiting
            && metadata_inner
                .reader_epochs
                .iter()
                .all(|(_, epoch)| epoch == &metadata_inner.current_epoch)
        {
            self.metadata.writer_cv.notify_one();
        }
    }
}

pub struct Metadata {
    inner: Mutex<MetadataInner>,
    writer_cv: Condvar,
}

pub struct MetadataInner {
    files: MetadataBackingFiles,

    current_epoch: u64,

    active_metadata: Arc<ActiveMetadata>,

    writer_waiting: bool,
    reader_epochs: Slab<u64>,

    prev_op_was_pop: bool,
}

impl Metadata {
    pub fn open(path: &Path) -> io::Result<Self> {
        let (files, active_metadata, current_epoch) =
            MetadataBackingFiles::open(&path.join("metadata"))?;

        Ok(Self {
            inner: Mutex::new(MetadataInner {
                files,
                current_epoch,
                active_metadata: Arc::new(active_metadata),
                writer_waiting: false,
                reader_epochs: Default::default(),
                prev_op_was_pop: false,
            }),
            writer_cv: Condvar::new(),
        })
    }

    pub fn metadata(&self) -> MetadataGuard<'_> {
        let mut inner = self.inner.lock();

        let epoch = inner.current_epoch;
        let reader_slot = inner.reader_epochs.insert(epoch);

        MetadataGuard {
            active_metadata: inner.active_metadata.clone(),
            metadata: self,
            reader_slot,
        }
    }

    pub fn update_metadata(
        &self,
        new_metadata: ActiveMetadata,
        is_pop: bool,
        persistence: Persistence,
    ) {
        let mut inner = self.inner.lock();

        // If the last op was a pop and this op is an append we need to wait for all readers to catch up
        // to the current epoch so they see the pop and we don't overwrite data they are reading.
        if !is_pop
            && inner.prev_op_was_pop
            && inner
                .reader_epochs
                .iter()
                .any(|(_, epoch)| epoch < &inner.current_epoch)
        {
            inner.writer_waiting = true;
            self.writer_cv.wait_while(&mut inner, |inner| {
                inner
                    .reader_epochs
                    .iter()
                    .any(|(_, epoch)| epoch < &inner.current_epoch)
            });
            inner.writer_waiting = false;
        }

        inner.current_epoch += 1;
        let epoch = inner.current_epoch;
        inner.files.update_metadata(epoch, &new_metadata);

        inner.active_metadata = Arc::new(new_metadata);
        inner.current_epoch += 1;
        inner.prev_op_was_pop = is_pop;

        inner.files.flush(persistence).unwrap();
    }
}

struct MetadataBackingFiles {
    left: File,
    right: File,
    data_to_flush: Vec<u8>,
    next_is_left: bool,
}

impl MetadataBackingFiles {
    pub fn open(path: &Path) -> io::Result<(Self, ActiveMetadata, u64)> {
        std::fs::create_dir_all(path)?;

        let open_file = |name: &str| {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path.join(name))
        };

        let mut left = open_file("left")?;
        let mut right = open_file("right")?;

        let left_metadata = try_read_metadata(&mut left).unwrap_or_default();
        let right_metadata = try_read_metadata(&mut right).unwrap_or_default();

        let (active_metadata, next_is_left) = if left_metadata.epoch > right_metadata.epoch {
            (left_metadata, false)
        } else {
            (right_metadata, true)
        };

        Ok((
            Self {
                left,
                right,
                data_to_flush: Vec::new(),
                next_is_left,
            },
            borsh::from_slice(&active_metadata.tapes)?,
            active_metadata.epoch,
        ))
    }

    pub fn update_metadata(&mut self, epoch: u64, new_metadata: &ActiveMetadata) {
        self.data_to_flush = serialise_metadata(epoch, new_metadata);
    }

    pub fn flush(&mut self, persistence: Persistence) -> io::Result<()> {
        if self.data_to_flush.is_empty() {
            panic!("Tried to flush empty metadata");
        }

        let file = if self.next_is_left {
            &mut self.left
        } else {
            &mut self.right
        };

        file.rewind()?;
        file.write_all(&self.data_to_flush)?;

        match persistence {
            Persistence::Buffer => (),
            Persistence::SyncData => file.sync_data()?,
            Persistence::SyncAll => file.sync_all()?,
        }

        self.data_to_flush.clear();
        self.next_is_left = !self.next_is_left;

        Ok(())
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
struct StoredMetadata {
    hash: [u8; 32],
    epoch: u64,
    tapes: Vec<u8>,
}

impl Default for StoredMetadata {
    fn default() -> Self {
        StoredMetadata {
            hash: [0; 32],
            epoch: 0,
            tapes: borsh::to_vec(&HashMap::<Box<str>, u64>::new()).unwrap(),
        }
    }
}

const EPOCH: &[u8] = b"epoch";
const TAPES: &[u8] = b"tapes";

fn serialise_metadata(epoch: u64, metadata: &ActiveMetadata) -> Vec<u8> {
    let mut hasher = blake2::Blake2b::new();

    let tapes_bytes = borsh::to_vec(&metadata).unwrap();

    hasher.update(EPOCH);
    hasher.update(epoch.to_le_bytes());
    hasher.update(TAPES);
    hasher.update(&tapes_bytes);

    let hash = hasher.finalize().into();

    borsh::to_vec(&StoredMetadata {
        hash,
        epoch,
        tapes: tapes_bytes,
    })
    .unwrap()
}

fn try_read_metadata<R: Read>(r: &mut R) -> io::Result<StoredMetadata> {
    let metadata = StoredMetadata::deserialize_reader(r)?;

    let mut hasher = blake2::Blake2b::new();

    hasher.update(EPOCH);
    hasher.update(metadata.epoch.to_le_bytes());
    hasher.update(TAPES);
    hasher.update(&metadata.tapes);

    let hash: [u8; 32] = hasher.finalize().into();

    if hash != metadata.hash {
        return Err(io::Error::other("Metadata hash mismatch"));
    }

    Ok(metadata)
}
