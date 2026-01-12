use blake2::Digest;
use borsh::{BorshDeserialize, BorshSerialize};
use parking_lot::{Condvar, Mutex};
use slab::Slab;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io;
use std::io::{BufWriter, Read, Seek, Write};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

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

    pub fn update_metadata(&self, new_metadata: ActiveMetadata, wait_for_old_readers: bool) {
        let mut inner = self.inner.lock();

        if wait_for_old_readers
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

        // TODO: configure this
        inner.files.flush().unwrap();
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

    pub fn flush(&mut self) -> io::Result<()> {
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

        file.sync_data()?;

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

const EPOCH: &'static [u8] = b"epoch";
const TAPES: &'static [u8] = b"tapes";

fn serialise_metadata(epoch: u64, metadata: &ActiveMetadata) -> Vec<u8> {
    let mut hasher = blake2::Blake2b::new();

    let tapes_bytes = borsh::to_vec(&metadata).unwrap();

    hasher.update(EPOCH);
    hasher.update(&epoch.to_le_bytes());
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

pub fn try_read_metadata<R: Read>(r: &mut R) -> io::Result<StoredMetadata> {
    let metadata = StoredMetadata::deserialize_reader(r)?;

    let mut hasher = blake2::Blake2b::new();

    hasher.update(EPOCH);
    hasher.update(&metadata.epoch.to_le_bytes());
    hasher.update(TAPES);
    hasher.update(&metadata.tapes);

    let hash: [u8; 32] = hasher.finalize().into();

    if hash != metadata.hash {
        todo!()
    }

    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use crate::metadata::{ActiveMetadata, Metadata};
    use std::collections::HashMap;

    #[test]
    fn basic_test() {
        let folder = tempfile::tempdir().unwrap();

        let meta = Metadata::open(folder.path()).unwrap();

        println!("{:?}", meta.metadata().active_metadata);

        let reader = meta.metadata();

        meta.update_metadata(HashMap::from_iter([("test".into(), 10)]), true);

        println!("{:?}", meta.metadata().active_metadata);

        std::thread::scope(|s| {
            s.spawn(|| {
                meta.update_metadata(HashMap::from_iter([("test".into(), 22)]), true);

                println!("{:?}", meta.metadata().active_metadata);
            });

            s.spawn(|| {
                println!("{:?}", reader.active_metadata);
                std::thread::sleep(std::time::Duration::from_millis(1200));
                drop(reader);
            });
        });

        drop(meta);

        let meta = Metadata::open(folder.path()).unwrap();

        println!("{:?}", meta.metadata().active_metadata);
    }
}
