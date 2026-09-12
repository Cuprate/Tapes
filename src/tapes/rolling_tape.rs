use std::{
    cmp::min,
    collections::VecDeque,
    fs::File,
    fs::OpenOptions,
    io,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use parking_lot::RwLock;

use crate::{
    Persistence,
    io_helpers::{read_exact_at_file, write_all_at},
    metadata::TapeMetadata,
    traits::{BlobTape, BlobTapeWriter, OpenConfig},
};

/// Open options for a [`RollingBlobTape`].
#[derive(Clone)]
pub struct RollingTapeOpenOptions {
    /// The max size of each file in the rolling tape.
    ///
    /// Bigger size means less likely to need to read across multiple files when reading data, but also
    /// means a longer time for old data to be deleted.
    ///
    /// You should keep in mind that if this is set too low and keep lots of data in the tape then lots of
    /// files will be created.
    pub file_size: u64,
    /// The directory to store the rolling tapes.
    pub dir: PathBuf,
    /// The byte index to start the rolling tapes at, only used when creating a new tape.
    ///
    /// This can be used to start the tape indexing at a specific index when creating a new tape.
    pub start_index: u64,
}

impl OpenConfig for RollingTapeOpenOptions {
    fn start_index(&self) -> u64 {
        self.start_index
    }
}

/// A rolling tape, allows removing data FIFO from the tape.
///
/// This should only be used when you need to remove data FIFO, if not use a regular tape.
pub struct RollingBlobTape {
    name: &'static str,
    files: Arc<RwLock<VecDeque<RollingTapeFile>>>,
    dir: PathBuf,
    file_size: u64,
}

impl BlobTape for RollingBlobTape {
    type OpenConfig = RollingTapeOpenOptions;
    type Writer = RollingBlobTapeWriter;

    fn name(&self) -> &'static str {
        self.name
    }

    fn open(
        name: &'static str,
        tape_metadata: Option<TapeMetadata>,
        current_epoch: u64,
        config: Self::OpenConfig,
    ) -> io::Result<Self> {
        if config.file_size == 0 {
            return Err(io::Error::other("file_size must not be 0."));
        }

        if name == "metadata" {
            return Err(io::Error::other("The tape name `metadata` is reserved."));
        }

        let path = config.dir.join("tapes").join(name);

        if tape_metadata.is_none() {
            match std::fs::remove_dir_all(&path) {
                Ok(_) => (),
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Err(e) => return Err(e),
            }
        }

        let mut files = Vec::new();

        match std::fs::read_dir(&path) {
            Ok(dir) => {
                for entry in dir {
                    let entry = entry?;
                    let Some(Ok(index)) = entry
                        .path()
                        .file_name()
                        .and_then(|i| i.to_str())
                        .map(u64::from_str)
                    else {
                        return Err(io::Error::other("File in rolling tapes has invalid name"));
                    };

                    if tape_metadata
                        .is_some_and(|m| index > offset_to_file_index(m.len, config.file_size))
                    {
                        continue;
                    }

                    let rolling_tape_file = RollingTapeFile::open(&path, index)?;

                    files.push(rolling_tape_file);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if tape_metadata.is_some() {
                    return Err(io::Error::other(
                        "tape was in metadata but files was not found.",
                    ));
                }

                std::fs::create_dir_all(&path)?;
            }
            Err(e) => return Err(e),
        }

        files.sort_by_key(|f| f.file_index);

        let files = files.into_iter().collect();

        let this = Self {
            name,
            files: Arc::new(RwLock::new(files)),
            dir: path,
            file_size: config.file_size,
        };

        if let Some(tape_metadata) = tape_metadata {
            remove_old_files(
                &this.files,
                &this.dir,
                tape_metadata,
                current_epoch,
                u64::MAX,
                this.file_size,
            )?;
        }

        Ok(this)
    }

    fn read_bytes(&self, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let file_index = offset_to_file_index(offset, self.file_size);

            let file = file_at(&self.files.read(), file_index)
                .ok_or_else(|| io::Error::other("Tape file not found"))?;

            let offset_in_file = offset - file_index_to_offset(file_index, self.file_size);
            let bytes_left_on_file = self.file_size - offset_in_file;

            let bytes_to_read = min(buf.len(), bytes_left_on_file.try_into().unwrap());

            read_exact_at_file(&file, &mut buf[..bytes_to_read], offset_in_file)?;

            offset += u64::try_from(bytes_to_read).unwrap();
            buf = &mut buf[bytes_to_read..];
        }

        Ok(())
    }

    fn writer(&self, len: u64) -> io::Result<Self::Writer> {
        let current_file_idx = offset_to_file_index(len, self.file_size);
        let first_file_touched = {
            let files = self.files.read();
            let slot = slot_for(&files, current_file_idx);
            let current_exists = files
                .get(slot)
                .is_some_and(|f| f.file_index == current_file_idx);

            if !current_exists && let Some(prev) = slot.checked_sub(1) {
                // The previous file could have unsynced writes.
                files[prev].file_index
            } else {
                current_file_idx
            }
        };

        Ok(RollingBlobTapeWriter {
            files: self.files.clone(),
            dir: self.dir.clone(),
            file_size: self.file_size,
            current_file_idx,
            first_file_touched,
            current_file: None,
            len,
            is_truncation: None,
        })
    }

    fn delete(self) -> io::Result<()> {
        std::fs::remove_dir_all(self.dir)
    }
}

#[derive(Clone)]
struct RollingTapeFile {
    file: Arc<File>,
    out_of_range_at_epoch: Option<u64>,
    file_index: u64,
}

impl RollingTapeFile {
    pub fn new(dir: &Path, file_index: u64) -> io::Result<Self> {
        let file_path = rolling_file_path(dir, file_index);

        let file = Arc::new(
            OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .truncate(false)
                .open(&file_path)?,
        );

        Ok(Self {
            file,
            out_of_range_at_epoch: None,
            file_index,
        })
    }

    pub fn open(dir: &Path, file_index: u64) -> io::Result<Self> {
        let path = rolling_file_path(dir, file_index);
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        Ok(Self {
            file: Arc::new(file),
            out_of_range_at_epoch: None,
            file_index,
        })
    }
}

/// A writer that writes to a [`RollingBlobTape`].
///
/// This should not be used directly.
pub struct RollingBlobTapeWriter {
    files: Arc<RwLock<VecDeque<RollingTapeFile>>>,
    dir: PathBuf,
    file_size: u64,
    current_file_idx: u64,
    first_file_touched: u64,
    current_file: Option<Arc<File>>,
    len: u64,
    is_truncation: Option<bool>,
}

impl RollingBlobTapeWriter {
    fn make_new_file(&self, file_index: u64) -> io::Result<RollingTapeFile> {
        let file = RollingTapeFile::new(&self.dir, file_index)?;

        let mut files = self.files.write();
        let slot = slot_for(&files, file_index);
        debug_assert!(
            files.get(slot).is_none_or(|f| f.file_index != file_index),
            "rolling tape file {file_index} already exists"
        );
        // We can clean up files here that have fallen off from the top due to a truncation here.
        files.truncate(slot);
        files.push_back(file.clone());

        Ok(file)
    }
}

impl BlobTapeWriter for RollingBlobTapeWriter {
    fn write_bytes(&mut self, mut buf: &[u8]) -> io::Result<u64> {
        assert!(self.is_truncation.is_none_or(|x| !x));
        self.is_truncation = Some(false);

        let idx = self.len;

        while !buf.is_empty() {
            let file_index = offset_to_file_index(self.len, self.file_size);

            if self.current_file.is_none() || file_index != self.current_file_idx {
                let file = file_at(&self.files.read(), file_index);
                self.current_file = Some(match file {
                    Some(file) => file,
                    None => self.make_new_file(file_index)?.file,
                });
                self.current_file_idx = file_index;
            }

            let offset_in_file = self.len - file_index_to_offset(file_index, self.file_size);
            let bytes_left_on_first_tape = self.file_size - offset_in_file;

            let bytes_to_write = min(buf.len(), bytes_left_on_first_tape.try_into().unwrap());

            write_all_at(
                self.current_file.as_ref().unwrap(),
                &buf[..bytes_to_write],
                offset_in_file,
            )?;

            self.len += u64::try_from(bytes_to_write).unwrap();
            buf = &buf[bytes_to_write..];
        }

        Ok(idx)
    }

    fn flush(&mut self, persistence: Persistence) -> io::Result<()> {
        if self.is_truncation.is_some_and(|x| x) {
            let mut files = self.files.write();
            let first = slot_for(&files, offset_to_file_index(self.len, self.file_size));

            for file in files
                .range_mut(first..)
                .take_while(|file| file.out_of_range_at_epoch.is_some())
            {
                file.out_of_range_at_epoch = None;
            }

            return Ok(());
        }

        if matches!(persistence, Persistence::Buffer) {
            return Ok(());
        }

        for file_index in self.first_file_touched..=self.current_file_idx {
            let Some(file) = file_at(&self.files.read(), file_index) else {
                continue;
            };

            match persistence {
                Persistence::Buffer => (),
                Persistence::SyncData => file.sync_data()?,
                Persistence::SyncAll => file.sync_all()?,
            }
        }

        Ok(())
    }

    fn truncate(&mut self, new_len: u64) {
        assert!(self.is_truncation.is_none_or(|x| x));
        self.is_truncation = Some(true);

        self.len = new_len;
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
        remove_old_files(
            &self.files,
            &self.dir,
            metadata,
            current_epoch,
            oldest_reader_epoch,
            self.file_size,
        )
    }
}

fn remove_old_files(
    files: &RwLock<VecDeque<RollingTapeFile>>,
    dir: &Path,
    metadata: TapeMetadata,
    current_epoch: u64,
    oldest_reader_epoch: u64,
    file_size: u64,
) -> io::Result<()> {
    let mut files = files.write();

    let end = slot_for(&files, offset_to_file_index(metadata.start, file_size));

    for file in files
        .range_mut(..end)
        .rev()
        .take_while(|file| file.out_of_range_at_epoch.is_none())
    {
        file.out_of_range_at_epoch = Some(current_epoch);
    }

    while let Some(file) = files.pop_front_if(|file| {
        file.out_of_range_at_epoch
            .is_some_and(|e| e < oldest_reader_epoch)
    }) {
        let path = rolling_file_path(dir, file.file_index);
        std::fs::remove_file(&path)?;
    }

    Ok(())
}

fn slot_for(files: &VecDeque<RollingTapeFile>, file_index: u64) -> usize {
    let (Some(first), Some(last)) = (files.front(), files.back()) else {
        return 0;
    };

    if file_index <= first.file_index {
        return 0;
    }

    if file_index > last.file_index {
        return files.len();
    }

    if let Ok(slot) = usize::try_from(file_index - first.file_index)
        && files.get(slot).is_some_and(|f| f.file_index == file_index)
    {
        return slot;
    }

    files.partition_point(|f| f.file_index < file_index)
}

fn file_at(files: &VecDeque<RollingTapeFile>, file_index: u64) -> Option<Arc<File>> {
    files
        .get(slot_for(files, file_index))
        .filter(|file| file.file_index == file_index)
        .map(|file| Arc::clone(&file.file))
}

fn offset_to_file_index(offset: u64, file_size: u64) -> u64 {
    offset / file_size
}

fn file_index_to_offset(file_index: u64, file_size: u64) -> u64 {
    file_index * file_size
}

fn rolling_file_path(dir: &Path, file_index: u64) -> PathBuf {
    dir.join(file_index.to_string())
}
