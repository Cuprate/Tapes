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

                std::fs::create_dir_all(path.clone())?;

                let first_file = RollingTapeFile::new(
                    &path,
                    offset_to_file_index(config.start_index, config.file_size),
                )?;

                return Ok(Self {
                    name,
                    files: Arc::new(RwLock::new(VecDeque::from([first_file]))),
                    dir: path,
                    file_size: config.file_size,
                });
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
        let files = self.files.read();

        let files = files
            .iter()
            .filter(|f| {
                let file_start = file_index_to_offset(f.file_index, self.file_size);
                let file_end = file_start + self.file_size;

                file_start < offset + buf.len() as u64 && file_end > offset
            })
            .cloned()
            .collect::<Vec<_>>();

        for file in files {
            let next_file_start =
                file_index_to_offset(file.file_index, self.file_size) + self.file_size;

            let bytes_left_on_file = next_file_start - offset;

            let bytes_to_read = min(buf.len(), bytes_left_on_file.try_into().unwrap());

            read_exact_at_file(
                &file.file,
                &mut buf[..bytes_to_read],
                offset - file_index_to_offset(file.file_index, self.file_size),
            )?;

            offset += u64::try_from(bytes_to_read).unwrap();
            buf = &mut buf[bytes_to_read..];
        }

        Ok(())
    }

    fn writer(&self, len: u64) -> io::Result<Self::Writer> {
        let files = self.files.read();

        let currently_writing_idx = match files
            .iter()
            .enumerate()
            .find(|(_, tape)| file_index_to_offset(tape.file_index, self.file_size) > len)
            .map(|(i, _)| i)
            .unwrap_or(files.len())
            .checked_sub(1)
        {
            Some(i) => i,
            None => {
                unreachable!("Tape files must contain at least one tape file.");
            }
        };

        drop(files);

        Ok(RollingBlobTapeWriter {
            files: self.files.clone(),
            dir: self.dir.clone(),
            file_size: self.file_size,
            currently_writing_idx,
            first_file_touched: currently_writing_idx,
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
                .truncate(true)
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
    currently_writing_idx: usize,
    first_file_touched: usize,
    len: u64,
    is_truncation: Option<bool>,
}

impl RollingBlobTapeWriter {
    fn make_new_file(&self) -> io::Result<RollingTapeFile> {
        let next_index = self
            .files
            .read()
            .back()
            .map_or_default(|f| f.file_index + 1);

        let file = RollingTapeFile::new(&self.dir, next_index)?;

        self.files.write().push_back(file.clone());

        Ok(file)
    }
}

impl BlobTapeWriter for RollingBlobTapeWriter {
    fn write_bytes(&mut self, mut buf: &[u8]) -> io::Result<u64> {
        assert!(self.is_truncation.is_none_or(|x| !x));
        self.is_truncation = Some(false);

        let idx = self.len;

        while !buf.is_empty() {
            let files = self.files.read();
            let tape_file = match files.get(self.currently_writing_idx) {
                Some(tape_file) => {
                    let tape_file = tape_file.clone();
                    drop(files);
                    tape_file
                }
                None => {
                    drop(files);
                    self.make_new_file()?
                }
            };

            let bytes_left_on_first_tape = self.file_size
                - (self.len - file_index_to_offset(tape_file.file_index, self.file_size));

            let bytes_to_write = min(buf.len(), bytes_left_on_first_tape.try_into().unwrap());

            write_all_at(
                &tape_file.file,
                &buf[..bytes_to_write],
                self.len - file_index_to_offset(tape_file.file_index, self.file_size),
            )?;

            self.len += u64::try_from(bytes_to_write).unwrap();
            buf = &buf[bytes_to_write..];

            if !buf.is_empty() {
                self.currently_writing_idx += 1;
            }
        }

        Ok(idx)
    }

    fn flush(&mut self, persistence: Persistence) -> io::Result<()> {
        if self.is_truncation.is_some_and(|x| x) {
            let mut files = self.files.write();

            let mut file_index = files.front().map(|f| f.file_index).unwrap_or_default();

            let mut start_index = file_index_to_offset(file_index, self.file_size);

            while start_index > self.len {
                files.push_front(RollingTapeFile::new(&self.dir, file_index - 1)?);

                file_index -= 1;
                start_index -= self.file_size;
            }

            for file in files.iter_mut() {
                if file_index_to_offset(file.file_index, self.file_size) <= self.len
                    && self.len
                        < file_index_to_offset(file.file_index, self.file_size) + self.file_size
                {
                    file.out_of_range_at_epoch = None;
                }
            }

            return Ok(());
        }

        if matches!(persistence, Persistence::Buffer) {
            return Ok(());
        }

        for i in self.first_file_touched..(self.currently_writing_idx + 1) {
            let file = self.files.read().get(i).unwrap().file.clone();

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

    let mut i = 1;
    while let Some(file_2) = files.get(i) {
        if file_index_to_offset(file_2.file_index, file_size) <= metadata.start {
            files.get_mut(i - 1).unwrap().out_of_range_at_epoch = Some(current_epoch);
        }

        i += 1;
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

fn offset_to_file_index(offset: u64, file_size: u64) -> u64 {
    offset / file_size
}

fn file_index_to_offset(file_index: u64, file_size: u64) -> u64 {
    file_index * file_size
}

fn rolling_file_path(dir: &Path, file_index: u64) -> PathBuf {
    dir.join(file_index.to_string())
}
