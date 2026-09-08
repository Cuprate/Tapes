use std::{
    fs::{File, OpenOptions},
    io,
    path::PathBuf,
    sync::Arc,
};

use crate::{
    Persistence,
    io_helpers::{read_exact_at_file, write_all_at},
    metadata::TapeMetadata,
    traits::{BlobTape, BlobTapeWriter, OpenConfig},
};

/// Open options for a [`WholeBlobTape`].
#[derive(Clone)]
pub struct WholeTapeOpenOptions {
    /// The directory to store the tapes.
    pub dir: PathBuf,
}

impl OpenConfig for WholeTapeOpenOptions {
    fn start_index(&self) -> u64 {
        0
    }
}

/// A handle to a whole blob tape.
pub struct WholeBlobTape {
    name: &'static str,
    file: Arc<File>,
    dir: PathBuf,
}

impl BlobTape for WholeBlobTape {
    type OpenConfig = WholeTapeOpenOptions;
    type Writer = WholeBlobTapeWriter;

    fn name(&self) -> &'static str {
        self.name
    }

    fn open(
        name: &'static str,
        tape_metadata: Option<TapeMetadata>,
        _: u64,
        config: Self::OpenConfig,
    ) -> io::Result<Self> {
        let dir = config.dir.join("tapes");

        match OpenOptions::new()
            .write(true)
            .read(true)
            .open(dir.join(name))
        {
            Ok(file) => {
                let metadata = tape_metadata.unwrap_or_default();

                if file.metadata()?.len() < metadata.len {
                    return Err(io::Error::other("Tape file is too small"));
                }

                let file = Arc::new(file);

                Ok(WholeBlobTape { name, file, dir })
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if tape_metadata.is_some() {
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
                        .open(dir.join(name))?,
                );

                Ok(WholeBlobTape { name, file, dir })
            }
            Err(e) => Err(e),
        }
    }

    fn read_bytes(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        read_exact_at_file(&self.file, buf, offset)
    }

    fn writer(&self, len: u64) -> io::Result<Self::Writer> {
        Ok(WholeBlobTapeWriter {
            file: self.file.clone(),
            len,
        })
    }

    fn delete(self) -> io::Result<()> {
        std::fs::remove_file(self.dir.join(self.name))
    }
}

/// A writer that writes to a [`WholeBlobTape`]
///
/// This should not be used directly.
pub struct WholeBlobTapeWriter {
    file: Arc<File>,
    len: u64,
}

impl BlobTapeWriter for WholeBlobTapeWriter {
    fn flush(&mut self, persistence: Persistence) -> io::Result<()> {
        match persistence {
            Persistence::Buffer => Ok(()),
            Persistence::SyncData => self.file.sync_data(),
            Persistence::SyncAll => self.file.sync_all(),
        }
    }

    fn write_bytes(&mut self, buf: &[u8]) -> io::Result<u64> {
        write_all_at(&self.file, buf, self.len)?;
        let idx = self.len;
        self.len += buf.len() as u64;
        Ok(idx)
    }

    fn truncate(&mut self, new_len: u64) {
        self.len = new_len;
    }

    fn len(&self) -> u64 {
        self.len
    }

    fn remove_old_files(&self, _: TapeMetadata, _: u64, _: u64) -> io::Result<()> {
        Ok(())
    }
}
