#![doc = include_str!("../README.md")]

mod metadata;

mod io_helpers;
mod tapes;
mod traits;

pub use tapes::{
    CachedBlobTape, CachedTapeOpenOptions, FixedSizedTape, RollingBlobTape, RollingTapeOpenOptions,
    Tapes, TapesAppendTransaction, TapesReadTransaction, TapesTruncateTransaction, WholeBlobTape,
    WholeTapeOpenOptions,
};
pub use traits::{BlobTape, TapesAppend, TapesRead, TapesTruncate};

/// How a commit is persisted.
///
/// `Buffer` is not durable, data committed with it can be lost on a crash until it is flushed to disk
/// by a later commit with `SyncData` or `SyncAll`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Persistence {
    /// Writes to the OS buffer only, not durable.
    Buffer,
    /// Syncs the file contents to disk.
    SyncData,
    /// Syncs the file contents and file metadata to disk.
    SyncAll,
}
