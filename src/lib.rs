mod metadata;
mod ring_buffer;

mod tapes;
mod traits;

pub use tapes::{
    BlobTape, FixedSizedTape, TapeOpenOptions, Tapes, TapesAppendTransaction, TapesReadTransaction,
    TapesTruncateTransaction,
};
pub use traits::{TapesAppend, TapesRead, TapesTruncate};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Persistence {
    Buffer,
    SyncData,
    SyncAll,
}
