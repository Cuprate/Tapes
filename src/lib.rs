mod metadata;
mod ring_buffer;

mod tapes;

pub use tapes::{
    BlobTape, FixedSizedTape, TapeOpenOptions, Tapes, TapesAppendTransaction,
    TapesReadTransaction,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Persistence {
    Buffer,
    SyncData,
    SyncAll,
}
