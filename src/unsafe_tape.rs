#![expect(clippy::undocumented_unsafe_blocks)]

use std::{io, ops::Range, slice};

use crate::memory::BackingMemory;
use crate::{Flush, Tape};

#[cfg(target_endian = "big")]
const BE_NOT_SUPPORTED: u8 = panic!();

/// A raw tape database that is used to build the other tape databases.
pub(crate) struct UnsafeTape<M: BackingMemory> {
    memory: M,
    tape_info: Tape<M>,
}

impl<M: BackingMemory> UnsafeTape<M> {
    /// Open an [`UnsafeTape`].
    ///
    /// # Safety
    ///
    /// This is marked unsafe as modifications to the underlying file can lead to UB.
    pub(crate) unsafe fn open(tape_info: Tape<M>) -> io::Result<Self> {
        let memory = M::open(
            &format!("{}.tape", tape_info.name),
            tape_info.initial_memory_size,
            tape_info.backing_memory_options.clone(),
        )?;
        memory.advise(tape_info.advice)?;

        Ok(Self { memory, tape_info })
    }

    /// Take a slice of bytes from the memory map.
    ///
    /// # Safety
    ///
    /// This memory map must be initialised in this range.
    pub(crate) unsafe fn slice(&self, start: usize, len: usize) -> &[u8] {
        unsafe {
            let ptr = self.memory.ptr().add(start);
            slice::from_raw_parts(ptr, len)
        }
    }

    /// Take a range of bytes from the memory map.
    ///
    /// # Safety
    ///
    /// This memory map must be initialised in this range.
    pub(crate) unsafe fn range(&self, range: Range<usize>) -> &[u8] {
        unsafe {
            let ptr = self.memory.ptr().add(range.start);
            slice::from_raw_parts(ptr, range.len())
        }
    }

    /// Take a mutable range of bytes from the memory map.
    ///
    /// # Safety
    ///
    /// This memory map must be initialised in this range, and there must not be multiple references to the same range.
    #[expect(clippy::mut_from_ref)]
    pub(crate) unsafe fn range_mut(&self, range: Range<usize>) -> &mut [u8] {
        unsafe {
            let ptr = self.memory.mut_ptr().add(range.start);
            slice::from_raw_parts_mut(ptr, range.len())
        }
    }

    /// Flush a range of bytes in the memory map to the filesystem.
    pub(crate) fn flush_range(&self, offset: usize, len: usize, mode: Flush) -> io::Result<()> {
        self.memory.flush(offset..offset + len, mode)
    }

    /// Flush the memory map to the filesystem.
    pub(crate) fn flush(&self, mode: Flush) -> io::Result<()> {
        self.memory.flush(.., mode)
    }

    /// Returns the total map size, including the header which can't be used for values.
    pub(crate) fn map_size(&self) -> usize {
        self.memory.capacity()
    }

    /// Resizes the current database to `needed_len`.
    pub(crate) fn resize_to_bytes(&mut self, needed_len: u64) -> io::Result<()> {
        self.memory.resize(needed_len)
    }

    /// Opens a new copy of this database that has been resized to `needed_len`.
    pub(crate) fn resize_to_bytes_copy(&self, needed_len: u64) -> io::Result<Self> {
        let memory = self.memory.resize_copy(needed_len)?;
        memory.advise(self.tape_info.advice)?;

        Ok(Self {
            memory,
            tape_info: self.tape_info.clone(),
        })
    }
}
