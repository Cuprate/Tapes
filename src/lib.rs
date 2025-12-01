//! # Linear Tapes
//!
//! ## Overview
//!
//! This crate implements a database system that is specialised for storing data in contiguous tapes,
//! and appending/popping data to/from them.
//!
//! [`Tapes`] supports atomically updating multiple tapes at a time with concurrent readers. Write
//! operations have been split into two separate operations: [`Popper`] and [`Appender`], this means you
//! cannot pop and append to different tapes in the same operation.
//!
//! ## Tapes
//!
//! There are two type of tapes, blob and fixed size. You can have a [`Tapes`] database made of a mix
//! of them.
//!
//! - fixed sized tapes store fixed sized values and allows direct indexing to get values.
//! - blob tapes store raw bytes, you must store the index fo the values to retrieve them.
//!
//! # Safety
//!
//! All databases are backed by a memory map, which practically can not be worked on in Rust in completely safe code.
//! So you must make sure the file is not edited in a way that is not allowed by this crate.
//!
use std::{
    cell::RefCell,
    collections::HashMap,
    io, ptr,
    sync::atomic::{AtomicBool, AtomicPtr, Ordering, fence},
};

use bytemuck::Pod;

mod blob_tape;
mod fixed_size;
mod memory;
mod metadata;
mod unsafe_tape;

pub use blob_tape::{Blob, BlobTapeAppender, BlobTapePopper, BlobTapeSlice};
pub use fixed_size::{FixedSizedTapeAppender, FixedSizedTapePopper, FixedSizedTapeSlice};
pub use memory::{BackingMemory, InMemory, MmapFile, MmapFileOpenOption};

use metadata::{APPEND_OP, Metadata, POP_OP};
use unsafe_tape::UnsafeTape;

/// The length of the RCU ring for the metadata.
/// More means slow read operations won't slow writes as much, but more slots will need to be checked
/// for no readers when it is required that all readers have seen that latest value. Also, more means
/// more copies of the metadata.
const METADATA_RING_LEN: usize = 8;

/// Advice to give the OS when opening the memory map file.
#[derive(Copy, Clone, Debug)]
pub enum Advice {
    /// [`memmap2::Advice::Normal`]
    Normal,
    /// [`memmap2::Advice::Random`]
    Random,
    /// [`memmap2::Advice::Sequential`]
    Sequential,
}

impl Advice {
    const fn to_memmap2_advice(self) -> memmap2::Advice {
        match self {
            Self::Normal => memmap2::Advice::Normal,
            Self::Random => memmap2::Advice::Random,
            Self::Sequential => memmap2::Advice::Sequential,
        }
    }
}

/// The method to use when flushing data.
#[derive(Copy, Clone)]
pub enum Flush {
    /// Function will block until data is persisted.
    Sync,
    /// The flush will be queued, this could leave the database in an invalid state if not all data
    /// is persisted.
    Async,
    /// No explicit synchronisation will be done, the OS has complete control of when data will be persisted.
    NoSync,
}

/// A database resize is needed as the underlying data has grown beyond it.
#[derive(Copy, Clone, Debug)]
pub struct ResizeNeeded;

/// A tape in the database.
#[derive(Debug)]
pub struct Tape<M: BackingMemory> {
    /// The name of the tape, must be unique.
    pub name: &'static str,
    /// Options for the underlying memory the tape is using.
    pub backing_memory_options: M::OpenOption,
    /// The advice to open on the database with.
    pub advice: Advice,
    /// The initial size of the backing memory we can write into.
    ///
    /// Will be ignored if we are opening a tape and the tape is larger.
    pub initial_memory_size: u64,
}

impl<M: BackingMemory> Clone for Tape<M> {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            backing_memory_options: self.backing_memory_options.clone(),
            advice: self.advice,
            initial_memory_size: self.initial_memory_size,
        }
    }
}

/// The Tapes database.
///
/// A tape stores data contiguously in the order it is inserted. This database allows storing data in multiple
/// different tapes with concurrent readers and a single writer. Write transactions have been split into pop
/// and append operations.
pub struct Tapes<M: BackingMemory> {
    /// The metadata of this database, tracks its state and ensure atomic updates.
    metadata: Metadata<M>,
    /// A map of a tape's name to its index in the list of tapes.
    tapes_to_index: HashMap<&'static str, usize>,

    /// A list of pointers to potentially old tape instances that have not been dropped yet as they could
    /// still have readers.
    old_tapes: Box<[AtomicPtr<UnsafeTape<M>>]>,
    /// An atomic bool for if we have any old tapes that need to be dropped.
    old_tapes_need_flush: AtomicBool,

    /// The list of current tape instances.
    tapes: Box<[AtomicPtr<UnsafeTape<M>>]>,

    /// The minimum step to resize a tape by.
    min_resize: u64,
}

impl<M: BackingMemory> Drop for Tapes<M> {
    fn drop(&mut self) {
        // Safety: this has been dropped so we know there are no readers pointing to this instance.
        unsafe { self.flush_old_tapes() };
        for tape in &self.tapes {
            // Safety: same as above.
            unsafe {
                drop(Box::from_raw(tape.load(Ordering::Acquire)));
            }
        }
    }
}

impl<M: BackingMemory> Tapes<M> {
    /// Open an [`Tapes`] database.
    ///
    /// # Safety
    ///
    /// This is marked unsafe as modifications to the underlying file can lead to UB.
    /// You must ensure across all processes that no unsafe accesses are done.
    pub unsafe fn new(
        mut tapes: Vec<Tape<M>>,
        metadata_open_options: M::OpenOption,
        min_resize: u64,
    ) -> Result<Self, io::Error> {
        tapes.sort_unstable_by_key(|tape| tape.name);
        // Safety: the requirement is on the caller to uphold the invariants.
        let metadata =
            unsafe { Metadata::open(metadata_open_options, tapes.len(), METADATA_RING_LEN)? };

        let tapes_to_index = tapes.iter().enumerate().map(|(i, t)| (t.name, i)).collect();

        let (old_tapes, tapes) = tapes
            .iter()
            .map(|tape| {
                // Safety: the requirement is on the caller to uphold the invariants.
                unsafe {
                    let tape = Box::into_raw(Box::new(UnsafeTape::open(tape.clone())?));

                    Ok::<_, io::Error>((AtomicPtr::new(ptr::null_mut()), AtomicPtr::new(tape)))
                }
            })
            .collect::<Result<(Vec<_>, Vec<_>), _>>()?;

        Ok(Self {
            tapes_to_index,
            metadata,
            old_tapes: old_tapes.into_boxed_slice(),
            old_tapes_need_flush: AtomicBool::new(false),
            min_resize,
            tapes: tapes.into_boxed_slice(),
        })
    }

    /// Flush old database tapes.
    ///
    /// # Safety
    ///
    /// It must be ensured no other thread is using these tapes.
    unsafe fn flush_old_tapes(&self) {
        for old_tape in &self.old_tapes {
            let ptr = old_tape.swap(ptr::null_mut(), Ordering::Relaxed);
            if ptr.is_null() {
                continue;
            }

            // Make sure the allocations don't get reordered to after the pointer was written.
            fence(Ordering::Acquire);
            // Safety: the above fence means we will see the allocation and its on the caller of this
            // function to ensure there are no users of this tape.
            unsafe {
                drop(Box::from_raw(ptr));
            }
        }
    }

    /// Start a new database appender.
    ///
    /// Only one write operation can be active at a given time, this will block if another write operation
    /// is active util it is finished. Also after a pop operation the next appender must wait for all readers
    /// to no longer be accessing the bytes in the range popped, so this thread will block waiting for readers
    /// to not be accessing old state if it follows a pop.
    ///
    /// Once you have finished appending data to the tapes your changes must be committed with [`Appender::flush`].
    pub fn appender(&self) -> Appender<'_, M> {
        let meta_guard = self.metadata.start_write(APPEND_OP);

        // Check if we need to drop an old tape handle.
        // We can use `Relaxed` here as this will be synchronised with the writer mutex in the meta_guard.
        if self
            .old_tapes_need_flush
            .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            // wait for al readers to be on the current data slot, so we know they are not using the
            // old tape pointer.
            self.metadata
                .wait_for_all_readers(meta_guard.current_data_slot_idx());
            // Safety: we just check above for all readers to be updated.
            unsafe { self.flush_old_tapes() };
        }

        Appender {
            meta_guard,
            tapes: self,
            min_resize: self.min_resize,
            added_bytes: vec![RefCell::new(0); self.tapes.len()],
            resized_tapes: (0..self.tapes.len()).map(|_| RefCell::new(None)).collect(),
        }
    }

    /// Start a new database popper.
    ///
    /// Only one write operation can be active at a given time, this will block if another write operation
    /// is active util it is finished.
    ///
    /// Once you have finished popping data from the tapes your changes must be committed with [`Popper::flush`]
    pub fn popper(&self) -> Popper<'_, M> {
        let meta_guard = self.metadata.start_write(POP_OP);

        if self
            .old_tapes_need_flush
            .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.metadata
                .wait_for_all_readers(meta_guard.current_data_slot_idx());
            // Safety: same as the appender.
            unsafe { self.flush_old_tapes() };
        }

        Popper {
            meta_guard,
            tapes: self,
        }
    }

    /// Start a database reader.
    ///
    /// # Errors
    ///
    /// This will error if the underlying data in any tape has outgrown our memory map, this can only
    /// happen if another instance of [`Tapes`] has extended the file. If all writers and reader
    /// go through the same [`Tapes`] this will never error.
    ///
    /// When this errors you will need to resize the tape with: (TODO: reader resizes (will require a specific function that takes the write lock.)
    pub fn reader(&self) -> Result<Reader<'_, M>, ResizeNeeded> {
        let meta_guard = self.metadata.start_read();

        let loaded_tapes = self
            .tapes
            .iter()
            .map(|tape_ptr| tape_ptr.load(Ordering::Relaxed).cast_const())
            .collect::<Vec<_>>();
        // Make sure the allocations don't get reordered to after the pointers were written.
        fence(Ordering::Acquire);

        let loaded_tapes = loaded_tapes
            .into_iter()
            .enumerate()
            .map(|(i, tape)| {
                // Safety: We are holding a reader guard so this will not be dropped.
                let tape = unsafe { &*tape };

                let bytes_used = meta_guard[i];

                if tape.map_size() < bytes_used {
                    return Err(ResizeNeeded);
                }

                Ok(tape)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Reader {
            meta_guard,
            loaded_tapes,
            tapes: self,
        })
    }

    pub fn flush_all_tapes(&self, mode: Flush) -> io::Result<()> {
        // Make sure writers see our reader so then don't close the memory map while we are using it.
        let _meta_guard = self.metadata.start_read();

        let loaded_tapes = self
            .tapes
            .iter()
            .map(|tape_ptr| tape_ptr.load(Ordering::Relaxed).cast_const())
            .collect::<Vec<_>>();
        // Make sure the allocations don't get reordered to after the pointers were written.
        fence(Ordering::Acquire);

        for tape in loaded_tapes.into_iter() {
            // Safety: We are holding a reader guard so this will not be dropped.
            let tape = unsafe { &*tape };

            tape.flush(mode)?
        }

        Ok(())
    }
}

/// An appender for a [`Tapes`] database, allows atomically pushing data to multiple tapes.
///
/// To push data to a tape you must open an appender for that tape:
/// - for fixed sized tapes: [`Appender::fixed_sized_tape_appender`]
/// - for blob tapes: [`Appender::blob_tape_appender`]
///
/// Once finished changes must be committed with: [`Appender::flush`]
pub struct Appender<'a, M: BackingMemory> {
    /// The metadata guard for this writer.
    meta_guard: metadata::MetadataWriteGuard<'a, M>,
    /// The tapes database.
    tapes: &'a Tapes<M>,
    /// The minimum step to resize a tape by.
    min_resize: u64,
    /// A vec with the same length as the amount of tapes, representing the amount of bytes that have
    /// been added to each.
    added_bytes: Vec<RefCell<usize>>,
    /// A vec with the same length as the amount of tapes, which holds new tapes handles that have been resized.
    resized_tapes: Vec<RefCell<Option<UnsafeTape<M>>>>,
}

impl<M: BackingMemory> Appender<'_, M> {
    /// Opens a handle to append to a fixed-sized tape.
    ///
    /// # Panics
    ///
    /// This will panic if you open the same tape twice without dropping the first handle first.
    /// Only 1 appender an exist for a given tape at a time.
    pub fn fixed_sized_tape_appender<'a, P: Pod>(
        &'a self,
        table_name: &'static str,
    ) -> FixedSizedTapeAppender<'a, P, M> {
        let i = *self
            .tapes
            .tapes_to_index
            .get(table_name)
            .expect("Tape was not specified when opening tapes");

        let tape = &self.tapes.tapes[i];

        FixedSizedTapeAppender {
            // Safety: We are holding a write lock and only writers will drop tapes.
            backing_file: unsafe { &*tape.load(Ordering::Acquire) },
            resized_backing_file: self.resized_tapes[i].borrow_mut(),
            min_resize: self.min_resize,
            phantom: Default::default(),
            current_used_bytes: self.meta_guard.tables_len()[i],
            bytes_added: self.added_bytes[i].borrow_mut(),
        }
    }

    /// Opens a handle to append to a blob tape.
    pub fn blob_tape_appender<'a>(&'a self, table_name: &'static str) -> BlobTapeAppender<'a, M> {
        let i = *self
            .tapes
            .tapes_to_index
            .get(table_name)
            .expect("Tape was not specified when opening tapes");

        let tape = &self.tapes.tapes[i];

        BlobTapeAppender {
            // Safety: We are holding a write lock and only writers will drop tapes.
            backing_file: unsafe { &*tape.load(Ordering::Acquire) },
            resized_backing_file: self.resized_tapes[i].borrow_mut(),
            min_resize: self.min_resize,
            current_used_bytes: self.meta_guard.tables_len()[i],
            bytes_added: self.added_bytes[i].borrow_mut(),
        }
    }

    /// Flush the changes to the database.
    ///
    /// Using a [`Flush`] mode other than [`Flush::Sync`] can leave the database in an invalid state if a crash
    /// happens before all changes are flushed to permanent storage.
    pub fn flush(mut self, mode: Flush) -> io::Result<()> {
        // First check if any resize happened.
        let mut resize_happened = false;
        for (i, resized_tape) in self.resized_tapes.iter_mut().enumerate() {
            if let Some(resized_tape) = resized_tape.take() {
                let resized_tape = Box::into_raw(Box::new(resized_tape));

                // We use `Release` here to prevent the allocation above from being reordered after this store.
                let old_tape = self.tapes.tapes[i].swap(resized_tape, Ordering::Release);
                let null = self.tapes.old_tapes[i].swap(old_tape, Ordering::Relaxed);

                // Can't happen, but make sure we don't leak any data.
                assert!(null.is_null());

                resize_happened = true;
            }
        }

        if resize_happened {
            // Tell the next write it needs to drop the old tape, this is synchronised with the write lock
            // so `Relaxed` is fine.
            self.tapes
                .old_tapes_need_flush
                .store(true, Ordering::Relaxed);
        }

        // flush each tapes changes to disk.
        // `Acquire` so we don't see the pointer before the allocation is done.
        for (i, tape) in self.tapes.tapes.iter().enumerate() {
            if *self.added_bytes[i].borrow() != 0 {
                // Safety: We are holding a write lock and only writers will drop tapes.
                unsafe { &*tape.load(Ordering::Acquire) }.flush_range(
                    self.meta_guard.tables_len_mut()[i],
                    *self.added_bytes[i].borrow(),
                    mode,
                )?;
            }
        }

        // Updated the length of each table in the metadata.
        for (len, added_bytes) in self
            .meta_guard
            .tables_len_mut()
            .iter_mut()
            .zip(&self.added_bytes)
        {
            *len += *added_bytes.borrow();
        }

        // push the update for readers to see.
        self.meta_guard.push_update(mode)
    }
}

/// A popper for a [`Tapes`] database, allows atomically popping data from multiple tapes.
///
/// To pop data from a tape you must open a popper for that tape:
/// - for fixed sized tapes: [`Popper::fixed_sized_tape_popper`]
/// - for blob tapes: [`Popper::blob_tape_popper`]
///
/// Once finished changes must be committed with: [`Popper::flush`]
pub struct Popper<'a, M: BackingMemory> {
    /// The metadata guard for this writer.
    meta_guard: metadata::MetadataWriteGuard<'a, M>,
    /// The tapes database.
    tapes: &'a Tapes<M>,
}

impl<M: BackingMemory> Popper<'_, M> {
    /// Opens a handle to pop from a fixed-sized tape.
    pub fn fixed_sized_tape_popper<'a, P: Pod>(
        &'a mut self,
        table_name: &'static str,
    ) -> FixedSizedTapePopper<'a, P, M> {
        let i = *self
            .tapes
            .tapes_to_index
            .get(table_name)
            .expect("Tape was not specified when opening tapes");

        let tape = &self.tapes.tapes[i];

        FixedSizedTapePopper {
            // Safety: We are holding a write lock and only writers will drop tapes.
            backing_file: unsafe { &*tape.load(Ordering::Acquire) },
            phantom: Default::default(),
            current_used_bytes: &mut self.meta_guard.tables_len_mut()[i],
        }
    }

    /// Opens a handle to pop from a blob tape.
    pub fn blob_tape_popper<'a>(&'a mut self, table_name: &'static str) -> BlobTapePopper<'a, M> {
        let i = *self
            .tapes
            .tapes_to_index
            .get(table_name)
            .expect("Tape was not specified when opening tapes");

        let tape = &self.tapes.tapes[i];

        BlobTapePopper {
            // Safety: We are holding a write lock and only writers will drop tapes.
            backing_file: unsafe { &*tape.load(Ordering::Acquire) },
            current_used_bytes: &mut self.meta_guard.tables_len_mut()[i],
        }
    }

    /// Opens a handle to read from a fixed-sized tape.
    pub fn fixed_sized_tape_slice<'a, P: Pod>(
        &'a self,
        table_name: &'static str,
    ) -> FixedSizedTapeSlice<'a, P> {
        let i = *self
            .tapes
            .tapes_to_index
            .get(table_name)
            .expect("Tape was not specified when opening tapes");

        let backing_file = unsafe { &*self.tapes.tapes[i].load(Ordering::Acquire) };
        let bytes = unsafe { backing_file.slice(0, self.meta_guard.tables_len()[i]) };

        FixedSizedTapeSlice {
            slice: bytemuck::cast_slice(bytes),
        }
    }

    /// Opens a handle to read from a blob tape.
    pub fn blob_tape_tape_reader<'a>(&'a self, table_name: &'static str) -> BlobTapeSlice<'a, M> {
        let i = *self
            .tapes
            .tapes_to_index
            .get(table_name)
            .expect("Tape was not specified when opening tapes");

        let backing_file = unsafe { &*self.tapes.tapes[i].load(Ordering::Acquire) };
        let bytes = unsafe { backing_file.slice(0, self.meta_guard.tables_len()[i]) };

        BlobTapeSlice {
            backing_file,
            slice: bytes,
        }
    }

    /// Flush the changes to the database.
    ///
    /// Using a [`Flush`] mode other than [`Flush::Sync`] can leave the database in an invalid state if a crash
    /// happens before all changes are flushed to permanent storage.
    pub fn flush(mut self, mode: Flush) -> io::Result<()> {
        // The poppers update the count directly + we don't need to flush anything to the files.
        self.meta_guard.push_update(mode)
    }
}

/// A [`Tapes`] database reader.
///
/// This type holds a read handle to the database so should not be kept around for longer than necessary.
pub struct Reader<'a, M: BackingMemory> {
    /// A write guard for the metadata
    meta_guard: metadata::MetadataHandle<'a>,
    /// The ptrs to the tapes, guaranteed to be safe to read from, upto the lengths in the metadata.
    loaded_tapes: Vec<&'a UnsafeTape<M>>,
    /// The tapes.
    tapes: &'a Tapes<M>,
}

unsafe impl<M: BackingMemory> Sync for Reader<'_, M> {}
unsafe impl<M: BackingMemory> Send for Reader<'_, M> {}

impl<M: BackingMemory> Reader<'_, M> {
    pub fn fixed_sized_tape_slice<P: Pod>(
        &self,
        table_name: &'static str,
    ) -> FixedSizedTapeSlice<'_, P> {
        let i = *self
            .tapes
            .tapes_to_index
            .get(table_name)
            .expect("Tape was not specified when opening tapes");

        let backing_file = self.loaded_tapes[i];
        let bytes = unsafe { backing_file.slice(0, self.meta_guard[i]) };

        FixedSizedTapeSlice {
            slice: bytemuck::cast_slice(bytes),
        }
    }

    /// Opens a handle to read from a blob tape.
    pub fn blob_tape_tape_slice<'a>(&'a self, table_name: &'static str) -> BlobTapeSlice<'a, M> {
        let i = *self
            .tapes
            .tapes_to_index
            .get(table_name)
            .expect("Tape was not specified when opening tapes");

        let backing_file = self.loaded_tapes[i];
        let bytes = unsafe { backing_file.slice(0, self.meta_guard[i]) };

        BlobTapeSlice {
            slice: bytes,
            backing_file,
        }
    }
}
