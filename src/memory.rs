use std::{
    cell::UnsafeCell,
    cmp::max,
    fmt::Debug,
    fs::{File, OpenOptions},
    io,
    ops::{Bound, RangeBounds},
    path::PathBuf,
};

use memmap2::MmapOptions;

use crate::{Advice, Flush};

/// An in-memory database.
///
/// Resizes with an in-memory database will copy the whole tape to a new allocation, which should
/// be kept in mind. If you expect resizes a memory map with a backing file is a better option: [`MmapFile`].
pub type InMemory = Vec<UnsafeCell<u8>>;

/// Backing memory that a tape can be built on top of.
///
/// # Safety
///
/// For the methods of this trait that return ptrs the ptrs must fulfill all requirements for [`core::slice::from_raw_parts`]
/// with the length being [`Self::capacity`]. This crate will handle making sure there are no concurrent mutable references
/// and non-mutable references to a given range of bytes.
pub unsafe trait BackingMemory: Sized {
    /// Options to set when creating/opening memory.
    type OpenOption: Clone + Debug;

    /// Open [`Self`] with the given name, minimum length and options.
    fn open(name: &str, min_len: u64, options: Self::OpenOption) -> io::Result<Self>;

    /// Returns a ptr to the start of the byte block.
    fn ptr(&self) -> *const u8;

    /// Returns a mutable ptr to the start of the bytes.
    fn mut_ptr(&self) -> *mut u8;

    /// Returns the capacity of the backing memory.
    fn capacity(&self) -> usize;

    /// Apply some advice to the backing memory.
    fn advise(&self, advice: Advice) -> io::Result<()>;

    /// Resize the given map to at least the given capacity.
    fn resize(&mut self, new_len: u64) -> io::Result<()>;

    /// Create a copy of the map with at least the given capacity.
    fn resize_copy(&self, new_len: u64) -> io::Result<Self>;

    /// flush this memory to storage if the backing memory supports that.
    fn flush<R: RangeBounds<usize>>(&self, range: R, mode: Flush) -> io::Result<()>;
}

/// A memory map backed by a file.
pub struct MmapFile {
    file: File,
    mmap_raw: memmap2::MmapRaw,
}

/// Open options for [`MmapFile`].
#[derive(Debug, Clone)]
pub struct MmapFileOpenOption {
    /// The directory the file will be stored under.
    pub dir: PathBuf,
}

unsafe impl BackingMemory for MmapFile {
    type OpenOption = MmapFileOpenOption;

    fn open(name: &str, min_len: u64, options: Self::OpenOption) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(options.dir.join(name))?;

        let len = file.metadata()?.len();

        if len < min_len {
            file.set_len(min_len)?;
        }

        let mmap_raw = MmapOptions::new().map_raw(&file)?;

        Ok(Self { file, mmap_raw })
    }

    fn ptr(&self) -> *const u8 {
        self.mmap_raw.as_ptr()
    }

    fn mut_ptr(&self) -> *mut u8 {
        self.mmap_raw.as_mut_ptr()
    }

    fn capacity(&self) -> usize {
        self.mmap_raw.len()
    }

    fn advise(&self, advice: Advice) -> io::Result<()> {
        self.mmap_raw.advise(advice.to_memmap2_advice())
    }

    fn resize(&mut self, new_len: u64) -> io::Result<()> {
        let current_len = self.file.metadata()?.len();
        let new_len = max(current_len, new_len);

        if new_len != current_len {
            self.file.set_len(max(current_len, new_len))?;
        }

        let mmap_raw = memmap2::MmapOptions::new().map_raw(&self.file)?;

        self.mmap_raw = mmap_raw;

        Ok(())
    }

    fn resize_copy(&self, new_len: u64) -> io::Result<Self> {
        let current_len = self.file.metadata()?.len();
        let new_len = max(current_len, new_len);

        if new_len != current_len {
            self.file.set_len(max(current_len, new_len))?;
        }

        let mmap_raw = memmap2::MmapOptions::new().map_raw(&self.file)?;

        Ok(Self {
            file: self.file.try_clone()?,
            mmap_raw,
        })
    }

    fn flush<R: RangeBounds<usize>>(&self, range: R, mode: Flush) -> io::Result<()> {
        let start = match range.start_bound() {
            Bound::Excluded(x) | Bound::Included(x) => max(*x, self.mmap_raw.len()),
            Bound::Unbounded => 0,
        };

        let len = match range.end_bound() {
            Bound::Excluded(x) | Bound::Included(x) => max(*x, self.mmap_raw.len() - start),
            Bound::Unbounded => self.mmap_raw.len() - start,
        };

        match mode {
            Flush::Sync => self.mmap_raw.flush_range(start, len),
            Flush::Async => self.mmap_raw.flush_async_range(start, len),
            Flush::NoSync => Ok(()),
        }
    }
}

unsafe impl BackingMemory for Vec<UnsafeCell<u8>> {
    type OpenOption = ();

    fn open(_: &str, min_len: u64, _: Self::OpenOption) -> io::Result<Self> {
        let vec = vec![0_u8; min_len as usize];

        let mut vec = std::mem::ManuallyDrop::new(vec);
        Ok(unsafe { Vec::from_raw_parts(vec.as_mut_ptr().cast(), vec.len(), vec.capacity()) })
    }

    fn ptr(&self) -> *const u8 {
        self.as_ptr().cast()
    }

    fn mut_ptr(&self) -> *mut u8 {
        self.as_ptr().cast_mut().cast()
    }

    fn capacity(&self) -> usize {
        self.len()
    }

    fn advise(&self, _: Advice) -> io::Result<()> {
        Ok(())
    }

    fn resize(&mut self, new_len: u64) -> io::Result<()> {
        self.reserve((new_len as usize).saturating_sub(self.len()));
        Ok(())
    }

    fn resize_copy(&self, new_len: u64) -> io::Result<Self> {
        let mut vec = vec![0_u8; new_len as usize];

        let slice = unsafe { std::slice::from_raw_parts(self.as_ptr().cast(), self.len()) };
        vec[0..self.len()].copy_from_slice(slice);

        let mut vec = std::mem::ManuallyDrop::new(vec);
        Ok(unsafe { Vec::from_raw_parts(vec.as_mut_ptr().cast(), vec.len(), vec.capacity()) })
    }

    fn flush<R: RangeBounds<usize>>(&self, _: R, _: Flush) -> io::Result<()> {
        Ok(())
    }
}
