use std::io;

use tapes::{
    CachedBlobTape, CachedTapeOpenOptions, FixedSizedTape, Persistence, RollingBlobTape,
    RollingTapeOpenOptions, Tapes, TapesAppend, TapesRead, WholeBlobTape, WholeTapeOpenOptions,
};

/// Writes `len` bytes of a deterministic pattern.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| b'a' + (i % 26) as u8).collect()
}

fn whole_options(dir: &tempfile::TempDir) -> WholeTapeOpenOptions {
    WholeTapeOpenOptions {
        dir: dir.path().to_path_buf(),
    }
}

/// Two blob tapes in one database must be independent.
#[test]
fn multiple_blob_tapes_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = whole_options(&dir);

    let (a, b) = {
        let mut append = tapes.append();
        let a = append
            .open_blob_tape::<WholeBlobTape>("tape_a", options.clone())
            .unwrap();
        let b = append
            .open_blob_tape::<WholeBlobTape>("tape_b", options.clone())
            .unwrap();
        append.append_bytes(&a, b"hello").unwrap();
        append.append_bytes(&b, b"world!!").unwrap();
        append.commit(Persistence::Buffer).unwrap();
        (a, b)
    };

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&a), Some(5));
    assert_eq!(reader.blob_tape_len(&b), Some(7));

    let mut a_data = [0u8; 5];
    reader.read_bytes(&a, 0, &mut a_data).unwrap();
    assert_eq!(&a_data, b"hello");

    let mut b_data = [0u8; 7];
    reader.read_bytes(&b, 0, &mut b_data).unwrap();
    assert_eq!(&b_data, b"world!!");

    // Appending to one must not affect the other.
    {
        let mut append = tapes.append();
        append.append_bytes(&a, b"!").unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&a), Some(6));
    assert_eq!(reader.blob_tape_len(&b), Some(7));
}

/// Two fixed-sized tapes in one database must be independent.
#[test]
fn multiple_fixed_tapes_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = whole_options(&dir);

    let (a, b) = {
        let mut append = tapes.append();
        let a = append
            .open_fixed_sized_tape::<u64, WholeBlobTape>("tape_a", options.clone())
            .unwrap();
        let b = append
            .open_fixed_sized_tape::<u64, WholeBlobTape>("tape_b", options.clone())
            .unwrap();
        append.append_entries(&a, &[1, 2, 3]).unwrap();
        append.append_entries(&b, &[10, 20]).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        (a, b)
    };

    let reader = tapes.reader();
    assert_eq!(reader.fixed_sized_tape_len(&a), Some(3));
    assert_eq!(reader.fixed_sized_tape_len(&b), Some(2));

    let mut a_buf = [0u64; 3];
    reader.read_entries(&a, 0, &mut a_buf).unwrap();
    assert_eq!(a_buf, [1, 2, 3]);

    let mut b_buf = [0u64; 2];
    reader.read_entries(&b, 0, &mut b_buf).unwrap();
    assert_eq!(b_buf, [10, 20]);
}

/// `shift_start_idx_fixed` rolls by entry index (multiplies by `size_of::<E>()`).
#[test]
fn fixed_shift_start_by_entry_index() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = RollingTapeOpenOptions {
        file_size: 8,
        dir: dir.path().to_path_buf(),
        start_index: 0,
    };
    let entries: Vec<u64> = vec![10, 20, 30, 40, 50, 60];

    let tape: FixedSizedTape<u64, RollingBlobTape> = {
        let mut append = tapes.append();
        let tape = append
            .open_fixed_sized_tape::<u64, RollingBlobTape>("tape", options.clone())
            .unwrap();
        append.append_entries(&tape, &entries).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    // A separate blob handle (same tape) for the start check.
    let blob: RollingBlobTape = {
        let mut append = tapes.append();
        append
            .open_blob_tape::<RollingBlobTape>("tape", options)
            .unwrap()
    };

    // Roll to entry index 2 (byte offset 16), popping entries 0,1.
    {
        let mut append = tapes.append();
        append.shift_start_idx_fixed(&tape, 2).unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_start(&blob), Some(16));

    // Live entries are indices 2..6.
    let mut buf = [0u64; 4];
    reader.read_entries(&tape, 2, &mut buf).unwrap();
    assert_eq!(buf, [30, 40, 50, 60]);

    // Reading a popped entry fails cleanly.
    let mut err = [0u64; 1];
    let e = reader.read_entries(&tape, 0, &mut err).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
}

/// Rolling to a start offset that is not a multiple of `file_size` must still
/// read the live region byte-precisely.
#[test]
fn rolling_non_aligned_start() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = RollingTapeOpenOptions {
        file_size: 8,
        dir: dir.path().to_path_buf(),
        start_index: 0,
    };
    let data = pattern(40);

    let tape = {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<RollingBlobTape>("tape", options.clone())
            .unwrap();
        append.append_bytes(&tape, &data).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    // Roll to start = 10 (not a multiple of file_size=8).
    {
        let mut append = tapes.append();
        append.shift_start_idx(&tape, 10).unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_start(&tape), Some(10));
    assert_eq!(reader.blob_tape_len(&tape), Some(40));

    // Live region is [10, 40).
    let mut contents = [0; 30];
    reader.read_bytes(&tape, 10, &mut contents).unwrap();
    assert_eq!(&contents, &data[10..40]);

    // Reading before the start fails cleanly.
    let mut err_buf = [0; 4];
    let err = reader.read_bytes(&tape, 0, &mut err_buf).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
}

/// A `CachedBlobTape` wrapping a `RollingBlobTape` must write and read correctly.
#[test]
fn cached_rolling_combo() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = CachedTapeOpenOptions::<RollingBlobTape> {
        inner: RollingTapeOpenOptions {
            file_size: 8,
            dir: dir.path().to_path_buf(),
            start_index: 0,
        },
        top_cache_size: 16,
    };
    let data = pattern(40);

    let tape: CachedBlobTape<RollingBlobTape> = {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<CachedBlobTape<RollingBlobTape>>("tape", options.clone())
            .unwrap();
        append.append_bytes(&tape, &data).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&tape), Some(40));

    // Full read (spans the cache boundary at 24).
    let mut contents = [0; 40];
    reader.read_bytes(&tape, 0, &mut contents).unwrap();
    assert_eq!(&contents, &data[..]);

    // A sub-range entirely inside the cache.
    let mut in_cache = [0; 4];
    reader.read_bytes(&tape, 26, &mut in_cache).unwrap();
    assert_eq!(&in_cache, &data[26..30]);

    // A sub-range that straddles the cache boundary.
    let mut straddle = [0; 8];
    reader.read_bytes(&tape, 22, &mut straddle).unwrap();
    assert_eq!(&straddle, &data[22..30]);
}

/// Multiple concurrent readers must all see the committed data.
#[test]
fn concurrent_readers() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern(200);

    {
        let tapes = Tapes::open(dir.path()).unwrap();
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<WholeBlobTape>("tape", whole_options(&dir))
            .unwrap();
        append.append_bytes(&tape, &data).unwrap();
        append.commit(Persistence::SyncData).unwrap();
    }

    let dir_path = dir.path().to_path_buf();
    let data_clone = data.clone();
    let mut handles = Vec::new();
    for _ in 0..4 {
        let data = data_clone.clone();
        let dir_path = dir_path.clone();
        handles.push(std::thread::spawn(move || {
            let tapes = Tapes::open(&dir_path).unwrap();
            let tape = {
                let mut append = tapes.append();
                let tape = append
                    .open_blob_tape::<WholeBlobTape>(
                        "tape",
                        WholeTapeOpenOptions {
                            dir: dir_path.clone(),
                        },
                    )
                    .unwrap();
                append.commit(Persistence::Buffer).unwrap();
                tape
            };
            let reader = tapes.reader();
            let mut contents = [0; 200];
            reader.read_bytes(&tape, 0, &mut contents).unwrap();
            assert_eq!(&contents, &data[..]);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

/// Opening a blob tape whose committed length is not a multiple of the entry
/// size as a fixed-sized tape must error.
#[test]
fn fixed_size_mismatch_errors() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = whole_options(&dir);

    // Create a blob tape with 5 bytes (not a multiple of 8).
    {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<WholeBlobTape>("tape", options.clone())
            .unwrap();
        append.append_bytes(&tape, b"abcde").unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    let result = {
        let mut append = tapes.append();
        append.open_fixed_sized_tape::<u64, WholeBlobTape>("tape", options)
    };
    assert!(result.is_err(), "expected a size-mismatch error, got Ok");
}

/// `iter_from` from exactly the end yields an empty iterator; past the end errors.
#[test]
fn iter_from_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = whole_options(&dir);

    let tape: FixedSizedTape<u64, WholeBlobTape> = {
        let mut append = tapes.append();
        let tape = append
            .open_fixed_sized_tape::<u64, WholeBlobTape>("tape", options.clone())
            .unwrap();
        append.append_entries(&tape, &[1, 2, 3]).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    let reader = tapes.reader();

    // `from == len` → empty iterator, no error.
    let collected: Vec<u64> = reader
        .iter_from(&tape, 3)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(collected.is_empty());

    // `from > len` → error.
    let result = reader.iter_from(&tape, 4);
    assert!(result.is_err(), "expected a past-end error");
}

/// A dropped append to a rolling tape must not corrupt the committed data.
///
/// (The rolling writer's `revert` is a no-op, so this verifies the stale bytes
/// written to disk are never observed.)
#[test]
fn dropped_rolling_append_does_not_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = RollingTapeOpenOptions {
        file_size: 8,
        dir: dir.path().to_path_buf(),
        start_index: 0,
    };
    let data = pattern(40);

    let tape = {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<RollingBlobTape>("tape", options.clone())
            .unwrap();
        append.append_bytes(&tape, &data).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    // A dropped append (no commit) writes stale bytes past the committed length.
    {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<RollingBlobTape>("tape", options.clone())
            .unwrap();
        append.append_bytes(&tape, b"STALE").unwrap();
        // `append` is dropped here WITHOUT commit.
    }

    // The committed data must be intact.
    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&tape), Some(40));

    let mut contents = [0; 40];
    reader.read_bytes(&tape, 0, &mut contents).unwrap();
    assert_eq!(&contents, &data[..]);
}

/// Multiple appends to the same tape within one transaction.
#[test]
fn multiple_appends_same_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = whole_options(&dir);

    let tape = {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<WholeBlobTape>("tape", options.clone())
            .unwrap();
        append.append_bytes(&tape, b"abc").unwrap();
        append.append_bytes(&tape, b"def").unwrap();
        append.append_bytes(&tape, b"ghi").unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&tape), Some(9));

    let mut contents = [0; 9];
    reader.read_bytes(&tape, 0, &mut contents).unwrap();
    assert_eq!(&contents, b"abcdefghi");
}

/// `tape_exists` distinguishes existing from missing tapes.
#[test]
fn tape_exist_distinguishes_missing() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();

    {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<WholeBlobTape>("existing", whole_options(&dir))
            .unwrap();
        append.append_bytes(&tape, b"hi").unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    let append = tapes.append();
    assert!(append.tape_exists("existing"));
    assert!(!append.tape_exists("nonexistent"));
}
