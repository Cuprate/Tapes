use std::io;

use tapes::{
    FixedSizedTape, Persistence, RollingBlobTape, RollingTapeOpenOptions, Tapes, TapesAppend,
    TapesRead, TapesTruncate,
};

const NAME: &str = "tape";

fn options(dir: &tempfile::TempDir, file_size: u64, start_index: u64) -> RollingTapeOpenOptions {
    RollingTapeOpenOptions {
        file_size,
        dir: dir.path().to_path_buf(),
        start_index,
    }
}

fn tape_dir(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join("tapes").join(NAME)
}

/// Writes `len` bytes of a deterministic pattern.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| b'a' + (i % 26) as u8).collect()
}

#[test]
fn rolling_basic_write_read() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = options(&dir, 8, 0);
    let data = pattern(40);

    let tape = {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<RollingBlobTape>(NAME, options)
            .unwrap();
        append.append_bytes(&tape, &data).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&tape), Some(40));
    assert_eq!(reader.blob_tape_start(&tape), Some(0));
    let mut contents = [0; 40];
    reader.read_bytes(&tape, 0, &mut contents).unwrap();
    assert_eq!(&contents, &data[..]);
}

#[test]
fn rolling_read_spans_files() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    // file_size = 4 → 10 bytes span 3 files.
    let options = options(&dir, 4, 0);
    let data = pattern(10);

    let tape = {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<RollingBlobTape>(NAME, options)
            .unwrap();
        append.append_bytes(&tape, &data).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    let reader = tapes.reader();

    // Full read.
    let mut contents = [0; 10];
    reader.read_bytes(&tape, 0, &mut contents).unwrap();
    assert_eq!(&contents, &data[..]);

    // A sub-range that spans the file boundaries at 4 and 8.
    let mut mid = [0; 6];
    reader.read_bytes(&tape, 3, &mut mid).unwrap();
    assert_eq!(&mid, &data[3..9]);
}

#[test]
fn rolling_roll_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = options(&dir, 8, 0);
    let data = pattern(40);

    let tape = {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<RollingBlobTape>(NAME, options)
            .unwrap();
        append.append_bytes(&tape, &data).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    // Roll to start = 16.
    {
        let mut append = tapes.append();
        append.shift_start_idx(&tape, 16).unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&tape), Some(40));
    assert_eq!(reader.blob_tape_start(&tape), Some(16));

    // The live region is [16, 40).
    let mut contents = [0; 24];
    reader.read_bytes(&tape, 16, &mut contents).unwrap();
    assert_eq!(&contents, &data[16..40]);

    // Reading before the start must fail cleanly.
    let mut err_buf = [0; 4];
    let err = reader.read_bytes(&tape, 0, &mut err_buf).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn rolling_roll_deletes_old_files() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    // file_size = 8, write 40 bytes → files 0,1,2,3,4.
    let options = options(&dir, 8, 0);
    let data = pattern(40);

    let tape = {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<RollingBlobTape>(NAME, options)
            .unwrap();
        append.append_bytes(&tape, &data).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    // Before rolling, all 5 files exist.
    let tdir = tape_dir(&dir);
    for i in 0..5 {
        assert!(
            tdir.join(i.to_string()).exists(),
            "file {i} should exist before roll"
        );
    }

    // Roll to start = 16 → files 0 and 1 ([0,8),[8,16)) are fully before the start.
    {
        let mut append = tapes.append();
        append.shift_start_idx(&tape, 16).unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    // Files 0 and 1 should be gone; 2,3,4 should remain.
    assert!(!tdir.join("0").exists(), "file 0 should be deleted");
    assert!(!tdir.join("1").exists(), "file 1 should be deleted");
    assert!(tdir.join("2").exists(), "file 2 should remain");
    assert!(tdir.join("3").exists(), "file 3 should remain");
    assert!(tdir.join("4").exists(), "file 4 should remain");
}

#[test]
fn rolling_roll_then_append() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = options(&dir, 8, 0);
    let data = pattern(40);

    let tape = {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<RollingBlobTape>(NAME, options)
            .unwrap();
        append.append_bytes(&tape, &data).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    {
        let mut append = tapes.append();
        append.shift_start_idx(&tape, 16).unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    // Append 2 more bytes at offset 40.
    {
        let mut append = tapes.append();
        append.append_bytes(&tape, b"XY").unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&tape), Some(42));
    assert_eq!(reader.blob_tape_start(&tape), Some(16));

    // Live region is [16, 42): old [16,40) + new "XY".
    let mut contents = [0; 26];
    reader.read_bytes(&tape, 16, &mut contents).unwrap();
    let mut expected = data[16..40].to_vec();
    expected.extend_from_slice(b"XY");
    assert_eq!(&contents, &expected[..]);
}

#[test]
fn rolling_roll_then_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = options(&dir, 8, 0);
    let data = pattern(40);

    let tape = {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<RollingBlobTape>(NAME, options)
            .unwrap();
        append.append_bytes(&tape, &data).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    {
        let mut append = tapes.append();
        append.shift_start_idx(&tape, 16).unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    // Truncate the tail down to len = 24 → live region [16, 24).
    {
        let mut truncate = tapes.truncate();
        truncate.truncate_blob_tape(&tape, 24).unwrap();
        truncate.commit(Persistence::Buffer).unwrap();
    }

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&tape), Some(24));
    assert_eq!(reader.blob_tape_start(&tape), Some(16));

    let mut contents = [0; 8];
    reader.read_bytes(&tape, 16, &mut contents).unwrap();
    assert_eq!(&contents, &data[16..24]);
}

#[test]
fn rolling_reopen_after_roll() {
    let dir = tempfile::tempdir().unwrap();
    let options = options(&dir, 8, 0);
    let data = pattern(40);

    {
        let tapes = Tapes::open(dir.path()).unwrap();
        let tape = {
            let mut append = tapes.append();
            let tape = append
                .open_blob_tape::<RollingBlobTape>(NAME, options.clone())
                .unwrap();
            append.append_bytes(&tape, &data).unwrap();
            append.commit(Persistence::Buffer).unwrap();
            tape
        };

        let mut append = tapes.append();
        append.shift_start_idx(&tape, 16).unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    // Reopen the database and verify the live region survived.
    let tapes = Tapes::open(dir.path()).unwrap();
    let tape = {
        let mut append = tapes.append();
        append
            .open_blob_tape::<RollingBlobTape>(NAME, options)
            .unwrap()
    };

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&tape), Some(40));
    assert_eq!(reader.blob_tape_start(&tape), Some(16));

    let mut contents = [0; 24];
    reader.read_bytes(&tape, 16, &mut contents).unwrap();
    assert_eq!(&contents, &data[16..40]);

    // Old files should still be gone after reopen.
    let tdir = tape_dir(&dir);
    assert!(!tdir.join("0").exists());
    assert!(!tdir.join("1").exists());
    assert!(tdir.join("2").exists());
}

#[test]
fn rolling_delete() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = options(&dir, 8, 0);

    let tape = {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<RollingBlobTape>(NAME, options)
            .unwrap();
        append.append_bytes(&tape, &pattern(20)).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    assert!(tape_dir(&dir).exists());

    tapes.delete_tape(tape).unwrap();

    assert!(!tape_dir(&dir).exists());
    {
        let append = tapes.append();
        assert!(!append.tape_exists(NAME));
    }
}

#[test]
fn rolling_fixed_write_read() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    // file_size = 8 → each file holds exactly one u64 entry.
    let options = options(&dir, 8, 0);
    let entries: Vec<u64> = vec![10, 20, 30, 40, 50];

    let tape: FixedSizedTape<u64, RollingBlobTape> = {
        let mut append = tapes.append();
        let tape = append
            .open_fixed_sized_tape::<u64, RollingBlobTape>(NAME, options)
            .unwrap();
        append.append_entries(&tape, &entries).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    let reader = tapes.reader();
    assert_eq!(
        reader.fixed_sized_tape_len(&tape),
        Some(entries.len() as u64)
    );

    let mut buf = [0u64; 5];
    reader.read_entries(&tape, 0, &mut buf).unwrap();
    assert_eq!(buf, entries.as_slice());

    // Read a sub-range of entries.
    let mut sub = [0u64; 2];
    reader.read_entries(&tape, 2, &mut sub).unwrap();
    assert_eq!(sub, [30, 40]);

    // Iterator over the whole tape.
    let collected: Vec<u64> = reader
        .iter_from(&tape, 0)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(collected, entries);
}

#[test]
fn rolling_fixed_roll_read() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = options(&dir, 8, 0);
    let entries: Vec<u64> = vec![10, 20, 30, 40, 50, 60];

    let tape: FixedSizedTape<u64, RollingBlobTape> = {
        let mut append = tapes.append();
        let tape = append
            .open_fixed_sized_tape::<u64, RollingBlobTape>(NAME, options.clone())
            .unwrap();
        append.append_entries(&tape, &entries).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    // A separate blob handle (same tape) for the roll and start check.
    let blob: RollingBlobTape = {
        let mut append = tapes.append();
        append
            .open_blob_tape::<RollingBlobTape>(NAME, options)
            .unwrap()
    };

    // Roll to start = 16 → entries 0,1 (bytes [0,16)) are popped.
    {
        let mut append = tapes.append();
        append.shift_start_idx(&blob, 16).unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_start(&blob), Some(16));

    // Live entries are indices 2..6.
    let mut buf = [0u64; 4];
    reader.read_entries(&tape, 2, &mut buf).unwrap();
    assert_eq!(buf, [30, 40, 50, 60]);

    // Reading popped entries fails cleanly.
    let mut err = [0u64; 1];
    let e = reader.read_entries(&tape, 0, &mut err).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn rolling_truncate_below_start_empties_tape() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = options(&dir, 8, 0);
    let data = pattern(40);

    let tape = {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<RollingBlobTape>(NAME, options.clone())
            .unwrap();
        append.append_bytes(&tape, &data).unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    // Roll to start = 16.
    {
        let mut append = tapes.append();
        append.shift_start_idx(&tape, 16).unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    // Truncating below the start index is allowed and empties the tape,
    // moving the start index down to the new length.
    {
        let mut truncate = tapes.truncate();
        truncate.truncate_blob_tape(&tape, 10).unwrap();
        truncate.commit(Persistence::Buffer).unwrap();
    }

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&tape), Some(10));
    assert_eq!(reader.blob_tape_start(&tape), Some(10));

    // Appending after such a truncation continues from the new length.
    {
        let mut append = tapes.append();
        append.append_bytes(&tape, &pattern(8)).unwrap();
        append.commit(Persistence::Buffer).unwrap();
    }

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&tape), Some(18));
    assert_eq!(reader.blob_tape_start(&tape), Some(10));

    let mut contents = vec![0u8; 8];
    reader.read_bytes(&tape, 10, &mut contents).unwrap();
    assert_eq!(contents, pattern(8));
}

#[test]
fn rolling_new_tape_start_index() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = options(&dir, 8, 16);

    let tape = {
        let mut append = tapes.append();
        let tape = append
            .open_blob_tape::<RollingBlobTape>(NAME, options)
            .unwrap();
        append.append_bytes(&tape, b"abcdef").unwrap();
        append.commit(Persistence::Buffer).unwrap();
        tape
    };

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&tape), Some(22));
    assert_eq!(reader.blob_tape_start(&tape), Some(16));

    let mut contents = [0; 6];
    reader.read_bytes(&tape, 16, &mut contents).unwrap();
    assert_eq!(&contents, b"abcdef");
}
