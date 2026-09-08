use std::{sync::mpsc, thread, time::Duration};

use tapes::{
    CachedBlobTape, CachedTapeOpenOptions, Persistence, Tapes, TapesAppend, TapesRead,
    TapesTruncate, WholeBlobTape, WholeTapeOpenOptions,
};

#[test]
fn reader_keeps_its_snapshot_after_truncate_commits() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = CachedTapeOpenOptions {
        inner: WholeTapeOpenOptions {
            dir: dir.path().to_path_buf(),
        },
        top_cache_size: 4,
    };

    let mut append = tapes.append();
    let tape = append
        .open_blob_tape::<CachedBlobTape<WholeBlobTape>>("tape", options)
        .unwrap();
    append.append_bytes(&tape, b"abcdefgh").unwrap();
    append.commit(Persistence::Buffer).unwrap();

    let old_reader = tapes.reader();

    let mut truncate = tapes.truncate();
    truncate.truncate_blob_tape(&tape, 6).unwrap();
    truncate.commit(Persistence::Buffer).unwrap();

    let new_reader = tapes.reader();
    assert_eq!(new_reader.blob_tape_len(&tape), Some(6));
    let mut new_contents = [0; 6];
    new_reader.read_bytes(&tape, 0, &mut new_contents).unwrap();
    assert_eq!(&new_contents, b"abcdef");

    assert_eq!(old_reader.blob_tape_len(&tape), Some(8));
    let mut old_contents = [0; 8];
    old_reader.read_bytes(&tape, 0, &mut old_contents).unwrap();
    assert_eq!(&old_contents, b"abcdefgh");
}

#[test]
fn reader_keeps_old_bytes_after_a_dropped_append_and_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = CachedTapeOpenOptions {
        inner: WholeTapeOpenOptions {
            dir: dir.path().to_path_buf(),
        },
        top_cache_size: 8,
    };

    let mut append = tapes.append();
    let tape = append
        .open_blob_tape::<CachedBlobTape<WholeBlobTape>>("tape", options)
        .unwrap();
    append.append_bytes(&tape, b"abcdefgh").unwrap();
    append.commit(Persistence::Buffer).unwrap();

    {
        let mut append = tapes.append();
        append.append_bytes(&tape, b"stale").unwrap();
    }

    let old_reader = tapes.reader();

    let mut truncate = tapes.truncate();
    truncate.truncate_blob_tape(&tape, 4).unwrap();
    truncate.commit(Persistence::Buffer).unwrap();

    let mut old_contents = [0; 8];
    old_reader.read_bytes(&tape, 0, &mut old_contents).unwrap();
    assert_eq!(&old_contents, b"abcdefgh");
    drop(old_reader);

    let mut append = tapes.append();
    append.append_bytes(&tape, b"XY").unwrap();
    append.commit(Persistence::Buffer).unwrap();

    let reader = tapes.reader();
    let mut contents = [0; 6];
    reader.read_bytes(&tape, 0, &mut contents).unwrap();
    assert_eq!(&contents, b"abcdXY");
}

#[test]
fn append_after_truncate_waits_for_the_old_reader() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = CachedTapeOpenOptions {
        inner: WholeTapeOpenOptions {
            dir: dir.path().to_path_buf(),
        },
        top_cache_size: 4,
    };

    let mut append = tapes.append();
    let tape = append
        .open_blob_tape::<CachedBlobTape<WholeBlobTape>>("tape", options)
        .unwrap();
    append.append_bytes(&tape, b"abcdefgh").unwrap();
    append.commit(Persistence::Buffer).unwrap();

    let old_reader = tapes.reader();

    let mut truncate = tapes.truncate();
    truncate.truncate_blob_tape(&tape, 6).unwrap();
    truncate.commit(Persistence::Buffer).unwrap();

    thread::scope(|scope| {
        let (attempting_tx, attempting_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        let tapes = &tapes;
        let tape = &tape;

        scope.spawn(move || {
            attempting_tx.send(()).unwrap();

            let mut append = tapes.append();
            append.append_bytes(tape, b"XY").unwrap();
            append.commit(Persistence::Buffer).unwrap();

            finished_tx.send(()).unwrap();
        });

        attempting_rx.recv().unwrap();
        let finished_while_reader_was_alive =
            finished_rx.recv_timeout(Duration::from_millis(100)).is_ok();

        let mut old_contents = [0; 8];
        old_reader.read_bytes(tape, 0, &mut old_contents).unwrap();
        assert_eq!(&old_contents, b"abcdefgh");
        drop(old_reader);

        if !finished_while_reader_was_alive {
            finished_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }

        assert!(!finished_while_reader_was_alive);
    });

    let reader = tapes.reader();
    let mut contents = [0; 8];
    reader.read_bytes(&tape, 0, &mut contents).unwrap();
    assert_eq!(&contents, b"abcdefXY");
}
