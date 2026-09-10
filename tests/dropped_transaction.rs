use tapes::{
    Persistence, Tapes, TapesAppend, TapesRead, TapesTruncate, WholeBlobTape, WholeTapeOpenOptions,
};

#[test]
fn dropped_append_is_overwritten_by_the_next_append() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = WholeTapeOpenOptions {
        dir: dir.path().to_path_buf(),
    };

    let mut append = tapes.append();
    let tape = append
        .open_blob_tape::<WholeBlobTape>("tape", options)
        .unwrap();
    append.append_bytes(&tape, b"abcde").unwrap();
    append.commit(Persistence::Buffer).unwrap();

    {
        let mut append = tapes.append();
        assert_eq!(append.append_bytes(&tape, b"X").unwrap(), 5);
    }

    let mut append = tapes.append();
    assert_eq!(append.append_bytes(&tape, b"Y").unwrap(), 5);
    append.commit(Persistence::Buffer).unwrap();

    let reader = tapes.reader();
    let mut contents = [0; 6];
    reader.read_bytes(&tape, 0, &mut contents).unwrap();
    assert_eq!(&contents, b"abcdeY");
}

#[test]
fn dropped_large_append_is_not_visible() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = WholeTapeOpenOptions {
        dir: dir.path().to_path_buf(),
    };

    let mut append = tapes.append();
    let tape = append
        .open_blob_tape::<WholeBlobTape>("tape", options)
        .unwrap();
    append.append_bytes(&tape, b"abcdefgh").unwrap();
    append.commit(Persistence::Buffer).unwrap();

    {
        let mut append = tapes.append();
        assert_eq!(append.append_bytes(&tape, b"stale").unwrap(), 8);
    }

    let reader = tapes.reader();
    let mut original_contents = [0; 8];
    reader.read_bytes(&tape, 0, &mut original_contents).unwrap();
    assert_eq!(&original_contents, b"abcdefgh");
    drop(reader);

    let mut append = tapes.append();
    assert_eq!(append.append_bytes(&tape, b"XY").unwrap(), 8);
    append.commit(Persistence::Buffer).unwrap();

    let reader = tapes.reader();
    let mut contents = [0; 10];
    reader.read_bytes(&tape, 0, &mut contents).unwrap();
    assert_eq!(&contents, b"abcdefghXY");
}

#[test]
fn dropped_truncate_does_not_change_the_tape() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = WholeTapeOpenOptions {
        dir: dir.path().to_path_buf(),
    };

    let mut append = tapes.append();
    let tape = append
        .open_blob_tape::<WholeBlobTape>("tape", options)
        .unwrap();
    append.append_bytes(&tape, b"abcdefgh").unwrap();
    append.commit(Persistence::Buffer).unwrap();

    {
        let mut truncate = tapes.truncate();
        truncate.truncate_blob_tape(&tape, 6).unwrap();
        truncate.truncate_blob_tape(&tape, 4).unwrap();

        let mut contents = [0; 4];
        truncate.read_bytes(&tape, 0, &mut contents).unwrap();
        assert_eq!(&contents, b"abcd");
    }

    let reader = tapes.reader();
    let mut contents = [0; 8];
    reader.read_bytes(&tape, 0, &mut contents).unwrap();
    assert_eq!(&contents, b"abcdefgh");
}

#[test]
fn append_after_committed_truncate_overwrites_the_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();
    let options = WholeTapeOpenOptions {
        dir: dir.path().to_path_buf(),
    };

    let mut append = tapes.append();
    let tape = append
        .open_blob_tape::<WholeBlobTape>("tape", options)
        .unwrap();
    append.append_bytes(&tape, b"abcdefgh").unwrap();
    append.commit(Persistence::Buffer).unwrap();

    {
        let mut truncate = tapes.truncate();
        truncate.truncate_blob_tape(&tape, 6).unwrap();
        truncate.truncate_blob_tape(&tape, 4).unwrap();

        truncate.commit(Persistence::Buffer).unwrap();
    }

    let reader = tapes.reader();
    assert_eq!(reader.blob_tape_len(&tape), Some(4));
    let mut contents = [0; 4];
    reader.read_bytes(&tape, 0, &mut contents).unwrap();
    assert_eq!(&contents, b"abcd");
    drop(reader);

    let mut append = tapes.append();
    assert_eq!(append.append_bytes(&tape, b"XY").unwrap(), 4);
    append.commit(Persistence::Buffer).unwrap();

    let reader = tapes.reader();
    let mut contents = [0; 6];
    reader.read_bytes(&tape, 0, &mut contents).unwrap();
    assert_eq!(&contents, b"abcdXY");
}
