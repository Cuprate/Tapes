use std::io;

use tapes::{Persistence, Tapes, TapesAppend, TapesRead, WholeBlobTape, WholeTapeOpenOptions};

#[test]
fn delete_blob_tape() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();

    let options = WholeTapeOpenOptions {
        dir: dir.path().to_path_buf(),
    };

    let mut append = tapes.append();
    let tape = append
        .open_blob_tape::<WholeBlobTape>("blob", options)
        .unwrap();
    append.append_bytes(&tape, b"contents").unwrap();
    append.commit(Persistence::Buffer).unwrap();

    {
        let reader = tapes.reader();
        assert_eq!(reader.blob_tape_len(&tape), Some(8));
    }

    let tape_dir = dir.path().join("tapes").join("blob");
    assert!(tape_dir.exists());

    tapes.delete_tape(tape).unwrap();

    assert!(!tape_dir.exists());

    {
        let append = tapes.append();
        assert!(!append.tape_exists("blob"));
    }
}

#[test]
fn refuses_to_delete_while_a_transaction_is_active() {
    let dir = tempfile::tempdir().unwrap();
    let tapes = Tapes::open(dir.path()).unwrap();

    let options = WholeTapeOpenOptions {
        dir: dir.path().to_path_buf(),
    };

    let mut append = tapes.append();
    let tape = append
        .open_blob_tape::<WholeBlobTape>("blob", options)
        .unwrap();
    append.append_bytes(&tape, b"contents").unwrap();
    append.commit(Persistence::Buffer).unwrap();

    let _reader = tapes.reader();

    let err = tapes.delete_tape(tape).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
}
