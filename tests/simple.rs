use tapes::{Advice, Flush, InMemory, Tape, Tapes};

#[test]
fn write_read() {
    let tapes = unsafe {
        Tapes::<InMemory>::new(
            vec![Tape {
                name: "test",
                backing_memory_options: (),
                advice: Advice::Normal,
                initial_memory_size: 10,
            }],
            (),
            10,
        )
    }
    .unwrap();

    let writer = tapes.appender();

    let mut test_tape = writer.blob_tape_appender("test");

    test_tape.push_bytes([1_u8].as_slice()).unwrap();
    test_tape.push_bytes([2_u8; 9].as_slice()).unwrap();
    test_tape.push_bytes([3_u8; 900].as_slice()).unwrap();

    drop(test_tape);
    writer.flush(Flush::Async).unwrap();

    let reader = tapes.reader().unwrap();

    let test_tape = reader.blob_tape_tape_slice("test");

    assert_eq!(test_tape[0], 1);
    assert_eq!(&test_tape[1..10], &[2_u8; 9]);
    assert_eq!(&test_tape[10..910], &[3_u8; 900]);
}
