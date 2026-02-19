use proptest::collection::vec;
use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use std::fmt::{Debug, Formatter};

use tapes::{BlobTape, Persistence, TapeOpenOptions, Tapes, TapesAppend, TapesRead, TapesTruncate};

#[derive(Clone, Debug)]
enum TapeTransition {
    Append(Vec<u8>),
    Truncate(u64),
    ReOpen,
}

struct TapesState {
    dir: tempfile::TempDir,

    tapes: Tapes,
    tape: BlobTape,
}

impl Debug for TapesState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("tapes")
            .field("cache", &self.tape.top_cache.read().as_slices())
            .finish()
    }
}

#[derive(Clone, Debug)]
struct StateMachineState {
    buf: Vec<u8>,
    cache_size: u64,
}

struct StateMachine;

impl ReferenceStateMachine for StateMachine {
    type State = StateMachineState;
    type Transition = TapeTransition;

    fn init_state() -> BoxedStrategy<Self::State> {
        (0..100_000_u64, vec(any::<u8>(), 0..1_000_usize))
            .prop_map(|(cache_size, buf)| StateMachineState { buf, cache_size })
            .boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        prop_oneof![
            20 => proptest::collection::vec(any::<u8>(), 1..1000).prop_map(TapeTransition::Append),
            5 => {
                (0..=state.buf.len() as u64).prop_map(TapeTransition::Truncate)
            },
            5 => Just(TapeTransition::ReOpen)
        ]
        .boxed()
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            TapeTransition::Append(data) => {
                state.buf.extend_from_slice(&data);
            }
            TapeTransition::Truncate(new_len) => state.buf.truncate(*new_len as usize),
            TapeTransition::ReOpen => (),
        }

        state
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        match transition {
            TapeTransition::Truncate(len) => *len <= state.buf.len() as u64,
            _ => true,
        }
    }
}

impl StateMachineTest for TapesState {
    type SystemUnderTest = Self;
    type Reference = StateMachine;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        let dir = tempfile::tempdir().unwrap();

        let tapes = Tapes::open(dir.path()).unwrap();
        let mut append = tapes.append();

        let tape = append
            .open_blob_tape(
                &"tape",
                &TapeOpenOptions {
                    top_cache_size: ref_state.cache_size,
                    dir: dir.path().to_path_buf(),
                },
            )
            .unwrap();

        append.append_bytes(&tape, &ref_state.buf).unwrap();

        append.commit(Persistence::SyncData).unwrap();

        drop(append);

        TapesState { dir, tapes, tape }
    }

    fn apply(
        mut state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        match transition {
            TapeTransition::Append(data) => {
                let mut append = state.tapes.append();

                let _ = append.append_bytes(&state.tape, &data).unwrap();

                append.commit(Persistence::Buffer).unwrap();
            }
            TapeTransition::Truncate(new_len) => {
                let mut truncate = state.tapes.truncate();

                truncate.truncate_blob_tape(&state.tape, new_len);

                truncate.commit(Persistence::Buffer).unwrap();
            }
            TapeTransition::ReOpen => {
                let tapes = Tapes::open(&state.dir.path()).unwrap();
                let mut append = tapes.append();

                let tape = append
                    .open_blob_tape(
                        &"tape",
                        &TapeOpenOptions {
                            top_cache_size: ref_state.cache_size,
                            dir: state.dir.path().to_path_buf(),
                        },
                    )
                    .unwrap();

                append.commit(Persistence::Buffer).unwrap();

                drop(append);
                state.tapes = tapes;
                state.tape = tape;
            }
        }
        let reader = state.tapes.reader();
        assert_eq!(
            reader.blob_tape_len(&state.tape).unwrap(),
            ref_state.buf.len() as u64
        );

        for i in 1..ref_state.buf.len() {
            let mut buf = vec![0; i];

            reader.read_bytes(&state.tape, 0, &mut buf).unwrap();

            assert_eq!(buf.as_slice(), &ref_state.buf[..i]);

            reader
                .read_bytes(&state.tape, ref_state.buf.len() as u64 - i as u64, &mut buf)
                .unwrap();

            assert_eq!(buf.as_slice(), &ref_state.buf[ref_state.buf.len() - i..]);
        }

        drop(reader);

        state
    }
}

prop_state_machine! {
    #[test]
    fn state_machine_blob(sequential 1..20 => TapesState);
}
