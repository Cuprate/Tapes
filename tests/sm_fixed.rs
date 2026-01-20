use proptest::collection::vec;
use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use std::fmt::{Debug, Formatter};

use tapes::{FixedSizedTape, Persistence, TapeOpenOptions, Tapes};

#[derive(Clone, Debug)]
enum TapeTransition {
    Append(Vec<u64>),
    Pop(u64),
    ReOpen,
}

struct TapesState {
    dir: tempfile::TempDir,

    tapes: Tapes,
    tape: FixedSizedTape<u64>,
}

impl Debug for TapesState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("tapes").finish()
    }
}

#[derive(Clone, Debug)]
struct StateMachineState {
    buf: Vec<u64>,
    cache_size: u64,
}

struct StateMachine;

impl ReferenceStateMachine for StateMachine {
    type State = StateMachineState;
    type Transition = TapeTransition;

    fn init_state() -> BoxedStrategy<Self::State> {
        (0..100_000_u64, vec(any::<u64>(), 0..1_000_usize))
            .prop_map(|(cache_size, buf)| StateMachineState { buf, cache_size })
            .boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        prop_oneof![
            20 => proptest::collection::vec(any::<u64>(), 1..1000).prop_map(TapeTransition::Append),
            5 => {
                (0..=state.buf.len() as u64).prop_map(TapeTransition::Pop)
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
            TapeTransition::Pop(amt) => {
                state.buf.drain(state.buf.len() - *amt as usize..);
            }
            TapeTransition::ReOpen => (),
        }

        state
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        match transition {
            TapeTransition::Pop(len) => *len <= state.buf.len() as u64,
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
            .open_fixed_sized_tape(
                &"tape",
                &TapeOpenOptions {
                    top_cache_size: ref_state.cache_size,
                    dir: dir.path().to_path_buf(),
                },
            )
            .unwrap();

        append.append_entries(&tape, &ref_state.buf).unwrap();

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

                let _ = append.append_entries(&state.tape, &data).unwrap();

                append.commit(Persistence::Buffer).unwrap();
            }
            TapeTransition::Pop(amt) => {
                let mut truncate = state.tapes.truncate();

                truncate.drop_from_fixed_sized_tape(&state.tape, amt);

                truncate.commit(Persistence::Buffer).unwrap();
            }
            TapeTransition::ReOpen => {
                let tapes = Tapes::open(&state.dir.path()).unwrap();
                let mut append = tapes.append();

                let tape = append
                    .open_fixed_sized_tape(
                        "tape",
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
            reader.fixed_sized_tape_len(&state.tape).unwrap(),
            ref_state.buf.len() as u64
        );

        for i in 1..ref_state.buf.len() {
            let mut buf = vec![0; i];

            reader.read_entries(&state.tape, 0, &mut buf).unwrap();

            assert_eq!(buf.as_slice(), &ref_state.buf[..i]);

            reader
                .read_entries(&state.tape, ref_state.buf.len() as u64 - i as u64, &mut buf)
                .unwrap();

            assert_eq!(buf.as_slice(), &ref_state.buf[ref_state.buf.len() - i..]);

            assert_eq!(
                reader.read_entry(&state.tape, i as u64).unwrap(),
                ref_state.buf.get(i).copied()
            );
        }

        let mut last_i = usize::MAX;
        for (i, entry) in reader.iter_from(&state.tape, 0).unwrap().enumerate() {
            assert_eq!(entry.unwrap(), ref_state.buf[i]);
            last_i = i;
        }
        
        assert_eq!(last_i.wrapping_add(1), ref_state.buf.len());
        

        drop(reader);

        state
    }
}

prop_state_machine! {
    #[test]
    fn state_machine_fixed_size(sequential 1..20 => TapesState);
}
