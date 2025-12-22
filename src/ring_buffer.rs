use std::mem::MaybeUninit;

pub(crate) struct RingBuffer {
    buf: Box<[MaybeUninit<u8>]>,
    start: usize,
    len: usize,
}

impl RingBuffer {

}