use std::io;

use crate::{FixedSizedTape, TapesRead, TapesReadTransaction};

pub struct Iter<'a, E> {
    tape: &'a FixedSizedTape<E>,
    tapes_read_transaction: &'a TapesReadTransaction<'a>,
    start_index: u64,

    buf: Vec<E>,
    index: u64,

    tape_len: u64,
}

impl<'a, E: bytemuck::Pod> Iter<'a, E> {
    pub(crate) fn new(
        tape: &'a FixedSizedTape<E>,
        tapes_read_transaction: &'a TapesReadTransaction<'a>,
        start_index: u64,
        tape_len: u64,
    ) -> io::Result<Self> {
        const READ_AHEAD_SIZE: usize = 8 * 1024;

        let mut buf = vec![E::zeroed(); READ_AHEAD_SIZE / size_of::<E>()];

        let entries_to_read =
            buf.len() - (start_index as usize + buf.len()).saturating_sub(tape_len as usize);

        if entries_to_read != 0 {
            tapes_read_transaction.read_entries(tape, start_index, &mut buf[..entries_to_read])?;
        }

        Ok(Self {
            tape,
            tapes_read_transaction,
            start_index,
            buf,
            index: 0,
            tape_len,
        })
    }
}

impl<'a, E: bytemuck::Pod> Iterator for Iter<'a, E> {
    type Item = io::Result<E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.tape_len {
            return None;
        }

        let index_to_buf = self.index as usize % self.buf.len();

        let next = self.buf[index_to_buf];
        self.index += 1;

        if index_to_buf == self.buf.len() - 1 {
            let offset = self.start_index + self.index;
            let entries_to_read = self.buf.len()
                - (offset as usize + self.buf.len()).saturating_sub(self.tape_len as usize);

            if entries_to_read != 0 {
                if let Err(e) = self.tapes_read_transaction.read_entries(
                    self.tape,
                    offset,
                    &mut self.buf[..entries_to_read],
                ) {
                    return Some(Err(e));
                }
            }
        }
        Some(Ok(next))
    }
}
