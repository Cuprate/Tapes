use std::io;

use crate::{FixedSizedTape, TapesRead};

pub struct Iter<'a, E, T: ?Sized> {
    tape: &'a FixedSizedTape<E>,
    tx: &'a T,
    start_index: u64,

    buf: Vec<E>,
    index: u64,

    tape_len: u64,
}

impl<'a, E: bytemuck::Pod, T: TapesRead + ?Sized> Iter<'a, E, T> {
    pub(crate) fn new(
        tape: &'a FixedSizedTape<E>,
        tx: &'a T,
        start_index: u64,
        tape_len: u64,
    ) -> io::Result<Self> {
        const READ_AHEAD_SIZE: usize = 8 * 1024;

        let mut buf = vec![E::zeroed(); READ_AHEAD_SIZE / size_of::<E>()];

        let entries_to_read =
            buf.len() - (start_index as usize + buf.len()).saturating_sub(tape_len as usize);

        if entries_to_read != 0 {
            tx.read_entries(tape, start_index, &mut buf[..entries_to_read])?;
        }

        Ok(Self {
            tape,
            tx,
            start_index,
            buf,
            index: 0,
            tape_len,
        })
    }
}

impl<'a, E: bytemuck::Pod, T: TapesRead + ?Sized> Iterator for Iter<'a, E, T> {
    type Item = io::Result<E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start_index + self.index >= self.tape_len {
            return None;
        }

        let index_to_buf = self.index as usize % self.buf.len();

        if self.index != 0 && index_to_buf == 0 {
            let offset = self.start_index + self.index;
            let entries_to_read = self.buf.len()
                - (offset as usize + self.buf.len()).saturating_sub(self.tape_len as usize);

            if entries_to_read != 0
                && let Err(e) =
                    self.tx
                        .read_entries(self.tape, offset, &mut self.buf[..entries_to_read])
            {
                return Some(Err(e));
            }
        }

        let next = self.buf[index_to_buf];
        self.index += 1;

        Some(Ok(next))
    }
}
