use std::fs::File;
use std::io;

pub(crate) fn read_exact_at_file(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;

        file.read_exact_at(buf, offset)
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;

        let mut buf = buf;
        let mut offset = offset;
        while !buf.is_empty() {
            match file.seek_read(buf, offset) {
                Ok(0) => {
                    break;
                }
                Ok(n) => {
                    buf = &mut buf[n..];
                    offset += n as u64;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if !buf.is_empty() {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill the whole buffer",
            ))
        } else {
            Ok(())
        }
    }
}

pub(crate) fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;

        file.write_all_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;

        let n = file.seek_write(buf, offset)?;
        if n != buf.len() {
            return Err(io::Error::other("Failed to write all bytes to tape"));
        }

        Ok(())
    }
}
