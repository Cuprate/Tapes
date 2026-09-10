# Tapes Database

A specialised database for storing data in contiguous tapes.

A tape is an append only log of data. This database is not a generic key/value store, it is
specialised for data that builds on top of previous data, in a way that old data is only removed
if the data created after it is removed too, and removing data is less common than adding it.
An example of such data would be a typical blockchain.

Each [`Tapes`] instance supports multiple independent tapes, with ACID updates across them.

## Tapes

There are 2 kinds of tapes: fixed sized tapes store fixed sized values, which allows lookup of
values by their index, blob tapes are a contiguous slice of bytes, so to access data you must
keep its index.

A [`WholeBlobTape`] stores all its data in a single file. A [`RollingBlobTape`] stores its data
in multiple files of at most a configured size, and deletes the files that hold removed data
once no reader needs them anymore. On a whole tape removing data from the front does not free up
disk space, on a rolling tape it does. Use a rolling tape when the tape grows without bound and
you need to remove old data FIFO, if not use a whole tape.

A [`FixedSizedTape`] is a handle to a blob tape that reads and writes fixed sized entries instead
of raw bytes, the entry type must be plain data with no padding [`bytemuck::Pod`].

A [`CachedBlobTape`] is a wrapper around a blob tape that keeps the top of the tape in memory,
this speeds up access to recent data and reduces disk I/O. These are both wrappers, so they can
be combined, for example a fixed sized tape over a cached rolling tape.

You probably want to always use a [`CachedBlobTape`] to prevent doing slow direct I/O.

## Transactions

Write transactions are split into append and pop, this means there are 3 transaction types:
read, append and pop. Splitting the writer transaction like this makes the database more
efficient at the cost of not being able to do a single atomic rewrite of data. Removing data and
then writing more is still ACID, it's just the database could be left with the data being
removed without the new data being written.

## Persistence

Commits are persisted according to the [`Persistence`] mode they are committed with:

- `Buffer` writes to the OS buffer only, it is not durable, data committed with it can be lost
  on a crash until it is flushed to disk by a later commit with `SyncData` or `SyncAll`.
- `SyncData` syncs the file contents to disk.
- `SyncAll` syncs the file contents and file metadata to disk.
