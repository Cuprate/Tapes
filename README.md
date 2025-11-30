# Tapes Database

A specialised database for storing data in contiguous tapes.

Each environment supports multiple independent tapes, with ACID updates across them. There are 2 types of tapes: fixed-sized and
blob. Fixed-sized tapes store fixed sized values, which allows arbitrary lookup of values by their index, blob tapes however is
just a contiguous slice of bytes so to access data you must keep its index.

Each tape is memory mapped and supports zero-copy access.

## Supported Operations

This database is not a generic key/value store. This crate is highly specialised for storing data that builds on top of previous
data, in a way that old data will only be removed if data created after it is also removed and that removing data is less 
common than adding data. An example of such data would be an average blockchain.

Instead of there being a reader and writer transactions, write transactions are split into append and pop. This means there
are 3 transaction types: read, append and pop. Splitting the writer transaction like this makes the database more efficient 
at the cost of not being able to do a single atomic rewrite of data. It is important to note though that removing data and
then writing more is still ACID, it's just the database could be left with the data being removed without the new data being
written.