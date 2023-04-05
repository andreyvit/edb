/*
Package edb implements a document database on top of a key-value store
(in this case, on top of Bolt).

We implement:

1. Tables, collection of arbitrary documents marshaled from the given struct.

2. Indices, allowing quick lookup of table data by custom values.

3. Maps, exposing raw key-value buckets with string keys and untyped values.

4. Singleton Keys, allowing to store a typed value for a given key within a map
(say, a “config” value in a “globals” map).

# Technical Details

**Buckets.**
We rely on scoped namespaces for keys called buckets. Bolt supports them natively.
A flat database like Redis could simulate buckets via key prefixes.
We use nested buckets in Bolt, but only for conveninece; flat buckets are fine.

**Index ordinal**
We assign a unique positive integer ordinal to each index. These values are never
reused, even if an index is removed.

**Table states**
We store a meta document per table, called “table state”. This document holds
the information about which indexes are defined for the table, and assigns
a  (an ordinal) to each one. Ordinals are never
reused as indexes are removed and added.

## Binary encoding

**Key encoding**.
Keys are encoded using a _tuple encoding_.

**Value**: value header, then encoded data, then encoded index key records.

**Value header**:
1. Flags (uvarint).
2. Schema version (uvarint).
3. Data size (uvarint).
3. Index size (uvarint).

**Value data**: msgpack of the row struct.

**Index key records** (inside a value) record the keys contributed by this row.
If index computation changes in the future, we still need to know which index
keys to delete when updating the row, so we store all index keys. Format:
1. Number of entries (uvarint).
2. For each entry: index ordinal (uvarint), key length (uvarint), key bytes.
*/
package edb
