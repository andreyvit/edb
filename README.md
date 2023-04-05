# Go Embedded Database — in-process document database using Bolt (Badger soon)

[![Go reference](https://pkg.go.dev/badge/github.com/andreyvit/edb.svg)](https://pkg.go.dev/github.com/andreyvit/edb) [![Go Report Card](https://goreportcard.com/badge/github.com/andreyvit/edb)](https://goreportcard.com/report/github.com/andreyvit/edb)


Why?
----

A single server in 202x can handle almost any load almost any small-to-mid-sized company has. This means you can *massively* save on development time, costs and complexity by going back from the cloud to a dedicated unicorn server.

This won't be the right choice _every_ time; there's availability and security boundaries to consider, too. However, a lot of companies could benefit from faster time-to-market and smaller development teams, and it is very often the right choice for startups in particular.

To realize those benefits, you need an efficient database:

* Very fast data access means that, instead of optimizing a tricky SQL, you can just _loop over your data_ with code. Imagine the savings.
* Serialized writes simplify statistics & similar, eliminate a whole class of errors, and greatly simplify the code.

To access data fast, you want that data to already be a part of your process memory. In-process key-value datastores do that quickly an efficiently. We're using Bolt for now, but its maintenance story leaves a lot to be desired, so we'll be switching to Badger soon.

We use msgpack for encoding structs currently. More options are possible, but not available right now.


Usage
-----

Install:

    go get github.com/andreyvit/edb

Saves ordinary structs in the database, the first field is the primary key (use another struct for a composite key) and must be marked as `msgpack:"-"` to avoid storing it as part of the value:

```go
type Post struct {
    ID        string    `msgpack:"-"`
    Time      time.Time `msgpack:"tm"`
    Author    string    `msgpack:"a"`
    Content   string    `msgpack:"c"`
    Published bool      `msgpack:"pub"`
}
```

Define schema:

```go
var (
    mySchema = edb.NewSchema(edb.SchemaOpts{})
    postsTable = edb.AddTable[Post](mySchema, "posts", 1, nil, nil, nil)
)
```

Those nils are: indexer func, migration func, a list of indices, all optional. Let's add a couple of indices:

```go
var (
    postsTable = edb.AddTable(mySchema, "posts", 1, func (post *Post, ib *edb.IndexBuilder) {
        if post.Author != "" {
            ib.Add(postsByAuthor, post.Author)
        }
        if post.Published {
            ib.Add(publishedPostsByTime, post.Time)
        }
    }, nil, []*edb.Index{
        postsByAuthor,
        publishedPostsByTime,
    })
    postsByAuthor = AddIndex[string]("by_author")
    publishedPostsByTime = AddIndex[time.Time]("published_by_time")
)
```

Open a db:

```go
db := must(edb.Open(filePath, mySchema, edb.Options{}))
```

Save a post:

```go
post := &Post{
    ID:        "123", // use UUID generator here, or Snowflake IDs, or something
    Time:      time.Now(),
    Author:    "alice",
    Content:   "This is my first post.",
    Published: true,
}
ensure(db.Tx(true, func(tx *db.Tx) error {
    edb.Put(tx, post) // no error possible, all errors are only returned when committing a tx
    return nil
}))
```

All operations must be performed inside a transaction. Those can be read-only (`db.Tx(false, ...)`) or mutable (`db.Tx(true, ...)`). We'll assume you have a transaction going in the code below.

Find a post by ID:

```go
post := edb.Get[Post](tx, "123")
if post == nil {
    log.Printf("not found")
} else {
    log.Printf("post = %v", *post)
}
```

Find posts by author name:

```go
for c := edb.ExactIndexScan[Post](tx, postsByAuthor, "alice"); c.Next(); {
    post := c.Row()
    log.Printf("found: %v", *post)
}
```

`ExactIndexScan` is a helper that combines `IndexScan` with `ExactScan` option. To find posts by time, scanning backwards, we'll have to use these lower-level tools:

```go
for c := edb.IndexScan[Post](tx, postsByAuthor, edb.ExactScan("alice").Reversed()); c.Next(); {
    post := c.Row()
    log.Printf("found: %v", *post)
}
```

You can scan the entire table:

```go
for c := edb.TableScan[Post](tx, edb.FullScan()); c.Next(); {
    post := c.Row()
    log.Printf("found: %v", *post)
}
```

Use `edb.All` to obtain a slice of all rows from a cursor (noting, of course, that this might use unbounded memory, so looping is preferrable whenever possible):

```go
allPosts := edb.All(edb.ExactIndexScan[Post](tx, postsByAuthor, "alice"))
```

Migrate schema versions by adding a migrator func; the number you pass in to `edb.AddTable` is the latest schema version number, and your func is responsible for migrating older versions:

```go
var (
    postsTable = edb.AddTable(mySchema, "posts", 2, ..., func(tx *edb.Tx, post *Post, oldVer uint64) {
        if oldVer < 2 {
            post.Author = strings.ToLower(posts.Author)
        }
    }, ...)
)
```

The migrator will be invoked when loading a post. Schema version is stored per row, and you don't need to migrate all rows immediately; it's okay to migrate them as they get saved. We'll add more options for schema migrations later.

These examples use the following two error handling helpers:

```go
func must[T any](v T, err error) T {
    if err != nil {
        panic(err)
    }
    return v
}

func ensure(err error) {
    if err != nil {
        panic(err)
    }
}
```


Technical Details
-----------------

### Buckets

We rely on scoped namespaces for keys called buckets. Bolt supports them natively. A flat database like Badger will simulate buckets via key prefixes. We use nested buckets in Bolt (tablename/data and tablename/i_indexname), but only for conveninece; flat buckets are fine.


### Key Encoding

Keys are encoded using a _tuple encoding_. This concatenates all values together, and then appends a _reversed variable-length encoding_ of lengths of each component except for the last one, and then the number of components. For single-component keys, the overhead is a single byte (1) at the end.


### Value Encoding

Value = value header, encoded data, encoded index keys.

_Value header:__

1. Flags (uvarint).
2. Schema version (uvarint).
3. Data size (uvarint).
4. Index size (uvarint).

_Encoded data_ is just msgpack encoding of the struct.

_Encoded index keys_ record the keys contributed by this row. If index computation changes in the future, we still need to know which index
keys to delete when updating the row, so we store all index keys added by the row. Format:

1. Number of entries (uvarint).
2. For each entry: index ordinal (uvarint), key length (uvarint), key bytes.


## Table States

We store a meta document per table, called “table state”. This document holds info on which indexes are defined for the table, and assigns
an _index ordinal_ (a unique positive integer) to each one. Ordinals are never reused as indexes are removed and added, so are safe to store inside values.

We should probably move to a single per-database state document.


Contributing
------------

Contributions are welcome. There's a lot to do here still. Tests and documention will be much appreciated, too.

Auto-testing via modd (`go install github.com/cortesi/modd/cmd/modd@latest`):

    modd


MIT license
-----------

Copyright (c) 2023 Andrey Tarantsov. Published under the terms of the [MIT license](LICENSE).
