## What?! ##

On https://bugzilla.mozilla.org/show_bug.cgi?id=889583, Taras was suggesting
SQLite was inefficient.  I suggested LevelDB, then I decided I was actually
interested in seeing what LevelDB would look like in this case.  Then I also
decided to get a little fancy in terms of denormalizing and supporting a
potentially efficient awesomebar implementation, etc.

## Make It Go ##

copy your places.sqlite to this directory from your profile

run:
  node convert-places-db.js

it will produce a leveldb-places.ldb directory which is apparently what a
LevelDB database is.

it will explode if that directory already exists, so if you try hacking on
things, then you may be better off typing something more like:
  rm -rf leveldb-places.ldb/; node convert-places-db.js


## Seeing What Went ##

LevelDB is just as hard to read by hand as SQLite, and the skills sadly don't
transfer even if you can read raw SQLite binary data.  So I recommend doing
this:
  node dump-places-leveldb.js | less


## I said What?! ##

There are a lot of comments in convert-places-db.js

## Does it do things? ##

No.  Re-writing places or embedding LevelDB in gecko are somewhat harder.
