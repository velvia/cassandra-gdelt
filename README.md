## cassandra-gdelt

Just a simple test of different Cassandra layouts and their impact on the query / IO speed of the GDELT dataset.

NOTE: Some source code from Google's [FlatBuffers](http://google.github.io/flatbuffers/index.html) project is copied over, since they don't distribute jars and the files are very small.  All code in `src/main/java/com/google` is Copyright Google.

## NOTE: This is just PoC code.  Please see the [filo](http://github.com/velvia/filo) and [FiloDB](http://github.com/velvia/FiloDB) projects if you are interested in working code.

### Setup

GDELT dataset, 1979-1984, 4,037,539 records.  The original data is 55 columns but this one is truncated to the first 20 columns.

- Local MacBook Pro,  2.3GHz Core i7, 16GB RAM, SSD
- Cassandra 2.0.9, installed locally with one node
- Benchmark run using `sbt run`

### Benchmark Results - GdeltCaseClass

This is a simple layout with one primary key, so one physical row per record.  One would think that Cassandra could do a massive multiget_slice to save time on reading one column out of 20, but it doesn't save much time.

LZ4 disk compression is enabled.

Space taken up by all records: 900MB

| What                | Time     | Records/sec   |
| :------------------ | :------- | :------------ |
| Ingestion from CSV  | 380 s    | 10429 rec/s    |
| Read every column   | 203 s   | 19889 rec/s   |
| Read 1 col (monthYear) | 163 s | 24770 rec/s   |

(Average of two runs)

### Benchmark Results - GdeltCaseClass2

This is an improvement on GdeltCaseClass, with use of both a partition key which is a simple grouping of the primary keys, and a clustering key, to effect wide rows for faster linear reads from disk.  In addition, the names of the columns have been shortened to use up less disk space.

Space taken up by all records: 576MB

| What                | Time     | Records/sec   |
| :------------------ | :------- | :------------ |
| Ingestion from CSV  | 886 s    | 4558 rec/s    |
| Read every column   | 524 s    | 7700 rec/s   |
| Read 1 col (monthYear) | 493 s | 8173 rec/s   |

What is surprising is how much slower the use of the clustering key makes it.  Perhaps it is due to the need to store the clustering key along with the column name, although this is not bourne out by the space check.

### Columnar Layout (FlatBuffers, No Compression)

```sql
CREATE TABLE data (
  dataset text,
  version int,
  shard int,
  columnname text,
  rowid int,
  bytes blob,
  PRIMARY KEY ((dataset, version, shard), columnname, rowid)
)
```

This layout places values of the same column from different rows together, and also serializes multiple row values into one cell.  There is no compression yet.

Space taken up by records:  166MB .... !!!
(LZ4 Compressed SSTable size; uncompressed actual ByteBuffers are 931MB)

| What                | Time     | Records/sec   |
| :------------------ | :------- | :------------ |
| Ingestion from CSV  | 56.2 s   | 71886 rec/s   |
| Read every column   |  6.3 s   |  640k rec/s   |
| Read 1 col (monthYear) | 0.63 s | **6.43 million rec/s**   |

The speedup and compactness is shocking.
* On ingest - roughly 10x faster and 5x less space with no compression!  (No dictionary and columnar compression that is - but LZ4 C* disk compression.  The encoding has lots of 0's in it, which appears to LZ4 compress well.) 
* On reads - 32x to 83x faster for reads of all columns, and 260 - 800x faster for read of a single column
    - (Actually, 2/3rds of the single column read time is my first cut code for iterating over elements of the binary data structure, which probably can be significantly optimized)

Is this for real?  Gathering stats of the data being read shows that it is:
- `GdeltDataTableQuery` compiles stats which show that every column is being read, the # of shards, chunks, and bytes seem to all make sense.  Evidently LZ4 is compressing data to roughly 1/4 of the total size of all the bytebuffers.  This debunks the theory that perhaps not all the data is being read.
- For the monthYear col, exactly 4037539 elements are being read back, and a top K of the monthYear values matches exactly with values derived from the original source CSV file

Also, FlatBuffers leaves lots of zeroes in the binary output, so there is plenty of room for improvement, plus the code for parsing the binary FlatBuffers has not been optimized at all.... plus LZ4 and different C* side compression schemes and their effects too.

### Columnar Layout (FlatBuffers, Dictionary Compressed)

Same as above but with dictionary encoding enabled for about 75% of column chunks
(auto-detection with a 50% cardinality threshold for enabling dictionary encoding)

Space taken up by records:  57MB .... !!!   Holy S***
(LZ4 Compressed SSTable size; uncompressed actual ByteBuffers are 290.9 MB)

| What                | Time     | Records/sec   |
| :------------------ | :------- | :------------ |
| Ingestion from CSV  | 46.7 s   | 86376 rec/s   |
| Read every column   |  3.7 s   |  1.09 million rec/s   |
| Read 1 col (monthYear) | 0.63 s | **6.43 million rec/s**   |

So, dictionary encoding saves a huge amount of space, cutting the actual storage
space to one-third of the columnar one, both uncompressed and compressed.
(Interesting that the compression ratio seems constant)

### Cap'n Proto

This is not yet working due to bugs in capnproto-java.  Notes for the setup:
1. Build and install Cap'n Proto Schema Compiler
2. Clone and build the java repo using [these instructions](https://dwrensha.github.io/capnproto-java/index.html)
3. To build the schema, do something like

        capnp compile -o../../capnproto-java/capnpc-java -I../../capnproto-java/compiler/src/main/schema column_storage.capnp

### For Additional Investigation

Right now the above comparison is just for C*, LZ4 C* disk compression, using the Phantom client.  Much more testing and performance evaluation would be needed to compare against, for example, Parquet, and to isolate the effects of
- C* itself, and the disk compression scheme used
- Effects of the Phantom client
- FlatBuffers vs Capt'n Proto
