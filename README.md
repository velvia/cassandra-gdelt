## cassandra-gdelt

Just a simple test of different Cassandra layouts and their impact on the query / IO speed of the GDELT dataset.

### Setup

GDELT dataset, 1979-1984, 4,037,539 records, 60 columns.

- Local MacBook Pro,  2.3GHz Core i7, 16GB RAM, SSD
- Cassandra 2.1.6, installed locally with one node using CCM
- Benchmark run using `sbt run`

NOTE: if you run this benchmark with C* 2.0.x, `GdeltCaseClass2` ingestion may crash C*.

Query and disk space benchmarks were run after a compaction cycle, with snapshots removed.

### Benchmark Results - GdeltCaseClass

This is a simple layout with one primary key, so one physical row per record.  One would think that Cassandra could do a massive multiget_slice to save time on reading one column out of 20, but it doesn't save much time.

LZ4 disk compression is enabled.

Space taken up by all records: 2.7GB

| What                | Time     | Records/sec   |
| :------------------ | :------- | :------------ |
| Ingestion from CSV  | 1927 s   | 2091 rec/s    |
| Read every column   | 505 s    | 7998 rec/s   |
| Read 1 col (monthYear) | 504 s | 8005 rec/s   |

### Benchmark Results - GdeltCaseClass2

This is an improvement on GdeltCaseClass, with use of both a partition key which is a simple grouping of the primary keys, and a clustering key, to effect wide rows for faster linear reads from disk.  In addition, the names of the columns have been shortened to use up less disk space.

Space taken up by all records: 1.6GB

| What                | Time     | Records/sec   |
| :------------------ | :------- | :------------ |
| Ingestion from CSV  | 3897 s   | 1034 rec/s    |
| Read every column   | 365 s    | 11044 rec/s   |
| Read 1 col (monthYear) | 351 s | 11474 rec/s   |

This does improve the read times, and we can see that somehow it takes up less space on disk as well - probably the result of compression (though logically, wide rows takes up more space uncompressed due to compound column names).

### Columnar Layout

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

This layout places values of the same column from different rows together, and also serializes multiple row values into one cell.

Dictionary encoding enabled for about 75% of column chunks
(auto-detection with a 50% cardinality threshold for enabling dictionary encoding)

Space taken up by records:  337MB .... !!!
(LZ4 Compressed SSTable size; uncompressed actual ByteBuffers are 918MB)

| What                | Time     | Records/sec   |
| :------------------ | :------- | :------------ |
| Ingestion from CSV  | 93.0 s   | 43324 rec/s   |
| Read every column   |  8.6 s   |  468,980 rec/s   |
| Read 1 col (monthYear) | 0.23 s | **17.4 million rec/s**   |

The speedup and compactness is shocking.
* On ingest - roughly 20-40x faster and 5x less disk space (of course this is from CSV with essentially no primary key, append only, probably not realistic)
* On reads - 42x to 59x faster for reads of all columns, and 1526 - 2190x faster for read of a single column
    - Granted, the speedup is for parsing an integer column, which is the most compact and benefits the most from efficient I/O; parsing a string column will not be quite as fast (though dictionary-encoded columns are very fast in deserialization)
* Dictionary encoding saves a huge amount of space, cutting the actual storage
space to one-third of the columnar one, both uncompressed and compressed. (sorry results without dictionary encoding are not available since a code change is needed to test that out)
(Interesting that the compression ratio seems constant)

Is this for real?  Gathering stats of the data being read shows that it is:
- `GdeltDataTableQuery` compiles stats which show that every column is being read, the # of shards, chunks, and bytes seem to all make sense.  Evidently LZ4 is compressing data to roughly 1/4 of the total size of all the bytebuffers.  This debunks the theory that perhaps not all the data is being read.
- For the monthYear col, exactly 4037539 elements are being read back, and a top K of the monthYear values matches exactly with values derived from the original source CSV file

Also, FlatBuffers leaves lots of zeroes in the binary output, so there is plenty of room for improvement, plus the code for parsing the binary FlatBuffers has not been optimized at all.... plus LZ4 and different C* side compression schemes and their effects too.

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

Another good dataset to test against is NYC Taxi Trip data: http://www.andresmh.com/nyctaxitrips/.   There are two helper scripts: `split_csv.sh` helps break up a big CSV into smaller CSVs with header intact, and `socrata_pointify.sh` adds a point column from lat and long columns.
