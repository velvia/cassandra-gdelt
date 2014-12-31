## cassandra-gdelt

Just a simple test of different Cassandra layouts and their impact on the query / IO speed of the GDELT dataset.

NOTE: Some source code from Google's [FlatBuffers](http://google.github.io/flatbuffers/index.html) project is copied over, since they don't distribute jars and the files are very small.  All code in `src/main/java/com/google` is Copyright Google.

### Setup

GDELT dataset, 1979-1984, 4,037,539 records.  The original data is 55 columns but this one is truncated to the first 20 columns.

- Local MacBook Pro,  2.3GHz Core i7, 16GB RAM, SSD
- Cassandra 2.0.9, installed locally with one node
- Benchmark run using `sbt run`

### Benchmark Results - GdeltCaseClass

This is a simple layout with one primary key, so one physical row per record.  One would think that Cassandra could do a massive multiget_slice to save time on reading one column out of 20, but it doesn't save much time.

Space taken up by all records: 1.8GB

| What                | Time     | Records/sec   |
| :------------------ | :------- | :------------ |
| Ingestion from CSV  | 503 s    | 8020 rec/s    |
| Read every column   | 332 s    | 12159 rec/s   |
| Read 1 col (monthYear) | 276 s | 14643 rec/s   |

(Average of two runs)

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

| What                | Time     | Records/sec   |
| :------------------ | :------- | :------------ |
| Ingestion from CSV  | 56.2 s   | 71886 rec/s   |
| Read every column   |  6.3 s   |  640k rec/s   |
| Read 1 col (monthYear) | 0.20 s | **20 million rec/s**   |

The speedup and compactness is shocking.
* On ingest - roughly 10x faster and 10x less space with no compression!  (No dictionary and columnar compression that is - but LZ4 C* disk compression.  The encoding has lots of 0's in it, which appears to LZ4 compress well.) 
* On reads - more than 50x faster for reads of all columns, and over 1000x faster for read of a single column

I'll have to run some verification tests to make sure that this is not BS, but a casual inspection of the data using CqlSH seems like it's legit.  Also, FlatBuffers leaves lots of zeroes in the binary output, so there is plenty of room for improvement.
- (update 1) `GdeltDataTableQuery` compiles stats which show that every column is being read, the # of shards, chunks, and bytes seem to all make sense.  Evidently LZ4 is compressing data to roughly 1/4 of the total size of all the bytebuffers.
