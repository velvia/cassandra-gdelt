## cassandra-gdelt

Just a simple test of different Cassandra layouts and their impact on the query / IO speed of the GDELT dataset.

### Setup

[GDELT dataset](http://data.gdeltproject.org/events/index.html), 1979-1984, 4,037,539 records, 57 columns* (There are actually 60 columns, but the last 3 columns are not populated for that date range)

(NOTE: the current code is designed only to parse data from the link above, which is TAB-delimited, and does NOT have a header row)

- Local MacBook Pro,  2.3GHz Core i7, 16GB RAM, SSD
- Cassandra 2.1.6, installed locally with one node using CCM
- Benchmark run using `sbt run`

NOTE: if you run this benchmark with C* 2.0.x, `GdeltCaseClass2` ingestion may crash C*.

Query and disk space benchmarks were run after a compaction cycle, with snapshots removed.

### Benchmark Results - GdeltCaseClass

This is a simple layout with one primary key, so one physical row per record.  One would think that Cassandra could do a massive multiget_slice to save time on reading one column out of 20, but it doesn't save much time.

LZ4 disk compression is enabled.

Space taken up by all records: 1.9GB

| What                | Time     | Records/sec   |
| :------------------ | :------- | :------------ |
| Ingestion from CSV  | 1927 s   | 2091 rec/s    |
| Read every column   | 505 s    | 7998 rec/s   |
| Read 1 col (monthYear) | 504 s | 8005 rec/s   |

### Benchmark Results - GdeltCaseClass2

This is an improvement on GdeltCaseClass, with use of both a partition key which is a simple grouping of the primary keys, and a clustering key, to effect wide rows for faster linear reads from disk.  In addition, the names of the columns have been shortened to use up less disk space.

Space taken up by all records: 2.2GB

| What                | Time     | Records/sec   |
| :------------------ | :------- | :------------ |
| Ingestion from CSV  | 3190 s   | 1300 rec/s    |
| Read every column   | 941 s    | 4417 rec/s   |
| Read 1 col (monthYear) | 707 s | 5880 rec/s   |

Oh no, what happened?  It seems it is slower to query, and it takes up more space on disk as well (logically, wide rows takes up more space uncompressed due to compound column names).  This is actually not entirely surprising, because reading only one column from such a layout, due to the way Cassandra lays out its data, essentially means having to scan all the data in a partition, and using clustering keys is inefficient because of how Cassandra prefixes clustering keys to the column names.  With the skinny layout, Cassandra is actually able to skip part of the row when reading.

However, let's say you were not scanning the whole table but instead had a WHERE clause to filter on your partition key.  In this case, this layout would be a huge win over the other one -- only data from one partition needs to be read.  Thus, pick a layout based on your needs.

### COMPACT STORAGE

COMPACT STORAGE is the old Cassandra 0.x - 1.x way of storing things - write your record as a blob of your choice (JSON, Protobuf, etc.) and Cassandra writes it as a single cell (physical row, column, blob).  It is not supposed to be supported going forward.  You need to parse the blob yourself, and the entire blob must be read.  If you use Protobuf or other binary format, then CQLSH won't show you the contents (in this case we are using a trick and joining the text fields together using a special \001 character, which shows up in a different color).  Writes are fast, but reads are still much slower than columnar layout.

Disk space: 260M

| What                | Time     | Records/sec   |
| :------------------ | :------- | :------------ |
| Ingestion from CSV  | 78.6 s   | 52877 rec/s    |
| Read every column   | 81.8 s    | 50850 rec/s   |
| Read 1 col (monthYear) | 81.8 s | 50850 rec/s   |

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

Space taken up by records:  266MB .... !!!
(LZ4 Compressed SSTable size; uncompressed actual ByteBuffers are 918MB)

| What                | Time     | Records/sec   |
| :------------------ | :------- | :------------ |
| Ingestion from CSV  | 93.0 s   | 43324 rec/s   |
| Read every column   |  8.6 s   |  468,980 rec/s   |
| Read 1 col (monthYear) | 0.23 s | **17.4 million rec/s**   |

The speedup and compactness is shocking.
* On ingest - roughly 20-35x faster and 7x less disk space (of course this is from CSV with essentially no primary key, append only, probably not realistic)
* On reads - 42x to 59x faster for reads of all columns, and 355 - 2190x faster for read of a single column
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

## Spark on Cassandra

The above can also be used to compare Spark on Cassandra reads.  First start up the spark shell, like this (we ensure there is only one thread for all tests for an even comparison):

    bin/spark-shell \
                  --packages com.datastax.spark:spark-cassandra-connector_2.10:1.4.0-M3 \
                  --conf spark.cassandra.connection.host=127.0.0.1 --conf spark.cassandra.input.split.size_in_mb=256 \
                  --conf spark.sql.shuffle.partitions=4 \
                  --driver-memory 5G --master "local[1]"

The split size is to ensure that the GDELT2 wide row table doesn't get too many splits when reading.  This is really tricky to configure by the way, as it is global but you will probably need different settings for different tables.

To load a Cassandra table into a Spark DataFrame, do something like this:

```scala
val df = sqlContext.read.format("org.apache.spark.sql.cassandra").
                    option("table", "gdelt2").
                    option("keyspace", "test").load
df.registerTempTable("gdelt")
```

Here are the queries used.  Query 1:

    df.select(count("numarticles")).show

Query 2:

    sqlContext.sql("SELECT a1name, AVG(avgtone) AS tone FROM gdelt GROUP BY a1name ORDER BY tone DESC").show

Query 3:

    sqlContext.sql("SELECT AVG(avgtone), MIN(avgtone), MAX(avgtone) FROM gdelt WHERE MonthYear = 198012").show

NOTE: the above are for querying the Cassandra CQL tables, in which the code shortens column names for performance and storage compactness reasons.  For Parquet and FiloDB, use the regular, capitalized column names (`Actor1Name` instead of `a1name`, `AvgTone` instead of `avgtone`, etc.)

TODO: instructions for querying the COMPACT STORAGE table

For testing Spark SQL cached tables, do the following:

    sqlContext.cacheTable("gdelt")
    sqlContext.sql("select avg(numarticles), avg(avgtone) from gdelt group by a1name").show

NOTE: For any DataFrame DSL (Scala) queries, make sure to get back a new DataFrame after the `cacheTable` operation, like this: `val df1 = sqlContext.table("gdelt")`.  The cached DataFrame reference is not the same as the original!

For FiloDB, the table is loaded just a tiny bit differently:

    df = sqlContext.read.format("filodb.spark").option("dataset", "gdelt").option("splits_per_node", "1").load

For Parquet, it is even more straightforward:

    df = sqlContext.load("/path/to/my/1979-1984.parquet")

For comparisons, I generated the Parquet file from one of the above Cassandra tables (not the COMPACT STORAGE one) by saving it out, like this:

    df.save("/path/to/my/1979-1984.parquet")
