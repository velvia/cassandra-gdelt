## cassandra-gdelt

Just a simple test of different Cassandra layouts and their impact on the query / IO speed of the GDELT dataset.

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