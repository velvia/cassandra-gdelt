/**
 * Represents the table model (and initialization code) for a columnar, multi dataset
 * multi versioned storage engine.  Also contains the low-level I/O methods.
 */

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits._
import com.websudos.phantom.zookeeper.{SimpleCassandraConnector, DefaultCassandraManager}
import java.nio.ByteBuffer
import org.joda.time.DateTime
import play.api.libs.iteratee.Enumerator
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

// Note that each "rowId" will probably contain multiple rows, because storing adjacent row
// data is much more efficient (less column headers) and allows for many types of compression
// possibilities
case class DataTableModel(dataset: String,
                          version: Int,
                          shard: Int,
                          columnName: String,
                          rowId: Int,
                          bytes: ByteBuffer)

sealed class DataTableRecord extends CassandraTable[DataTableRecord, DataTableModel] {
  object dataset extends StringColumn(this) with PartitionKey[String]
  object version extends IntColumn(this) with PartitionKey[Int]
  object shard extends IntColumn(this) with PartitionKey[Int]
  object columnName extends StringColumn(this) with PrimaryKey[String]
  object rowId extends IntColumn(this) with PrimaryKey[Int]
  object bytes extends BlobColumn(this)

  override def fromRow(row: Row): DataTableModel =
    DataTableModel(dataset(row),
                   version(row),
                   shard(row),
                   columnName(row),
                   rowId(row),
                   bytes(row))
}

object DataTableRecord extends DataTableRecord with LocalConnector {
  override val tableName = "data"

  /**
   * Inserts one "rowId" of data from different columns.  Note that this may in fact contain data from multiple
   * rows.
   * @param dataset the name of the dataset to write to
   * @param version the version of the data to write to
   * @param shard the shard number
   * @param rowId the integer starting rowId of the batch of data.
   * @param columnsBytes a Map of column names to the bytes to write for that column
   * @returns a Future[ResultSet]
   */
  def insertOneRow(dataset: String, version: Int, shard: Int, rowId: Int,
                   columnsBytes: Map[String, ByteBuffer]): Future[ResultSet] = {
    // NOTE: This is actually a good use of Unlogged Batch, because all of the inserts
    // are to the same partition key, so they will get collapsed down into one insert
    // for efficiency.
    val batch = UnloggedBatchStatement()
    columnsBytes.foreach { case (columnName, bytes) =>
      // Sucks, it seems that reusing a partially prepared query doesn't work.
      // Issue filed: https://github.com/websudos/phantom/issues/166
      batch.add(insert.value(_.dataset, dataset)
                      .value(_.version, version)
                      .value(_.shard,   shard)
                      .value(_.rowId,   rowId)
                      .value(_.columnName, columnName)
                      .value(_.bytes, bytes))
    }
    batch.future()
  }

  // The tuple type returned by the low level readColumns API in the Enumerator
  type ColRowBytes = (String, Int, ByteBuffer)

  /**
   * Reads columnar data back for a particular dataset/version/shard, for select columns.
   * Reads back all rows in a given shard, in column order
   * (col1 rowId1, col1 rowId2, col1 rowId3, col2 rowId1, etc.)
   * The lowest level read API.
   * @param dataset the name of the dataset to read from
   * @param version the version of the data to read from
   * @param shard the shard number to read from
   * @param columns the set of column names to read from
   * @returns an Enumerator[ColRowBytes]
   *
   * TODO: how to restrict the range of rows to read?
   */
  def readSelectColumns(dataset: String, version: Int, shard: Int, columns: List[String]):
      Enumerator[ColRowBytes] = {
    // NOTE: Cassandra does not allow using IN operator on one part of the clustering key.
    // Grrrr.
    // For now just make it work for one column, but TODO to read from multiple columns
    // using multiple parallel read commands, which can be ZIPped together or JOINed together.
    require(columns.length == 1, "Only works with one column for now")
    select(_.columnName, _.rowId, _.bytes).where(_.dataset eqs dataset)
                                          .and(_.version eqs version)
                                          .and(_.shard eqs shard)
                                          // .and(_.columnName in columns)
                                          .and(_.columnName eqs columns.head)
                                          .fetchEnumerator
  }

  /**
   * Reads columnar data back for a particular dataset/version/shard, for all columns.
   * Reads back all rows in a given shard, in column order
   * (col1 rowId1, col1 rowId2, col1 rowId3, col2 rowId1, etc.)
   * The lowest level read API.
   * @param dataset the name of the dataset to read from
   * @param version the version of the data to read from
   * @param shard the shard number to read from
   * @returns an Enumerator[ColRowBytes]
   */
  def readAllColumns(dataset: String, version: Int, shard: Int): Enumerator[ColRowBytes] = {
    select(_.columnName, _.rowId, _.bytes).where(_.dataset eqs dataset)
                                          .and(_.version eqs version)
                                          .and(_.shard eqs shard)
                                          .fetchEnumerator
  }
}

/**
 * Run this to set up the test keyspace and data table on localhost/9042.
 * TODO: move to another file and create all the tables at once
 */
object DataTableSetup extends App with LocalConnector {
  println("Setting up keyspace and data table...")
  println(Await.result(DataTableRecord.create.future(), 5000 millis))
  println("...done")
}
