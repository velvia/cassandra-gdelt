/**
 * Represents the table model (and initialization code) for a columnar, multi dataset
 * multi versioned storage engine.  Also contains the low-level I/O methods.
 */

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits._
import com.websudos.phantom.zookeeper.{SimpleCassandraConnector, DefaultCassandraManager}
import java.nio.ByteBuffer
import org.joda.time.DateTime
import play.api.libs.iteratee.Iteratee
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

  // TODO: how to restrict the range of rows to read?
  def readColumns(dataset: String, version: Int, shard: Int, columns: Seq[String]): Future[Int] = ???
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
