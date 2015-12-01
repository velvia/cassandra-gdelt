/**
 * A wide row layout like GdeltCaseClass2, but using COMPACT STORAGE and one text value for all fields,
 * as one would do in Cassandra 0.x - 1.x.
 */

import com.datastax.driver.core.Row
import com.opencsv.CSVReader
import com.websudos.phantom.Implicits._
import com.websudos.phantom.zookeeper.{SimpleCassandraConnector, DefaultCassandraManager}
import java.io.{BufferedReader, FileReader}
import org.joda.time.DateTime
import play.api.libs.iteratee.Iteratee
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try


sealed class GdeltCompact extends CassandraTable[GdeltCompact, Array[String]] {
  object idPrefix extends StringColumn(this) with PartitionKey[String]
  object globalEventId extends StringColumn(this) with PrimaryKey[String]
  object record extends StringColumn(this)

  override def fromRow(row: Row): Array[String] =
    record(row).split("\001")
}

object GdeltCompact extends GdeltCompact with LocalConnector {
  override val tableName = "gdelt_compact"

  def insertRecords(records: Seq[Array[String]]): Future[ResultSet] = {
    val batch = records.foldLeft(UnloggedBatchStatement()) { case (batch, record) =>
      val globalId = record(0)
      batch.add(insert.value(_.idPrefix,      (globalId.toLong / 10000).toString)
                      .value(_.globalEventId, globalId)
                      .value(_.record,        record.mkString("\001")))
    }
    batch.future()
  }

  def elapsed[A](f: => A): (A, Double) = {
    val startTime = System.currentTimeMillis
    val ret = f
    val elapsed = (System.currentTimeMillis - startTime) / 1000.0
    (ret, elapsed)
  }
}

/**
 * Run this to set up the test keyspace and table on localhost/9042.
 * NOTE: you need to manually use cqlsh to change the table to COMPACT STORAGE since
 * in this version of Phantom it is not available.
 * drop table gdelt_compact; then do a create table with same cqlsh description but WITH COMPACT STORAGE
 */
object GdeltCompactSetup extends App with LocalConnector {
  println("Setting up keyspace and table...")
  println(Await.result(GdeltCompact.create.future(), 5000 millis))
  println("...done")
}

/**
 * Run this to import the rows into local Casandra.
 *
 * NOTE: expects a TSV (tab-separated values) file.
 */
object GdeltCompactImporter extends App with LocalConnector {
  import CsvParsingUtils._
  import collection.JavaConverters._

  val gdeltFilePath = Try(args(0)).getOrElse {
    println("Usage: pass the gdelt CSV file as the first arg")
    sys.exit(0)
  }

  val reader = new CSVReader(new BufferedReader(new FileReader(gdeltFilePath)), '\t')

  // Parse each line into a case class
  println("Ingesting, each dot equals 1000 records...")
  var recordCount = 0L
  val (_, elapsed) = GdeltRecord2.elapsed {
    reader.iterator.asScala.grouped(1000)
            .foreach { records =>
              recordCount += records.length
              Await.result(GdeltCompact.insertRecords(records), 10 seconds)
              print(".")
            }
  }
  println(s"Done in ${elapsed} secs, ${recordCount / elapsed} records/sec")
}

/**
 * Run this to time queries against the imported records
 */
object GdeltCompactQuery extends App with LocalConnector {
  println("Querying every column (full export)...")
  val (result, elapsed) = GdeltCompact.elapsed {
    val f = GdeltCompact.select.
              fetchEnumerator run (Iteratee.fold(0) { (acc, elt: Any) => acc + 1 })
    Await.result(f, 5000 seconds)
  }
  println(s".... got count of $result in $elapsed seconds")

  // Just to see if Cassandra can optimize reads of only partition keys
  println("Querying only (idPrefix) ...")
  val (result2, elapsed2) = GdeltCompact.elapsed {
    val f = GdeltCompact.select(_.idPrefix).
              fetchEnumerator run (Iteratee.fold(0) { (acc, elt: Any) => acc + 1 })
    Await.result(f, 5000 seconds)
  }
  println(s".... got count of $result2 in $elapsed2 seconds")
}

