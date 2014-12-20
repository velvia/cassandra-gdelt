/*
 * Represents each Gdelt record as a Scala case class.
 * One physical row in C* per Gdelt record, with separate columns for each column.
 *
 * Note: This is not a workable model for a large number of tables/datasets, or
 * for a large number of columns.
 * - Cassandra uses memory for each table, and most people use very few tables;
 * - Defining all the columns is really tedious, as you can see here
 */

import com.datastax.driver.core.Row
import com.github.marklister.collections.io._
import com.websudos.phantom.Implicits._
import com.websudos.phantom.zookeeper.{SimpleCassandraConnector, DefaultCassandraManager}
import java.io.{BufferedReader, FileReader}
import org.joda.time.DateTime
import play.api.libs.iteratee.Iteratee
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

// I think I can only bear to fill in 20 columns right now.  :-p
case class GdeltModel(globalEventId: String,
                      sqlDate: DateTime,
                      monthYear: Option[Int],
                      year: Option[Int],
                      fractionDate: Option[Double],    // fractional year, like 1984.989
                      actor1Code: Option[String],
                      actor1Name: Option[String],
                      actor1CountryCode: Option[String],
                      actor1KnownGroupCode: Option[String],
                      actor1EthnicCode: Option[String],
                      actor1Religion1Code: Option[String],
                      actor1Religion2Code: Option[String],
                      actor1Type1Code: Option[String],
                      actor1Type2Code: Option[String],
                      actor1Type3Code: Option[String],
                      actor2Code: Option[String],
                      actor2Name: Option[String],
                      actor2CountryCode: Option[String],
                      actor2KnownGroupCode: Option[String],
                      actor2EthnicCode: Option[String]
                      )

sealed class GdeltRecord extends CassandraTable[GdeltRecord, GdeltModel] {
  object globalEventId extends StringColumn(this) with PartitionKey[String]
  object sqlDate extends DateTimeColumn(this)
  object monthYear extends OptionalIntColumn(this)
  object year extends OptionalIntColumn(this)
  object fractionDate extends OptionalDoubleColumn(this)
  object actor1Code extends OptionalStringColumn(this)
  object actor1Name extends OptionalStringColumn(this)
  object actor1CountryCode extends OptionalStringColumn(this)
  object actor1KnownGroupCode extends OptionalStringColumn(this)
  object actor1EthnicCode extends OptionalStringColumn(this)
  object actor1Religion1Code extends OptionalStringColumn(this)
  object actor1Religion2Code extends OptionalStringColumn(this)
  object actor1Type1Code extends OptionalStringColumn(this)
  object actor1Type2Code extends OptionalStringColumn(this)
  object actor1Type3Code extends OptionalStringColumn(this)
  object actor2Code extends OptionalStringColumn(this)
  object actor2Name extends OptionalStringColumn(this)
  object actor2CountryCode extends OptionalStringColumn(this)
  object actor2KnownGroupCode extends OptionalStringColumn(this)
  object actor2EthnicCode extends OptionalStringColumn(this)

  override def fromRow(row: Row): GdeltModel =
    GdeltModel(globalEventId(row),
               sqlDate(row),
               monthYear(row),
               year(row),
               fractionDate(row),
               actor1Code(row),
               actor1Name(row),
               actor1CountryCode(row),
               actor1KnownGroupCode(row),
               actor1EthnicCode(row),
               actor1Religion1Code(row),
               actor1Religion2Code(row),
               actor1Type1Code(row),
               actor1Type2Code(row),
               actor1Type3Code(row),
               actor2Code(row),
               actor2Name(row),
               actor2CountryCode(row),
               actor2KnownGroupCode(row),
               actor2EthnicCode(row))
}

// NOTE: default CQL port is 9042
trait LocalConnector extends SimpleCassandraConnector {
  val keySpace = "test"
}

object GdeltRecord extends GdeltRecord with LocalConnector {
  override val tableName = "gdelt"

  def insertRecords(records: Seq[GdeltModel]): Future[ResultSet] = {
    // NOTE: Apparently this is an anti-pattern, because a BATCH statement
    // forces a single coordinator to handle everything, whereas if they were
    // individual writes, they could go to the right node, skipping a hop.
    // However this test is done on localhost, so it probably doesn't matter as much.
    val batch = UnloggedBatchStatement()
    records.foreach { record =>
      batch.add(insert.value(_.globalEventId, record.globalEventId)
                      .value(_.sqlDate,       record.sqlDate)
                      .value(_.monthYear,     record.monthYear)
                      .value(_.year,          record.year)
                      .value(_.fractionDate,  record.fractionDate)
                      .value(_.actor1Code,    record.actor1Code)
                      .value(_.actor1Name,    record.actor1Name)
                      .value(_.actor1CountryCode, record.actor1CountryCode)
                      .value(_.actor1KnownGroupCode, record.actor1KnownGroupCode)
                      .value(_.actor1EthnicCode, record.actor1EthnicCode)
                      .value(_.actor1Religion1Code, record.actor1Religion1Code)
                      .value(_.actor1Religion2Code, record.actor1Religion2Code)
                      .value(_.actor1Type1Code, record.actor1Type1Code)
                      .value(_.actor1Type2Code, record.actor1Type2Code)
                      .value(_.actor1Type3Code, record.actor1Type3Code)
                      .value(_.actor2Code,    record.actor2Code)
                      .value(_.actor2Name,    record.actor2Name)
                      .value(_.actor2CountryCode, record.actor2CountryCode)
                      .value(_.actor2KnownGroupCode, record.actor2KnownGroupCode)
                      .value(_.actor2EthnicCode, record.actor2EthnicCode)
                )
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
 */
object GdeltCaseClassSetup extends App with LocalConnector {
  println("Setting up keyspace and table...")
  println(Await.result(GdeltRecord.create.future(), 5000 millis))
  println("...done")
}

/**
 * Run this to import the rows into local Casandra.
 */
object GdeltCaseClassImporter extends App with LocalConnector {
  import CsvParsingUtils._

  val gdeltFilePath = Try(args(0)).getOrElse {
    println("Usage: pass the gdelt CSV file as the first arg")
    sys.exit(0)
  }

  val reader = new BufferedReader(new FileReader(gdeltFilePath))
  val lineIter = CsvParser[String, DateTime, Option[Int], Option[Int], Option[Double],
                           Option[String], Option[String], Option[String], Option[String], Option[String],
                           Option[String], Option[String], Option[String], Option[String], Option[String],
                           Option[String], Option[String], Option[String], Option[String], Option[String]].
                   iterator(reader, hasHeader = true)

  def toGdeltCaseClass = (GdeltModel.apply _).tupled

  // Parse each line into a case class
  println("Ingesting, each dot equals 1000 records...")
  var recordCount = 0L
  val (_, elapsed) = GdeltRecord.elapsed {
    lineIter.map(toGdeltCaseClass)
            .grouped(1000)
            .foreach { records =>
              recordCount += records.length
              Await.result(GdeltRecord.insertRecords(records), 10 seconds)
              print(".")
            }
  }
  println(s"Done in ${elapsed} secs, ${recordCount / elapsed} records/sec")
}

/**
 * Run this to time queries against the imported records
 */
object GdeltCaseClassQuery extends App with LocalConnector {
  println("Querying every column (full export)...")
  val (result, elapsed) = GdeltRecord.elapsed {
    val f = GdeltRecord.select.
              fetchEnumerator run (Iteratee.fold(0) { (acc, elt: Any) => acc + 1 })
    Await.result(f, 5000 seconds)
  }
  println(s".... got count of $result in $elapsed seconds")

  println("Querying just monthYear column out of 20...")
  val (result2, elapsed2) = GdeltRecord.elapsed {
    val f = GdeltRecord.select(_.monthYear).
              fetchEnumerator run (Iteratee.fold(0) { (acc, elt: Any) => acc + 1 })
    Await.result(f, 5000 seconds)
  }
  println(s".... got count of $result2 in $elapsed2 seconds")
}