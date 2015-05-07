/*
 * Represents each Gdelt record as a Scala case class.
 * Sharded so the clustering key is the globalEventId, and the partition key is the first
 * part of the globalEventId, so there are much less rows for horizontal efficiency.
 * Also the names of the columns are shortened to reduce space used for each column key.
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
case class GdeltModel2(globalEventId: String,
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

sealed class GdeltRecord2 extends CassandraTable[GdeltRecord2, GdeltModel] {
  object idPrefix extends StringColumn(this) with PartitionKey[String]
  object globalEventId extends StringColumn(this) with PrimaryKey[String]
  object sqlDate extends DateTimeColumn(this)
  object monthYear extends OptionalIntColumn(this)
  object year extends OptionalIntColumn(this)
  object fractionDate extends OptionalDoubleColumn(this)
  object a1C extends OptionalStringColumn(this)
  object a1N extends OptionalStringColumn(this)
  object a1CC extends OptionalStringColumn(this)
  object a1KG extends OptionalStringColumn(this)
  object a1EC extends OptionalStringColumn(this)
  object a1R1C extends OptionalStringColumn(this)
  object a1R2C extends OptionalStringColumn(this)
  object a1T1 extends OptionalStringColumn(this)
  object a1T2 extends OptionalStringColumn(this)
  object a1T3 extends OptionalStringColumn(this)
  object actor2Code extends OptionalStringColumn(this)
  object actor2Name extends OptionalStringColumn(this)
  object a2CC extends OptionalStringColumn(this)
  object a2KG extends OptionalStringColumn(this)
  object a2EC extends OptionalStringColumn(this)

  override def fromRow(row: Row): GdeltModel =
    GdeltModel(globalEventId(row),
               sqlDate(row),
               monthYear(row),
               year(row),
               fractionDate(row),
               a1C(row),
               a1N(row),
               a1CC(row),
               a1KG(row),
               a1EC(row),
               a1R1C(row),
               a1R2C(row),
               a1T1(row),
               a1T2(row),
               a1T3(row),
               actor2Code(row),
               actor2Name(row),
               a2CC(row),
               a2KG(row),
               a2EC(row))
}

object GdeltRecord2 extends GdeltRecord2 with LocalConnector {
  override val tableName = "gdelt2"

  def insertRecords(records: Seq[GdeltModel]): Future[ResultSet] = {
    // NOTE: Apparently this is an anti-pattern, because a BATCH statement
    // forces a single coordinator to handle everything, whereas if they were
    // individual writes, they could go to the right node, skipping a hop.
    // However this test is done on localhost, so it probably doesn't matter as much.
    val batch = UnloggedBatchStatement()
    records.foreach { record =>
      batch.add(insert.value(_.idPrefix, (record.globalEventId.toLong / 10000).toString)
                      .value(_.globalEventId, record.globalEventId)
                      .value(_.sqlDate,       record.sqlDate)
                      .value(_.monthYear,     record.monthYear)
                      .value(_.year,          record.year)
                      .value(_.fractionDate,  record.fractionDate)
                      .value(_.a1C,           record.actor1Code)
                      .value(_.a1N,           record.actor1Name)
                      .value(_.a1CC,          record.actor1CountryCode)
                      .value(_.a1KG,          record.actor1KnownGroupCode)
                      .value(_.a1EC,          record.actor1EthnicCode)
                      .value(_.a1R1C,         record.actor1Religion1Code)
                      .value(_.a1R2C,         record.actor1Religion2Code)
                      .value(_.a1T1,          record.actor1Type1Code)
                      .value(_.a1T2,          record.actor1Type2Code)
                      .value(_.a1T3,          record.actor1Type3Code)
                      .value(_.actor2Code,    record.actor2Code)
                      .value(_.actor2Name,    record.actor2Name)
                      .value(_.a2CC,          record.actor2CountryCode)
                      .value(_.a2KG,          record.actor2KnownGroupCode)
                      .value(_.a2EC,          record.actor2EthnicCode)
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
object GdeltCaseClass2Setup extends App with LocalConnector {
  println("Setting up keyspace and table...")
  println(Await.result(GdeltRecord2.create.future(), 5000 millis))
  println("...done")
}

/**
 * Run this to import the rows into local Casandra.
 */
object GdeltCaseClass2Importer extends App with LocalConnector {
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
  val (_, elapsed) = GdeltRecord2.elapsed {
    lineIter.map(toGdeltCaseClass)
            .grouped(1000)
            .foreach { records =>
              recordCount += records.length
              Await.result(GdeltRecord2.insertRecords(records), 10 seconds)
              print(".")
            }
  }
  println(s"Done in ${elapsed} secs, ${recordCount / elapsed} records/sec")
}

/**
 * Run this to time queries against the imported records
 */
object GdeltCaseClass2Query extends App with LocalConnector {
  println("Querying every column (full export)...")
  val (result, elapsed) = GdeltRecord2.elapsed {
    val f = GdeltRecord2.select.
              fetchEnumerator run (Iteratee.fold(0) { (acc, elt: Any) => acc + 1 })
    Await.result(f, 5000 seconds)
  }
  println(s".... got count of $result in $elapsed seconds")

  println("Querying just monthYear column out of 20...")
  val (result2, elapsed2) = GdeltRecord2.elapsed {
    val f = GdeltRecord2.select(_.monthYear).
              fetchEnumerator run (Iteratee.fold(0) { (acc, elt: Any) => acc + 1 })
    Await.result(f, 5000 seconds)
  }
  println(s".... got count of $result2 in $elapsed2 seconds")
}