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
import com.opencsv.CSVReader
import com.websudos.phantom.Implicits._
import com.websudos.phantom.zookeeper.{SimpleCassandraConnector, DefaultCassandraManager}
import java.io.{BufferedReader, FileReader}
import org.joda.time.DateTime
import play.api.libs.iteratee.Iteratee
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

sealed class GdeltRecord2 extends GdeltRecordBase[GdeltRecord2] {
  object idPrefix extends StringColumn(this) with PartitionKey[String]
  object globalEventId extends StringColumn(this) with PrimaryKey[String]

  override def fromRow(row: Row): GdeltModel =
    super.fromRow(row).copy(globalEventId = globalEventId(row))
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
      // Wish didn't have to copy and paste this from GdeltCaseClass
      batch.add(insert.value(_.idPrefix, (record.globalEventId.toLong / 10000).toString)
                      .value(_.globalEventId, record.globalEventId)
                      .value(_.sqlDate,       record.sqlDate)
                      .value(_.monthYear,     record.monthYear)
                      .value(_.year,          record.year)
                      .value(_.fractionDate,  record.fractionDate)

                      .value(_.a1Code,        record.actor1.code)
                      .value(_.a1Name,        record.actor1.name)
                      .value(_.a1CountryCode, record.actor1.countryCode)
                      .value(_.a1KnownGroupCode, record.actor1.knownGroupCode)
                      .value(_.a1EthnicCode,  record.actor1.ethnicCode)
                      .value(_.a1Religion1Code, record.actor1.rel1Code)
                      .value(_.a1Religion2Code, record.actor1.rel2Code)
                      .value(_.a1Type1Code,   record.actor1.type1Code)
                      .value(_.a1Type2Code,   record.actor1.type2Code)
                      .value(_.a1Type3Code,   record.actor1.type3Code)

                      .value(_.a2Code,        record.actor2.code)
                      .value(_.a2Name,        record.actor2.name)
                      .value(_.a2CountryCode, record.actor2.countryCode)
                      .value(_.a2KnownGroupCode, record.actor2.knownGroupCode)
                      .value(_.a2EthnicCode,  record.actor2.ethnicCode)
                      .value(_.a2Religion1Code, record.actor2.rel1Code)
                      .value(_.a2Religion2Code, record.actor2.rel2Code)
                      .value(_.a2Type1Code,   record.actor2.type1Code)
                      .value(_.a2Type2Code,   record.actor2.type2Code)
                      .value(_.a2Type3Code,   record.actor2.type3Code)

                      .value(_.isRootEvent,   record.isRootEvent)
                      .value(_.eventCode,     record.eventCode)
                      .value(_.eventBaseCode, record.eventBaseCode)
                      .value(_.eventRootCode, record.eventRootCode)
                      .value(_.quadClass,     record.quadClass)
                      .value(_.goldsteinScale, record.goldsteinScale)
                      .value(_.numMentions,   record.numMentions)
                      .value(_.numSources,    record.numSources)
                      .value(_.numArticles,   record.numArticles)
                      .value(_.avgTone,       record.avgTone)
                      .value(_.dateAdded,     record.dateAdded)

                      .value(_.a1geoType,     record.actor1geo.geoType)
                      .value(_.a1fullName,    record.actor1geo.fullName)
                      .value(_.a1gcountryCode, record.actor1geo.countryCode)
                      .value(_.a1adm1Code,    record.actor1geo.adm1Code)
                      .value(_.a1lat,         record.actor1geo.lat)
                      .value(_.a1long,        record.actor1geo.long)
                      .value(_.a1featureID,   record.actor1geo.featureID)
                      .value(_.a1fullLocation, record.actor1geo.fullLocation)

                      .value(_.a2geoType,     record.actor2geo.geoType)
                      .value(_.a2fullName,    record.actor2geo.fullName)
                      .value(_.a2gcountryCode, record.actor2geo.countryCode)
                      .value(_.a2adm1Code,    record.actor2geo.adm1Code)
                      .value(_.a2lat,         record.actor2geo.lat)
                      .value(_.a2long,        record.actor2geo.long)
                      .value(_.a2featureID,   record.actor2geo.featureID)
                      .value(_.a2fullLocation, record.actor2geo.fullLocation)

                      .value(_.actgeoType,     record.actionGeo.geoType)
                      .value(_.actfullName,    record.actionGeo.fullName)
                      .value(_.actgcountryCode, record.actionGeo.countryCode)
                      .value(_.actadm1Code,    record.actionGeo.adm1Code)
                      .value(_.actlat,         record.actionGeo.lat)
                      .value(_.actlong,        record.actionGeo.long)
                      .value(_.actfeatureID,   record.actionGeo.featureID)
                      .value(_.actfullLocation, record.actionGeo.fullLocation)
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

  val reader = new CSVReader(new BufferedReader(new FileReader(gdeltFilePath)), '\t')
  val gdeltIter = new GdeltReader(reader)

  // Parse each line into a case class
  println("Ingesting, each dot equals 1000 records...")
  var recordCount = 0L
  val (_, elapsed) = GdeltRecord2.elapsed {
    gdeltIter.grouped(1000)
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