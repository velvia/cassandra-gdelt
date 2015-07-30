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
import com.opencsv.CSVReader
import com.websudos.phantom.Implicits._
import com.websudos.phantom.zookeeper.{SimpleCassandraConnector, DefaultCassandraManager}
import java.io.{BufferedReader, FileReader}
import org.joda.time.DateTime
import play.api.libs.iteratee.Iteratee
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

case class GdeltModel(globalEventId: String,
                      sqlDate: DateTime,
                      monthYear: Option[Int],
                      year: Option[Int],
                      fractionDate: Option[Double],    // fractional year, like 1984.989
                      actor1: ActorInfo,
                      actor2: ActorInfo,
                      isRootEvent: Int,
                      eventCode: Option[String],
                      eventBaseCode: Option[String],
                      eventRootCode: Option[String],
                      quadClass: Option[Int],
                      goldsteinScale: Double,
                      numMentions: Int,
                      numSources: Int,
                      numArticles: Int,
                      avgTone: Double,
                      dateAdded: Option[String],
                      actor1geo: GeoInfo,
                      actor2geo: GeoInfo,
                      actionGeo: GeoInfo
                      )

case class ActorInfo(code: Option[String],
                     name: Option[String],
                     countryCode: Option[String],
                     knownGroupCode: Option[String],
                     ethnicCode: Option[String],
                     rel1Code: Option[String],
                     rel2Code: Option[String],
                     type1Code: Option[String],
                     type2Code: Option[String],
                     type3Code: Option[String])

case class GeoInfo(geoType: Option[String],
                   fullName: Option[String],
                   countryCode: Option[String],
                   adm1Code: Option[String],
                   lat: Double,
                   long: Double,
                   featureID: Option[String],
                   fullLocation: Option[String])

trait GdeltRecordBase[G <: CassandraTable[G, GdeltModel]] extends CassandraTable[G, GdeltModel] {
  // Cols 0-4
  object sqlDate extends DateTimeColumn(this)
  object monthYear extends OptionalIntColumn(this)
  object year extends OptionalIntColumn(this)
  object fractionDate extends OptionalDoubleColumn(this)

  // Cols 5-14
  object a1Code extends OptionalStringColumn(this)
  object a1Name extends OptionalStringColumn(this)
  object a1CountryCode extends OptionalStringColumn(this)
  object a1KnownGroupCode extends OptionalStringColumn(this)
  object a1EthnicCode extends OptionalStringColumn(this)
  object a1Religion1Code extends OptionalStringColumn(this)
  object a1Religion2Code extends OptionalStringColumn(this)
  object a1Type1Code extends OptionalStringColumn(this)
  object a1Type2Code extends OptionalStringColumn(this)
  object a1Type3Code extends OptionalStringColumn(this)

  // Cols 15-24
  object a2Code extends OptionalStringColumn(this)
  object a2Name extends OptionalStringColumn(this)
  object a2CountryCode extends OptionalStringColumn(this)
  object a2KnownGroupCode extends OptionalStringColumn(this)
  object a2EthnicCode extends OptionalStringColumn(this)
  object a2Religion1Code extends OptionalStringColumn(this)
  object a2Religion2Code extends OptionalStringColumn(this)
  object a2Type1Code extends OptionalStringColumn(this)
  object a2Type2Code extends OptionalStringColumn(this)
  object a2Type3Code extends OptionalStringColumn(this)

  // Cols 25-34
  object isRootEvent extends IntColumn(this)
  object eventCode extends OptionalStringColumn(this)
  object eventBaseCode extends OptionalStringColumn(this)
  object eventRootCode extends OptionalStringColumn(this)
  object quadClass extends OptionalIntColumn(this)
  object goldsteinScale extends DoubleColumn(this)
  object numMentions extends IntColumn(this)
  object numSources extends IntColumn(this)
  object numArticles extends IntColumn(this)
  object avgTone extends DoubleColumn(this)

  // Cols 35-41
  object a1geoType extends OptionalStringColumn(this)
  object a1fullName extends OptionalStringColumn(this)
  object a1gcountryCode extends OptionalStringColumn(this)
  object a1adm1Code extends OptionalStringColumn(this)
  object a1lat extends DoubleColumn(this)
  object a1long extends DoubleColumn(this)
  object a1featureID extends OptionalStringColumn(this)

  // Cols 42-48
  object a2geoType extends OptionalStringColumn(this)
  object a2fullName extends OptionalStringColumn(this)
  object a2gcountryCode extends OptionalStringColumn(this)
  object a2adm1Code extends OptionalStringColumn(this)
  object a2lat extends DoubleColumn(this)
  object a2long extends DoubleColumn(this)
  object a2featureID extends OptionalStringColumn(this)

  // Cols 49-55
  object actgeoType extends OptionalStringColumn(this)
  object actfullName extends OptionalStringColumn(this)
  object actgcountryCode extends OptionalStringColumn(this)
  object actadm1Code extends OptionalStringColumn(this)
  object actlat extends DoubleColumn(this)
  object actlong extends DoubleColumn(this)
  object actfeatureID extends OptionalStringColumn(this)

  // Cols 56-59
  object dateAdded extends OptionalStringColumn(this)
  object a1fullLocation extends OptionalStringColumn(this)
  object a2fullLocation extends OptionalStringColumn(this)
  object actfullLocation extends OptionalStringColumn(this)

  override def fromRow(row: Row): GdeltModel =
    GdeltModel("",   // to be filled in by extension classes
               sqlDate(row),
               monthYear(row),
               year(row),
               fractionDate(row),
               ActorInfo(a1Code(row), a1Name(row), a1CountryCode(row),
                         a1KnownGroupCode(row), a1EthnicCode(row),
                         a1Religion1Code(row), a1Religion2Code(row),
                         a1Type1Code(row), a1Type2Code(row), a1Type3Code(row)),
               ActorInfo(a2Code(row), a2Name(row), a2CountryCode(row),
                         a2KnownGroupCode(row), a2EthnicCode(row),
                         a2Religion1Code(row), a2Religion2Code(row),
                         a2Type1Code(row), a2Type2Code(row), a2Type3Code(row)),
               isRootEvent(row),
               eventCode(row),
               eventBaseCode(row),
               eventRootCode(row),
               quadClass(row),
               goldsteinScale(row),
               numMentions(row),
               numSources(row),
               numArticles(row),
               avgTone(row),
               dateAdded(row),
               GeoInfo(a1geoType(row), a1fullName(row), a1gcountryCode(row), a1adm1Code(row),
                       a1lat(row), a1long(row), a1featureID(row), a1fullLocation(row)),
               GeoInfo(a2geoType(row), a2fullName(row), a2gcountryCode(row), a2adm1Code(row),
                       a2lat(row), a2long(row), a2featureID(row), a2fullLocation(row)),
               GeoInfo(actgeoType(row), actfullName(row), actgcountryCode(row), actadm1Code(row),
                       actlat(row), actlong(row), actfeatureID(row), actfullLocation(row))
               )
}

sealed class GdeltRecord extends GdeltRecordBase[GdeltRecord] {
  object globalEventId extends StringColumn(this) with PartitionKey[String]

  override def fromRow(row: Row): GdeltModel =
    super.fromRow(row).copy(globalEventId = globalEventId(row))
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

  val reader = new CSVReader(new BufferedReader(new FileReader(gdeltFilePath)), '\t')
  val gdeltIter = new GdeltReader(reader)

  // Parse each line into a case class
  println("Ingesting, each dot equals 1000 records...")
  var recordCount = 0L
  val (_, elapsed) = GdeltRecord.elapsed {
    gdeltIter.grouped(1000)
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

  println("Querying just monthYear column out of 60...")
  val (result2, elapsed2) = GdeltRecord.elapsed {
    val f = GdeltRecord.select(_.monthYear).
              fetchEnumerator run (Iteratee.fold(0) { (acc, elt: Any) => acc + 1 })
    Await.result(f, 5000 seconds)
  }
  println(s".... got count of $result2 in $elapsed2 seconds")
}