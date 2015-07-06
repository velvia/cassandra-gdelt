import com.opencsv.CSVReader
import java.io.{BufferedReader, FileReader}
import java.nio.ByteBuffer
import org.joda.time.DateTime
import org.velvia.filo._
import play.api.libs.iteratee.Iteratee
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try


object GdeltSchema {
  val schema = Seq(
    IngestColumn("globalEventId", classOf[String]),
    IngestColumn("sqlDate",       classOf[String]),
    IngestColumn("monthYear",     classOf[Int]),
    IngestColumn("year",          classOf[Int]),
    IngestColumn("fractionDate",  classOf[Double]),
    IngestColumn("a1Code",    classOf[String]),
    IngestColumn("a1Name",    classOf[String]),
    IngestColumn("a1CountryCode",    classOf[String]),
    IngestColumn("a1KnownGroupCode", classOf[String]),
    IngestColumn("a1EthnicCode",    classOf[String]),
    IngestColumn("a1Religion1Code",    classOf[String]),
    IngestColumn("a1Religion2Code",    classOf[String]),
    IngestColumn("a1Type1Code",    classOf[String]),
    IngestColumn("a1Type2Code",    classOf[String]),
    IngestColumn("a1Type3Code",    classOf[String]),
    IngestColumn("a2Code",    classOf[String]),
    IngestColumn("a2Name",    classOf[String]),
    IngestColumn("a2CountryCode",    classOf[String]),
    IngestColumn("a2KnownGroupCode", classOf[String]),
    IngestColumn("a2EthnicCode",    classOf[String]),
    IngestColumn("a2Religion1Code",  classOf[String]),
    IngestColumn("a2Religion2Code",  classOf[String]),
    IngestColumn("a2Type1Code",  classOf[String]),
    IngestColumn("a2Type2Code",  classOf[String]),
    IngestColumn("a2Type3Code",  classOf[String]),

    // Cols 25-34
    IngestColumn("isRootEvent",  classOf[Int]),
    IngestColumn("eventCode",  classOf[String]),
    IngestColumn("eventBaseCode",  classOf[String]),
    IngestColumn("eventRootCode",  classOf[String]),
    IngestColumn("quadClass",  classOf[Int]),
    IngestColumn("goldsteinScale",  classOf[Double]),
    IngestColumn("numMentions",  classOf[Int]),
    IngestColumn("numSources",  classOf[Int]),
    IngestColumn("numArticles",  classOf[Int]),
    IngestColumn("avgTone",  classOf[Double]),

    // Cols 35-41
    IngestColumn("a1geoType",  classOf[String]),
    IngestColumn("a1fullName",  classOf[String]),
    IngestColumn("a1gcountryCode",  classOf[String]),
    IngestColumn("a1adm1Code",  classOf[String]),
    IngestColumn("a1lat",  classOf[Double]),
    IngestColumn("a1long",  classOf[Double]),
    IngestColumn("a1featureID",  classOf[String]),

    // Cols 42-48
    IngestColumn("a2geoType",  classOf[String]),
    IngestColumn("a2fullName",  classOf[String]),
    IngestColumn("a2gcountryCode",  classOf[String]),
    IngestColumn("a2adm1Code",  classOf[String]),
    IngestColumn("a2lat",  classOf[Double]),
    IngestColumn("a2long",  classOf[Double]),
    IngestColumn("a2featureID",  classOf[String]),

    // Cols 49-55
    IngestColumn("actgeoType",  classOf[String]),
    IngestColumn("actfullName",  classOf[String]),
    IngestColumn("actgcountryCode",  classOf[String]),
    IngestColumn("actadm1Code",  classOf[String]),
    IngestColumn("actlat",  classOf[Double]),
    IngestColumn("actlong",  classOf[Double]),
    IngestColumn("actfeatureID",  classOf[String]),

    // Cols 56-59
    IngestColumn("dateAdded",  classOf[String]),
    IngestColumn("a1fullLocation",  classOf[String]),
    IngestColumn("a2fullLocation",  classOf[String]),
    IngestColumn("actfullLocation",  classOf[String])
  )
}

/**
 * Run this to import the rows into local Casandra.
 */
object GdeltDataTableImporter extends App with LocalConnector {
  import CsvParsingUtils._

  val gdeltFilePath = Try(args(0)).getOrElse {
    println("Usage: pass the gdelt CSV file as the first arg")
    sys.exit(0)
  }

  val reader = new CSVReader(new BufferedReader(new FileReader(gdeltFilePath)), ',')

  // Skip over header
  reader.readNext

  val lineIter = new LineReader(reader)

  // Parse each line into a case class
  println("Ingesting, each dot equals 1000 records...")
  val builder = new RowToColumnBuilder(GdeltSchema.schema, ArrayStringRowSupport)
  var recordCount = 0L
  var rowId = 0
  var shard = 0
  val (_, elapsed) = GdeltRecord.elapsed {
    lineIter.grouped(1000)
            .foreach { records =>
              recordCount += records.length
              records.foreach { r => builder.addRow(r) }
              val columnToBytes = builder.convertToBytes()
              Await.result(DataTableRecord.insertOneRow("gdelt", 0, shard, rowId, columnToBytes), 10 seconds)
              // analyzeData()
              print(".")
              builder.reset()
              rowId += 1
              if (rowId >= 100) {
                // After 100 * 1000 rows, go to the next shard.
                shard += 1
                rowId = 0
              }
            }
  }
  println(s"Done in ${elapsed} secs, ${recordCount / elapsed} records/sec")
  println(s"shard = $shard   rowId = $rowId")
  println(s"# of SimpleColumns: ${SimpleEncoders.count}")
  println(s"# of DictEncodingColumns: ${DictEncodingEncoders.count}")

  private def analyzeData() {
    println("\n---")
    GdeltSchema.schema.map(_.name).zip(builder.builders).foreach { case (name, builder) =>
      println(s"  name: $name \t#NAbits: ${builder.naMask.size} \tcardinality: ${builder.data.toSet.size}")
    }
  }
}

object GdeltDataTableQuery extends App with LocalConnector {
  import scala.concurrent.ExecutionContext.Implicits.global
  import collection.mutable.HashMap
  import collection.mutable.{Map => MMap}

  case class RecordCounter(maxRowIdMap: MMap[Int, Int] = HashMap.empty.withDefaultValue(0),
                           colCount: MMap[String, Int] = HashMap.empty.withDefaultValue(0),
                           bytesRead: MMap[String, Long] = HashMap.empty.withDefaultValue(0L)) {
    def addRowIdForShard(shard: Int, rowId: Int) {
      maxRowIdMap(shard) = Math.max(maxRowIdMap(shard), rowId)
    }

    def addColCount(column: String) { colCount(column) += 1 }

    def addColBytes(column: String, bytes: Long) { bytesRead(column) += bytes }
  }

  // NOTE: we are cheating since I know beforehand there are 40 shards.
  // Gather some statistics to make sure we are indeed reading every row and shard
  println("Querying every column (full export)...")
  val counter = RecordCounter()
  val (result, elapsed) = GdeltRecord.elapsed {
    (0 to 40).foldLeft(0) { (acc, shard) =>
      val f = DataTableRecord.readAllColumns("gdelt", 0, shard) run (
                Iteratee.fold(0) { (acc, x: DataTableRecord.ColRowBytes) =>
                  counter.addRowIdForShard(shard, x._2)
                  counter.addColCount(x._1)
                  counter.addColBytes(x._1, x._3.remaining.toLong)
                acc + 1 }
              )
      acc + Await.result(f, 5000 seconds)
    }
  }
  println(s".... got count of $result in $elapsed seconds")
  println("Shard and column count stats: " + counter)
  println("Total bytes read: " + counter.bytesRead.values.sum)

  import ColumnParser._

  println("Querying just monthYear column out of 20, counting # of elements...")
  val (result2, elapsed2) = GdeltRecord.elapsed {
    (0 to 40).foldLeft(0) { (acc, shard) =>
      val f = DataTableRecord.readSelectColumns("gdelt", 0, shard, List("monthYear")) run (
                Iteratee.fold(0) { (acc, x: DataTableRecord.ColRowBytes) =>
                  val col = ColumnParser.parse[Int](x._3)
                  var count = 0
                  col.foreach { monthYear => count += 1 }
                  acc + count
                } )
      acc + Await.result(f, 5000 seconds)
    }
  }
  println(s".... got count of $result2 in $elapsed2 seconds")

  println("Querying just monthYear column out of 20, top K of elements...")
  val (result3, elapsed3) = GdeltRecord.elapsed {
    val myCount = HashMap.empty[Int, Int].withDefaultValue(0)
    (0 to 40).foreach { shard =>
      val f = DataTableRecord.readSelectColumns("gdelt", 0, shard, List("monthYear")) run (
                Iteratee.fold(0) { (acc, x: DataTableRecord.ColRowBytes) =>
                  val col = ColumnParser.parse[Int](x._3)
                  col.foreach { monthYear => myCount(monthYear) += 1 }
                  0
                } )
      Await.result(f, 5000 seconds)
    }
    myCount.toSeq.sortBy(_._2).reverse.take(10)
  }
  println(s".... got count of $result3 in $elapsed3 seconds")

  println("All done!")
}