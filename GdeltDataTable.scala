import com.github.marklister.collections.io._
import java.io.{BufferedReader, FileReader}
import java.nio.ByteBuffer
import org.joda.time.DateTime
import play.api.libs.iteratee.Iteratee
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

case class IngestColumn(name: String, builder: ColumnBuilder[_])

/*  Must be implemented for proper building of columns from rows
 */
trait RowIngestSupport[R] {
  def getString(row: R, columnNo: Int): Option[String]
  def getInt(row: R, columnNo: Int): Option[Int]
  def getLong(row: R, columnNo: Int): Option[Long]
  def getDouble(row: R, columnNo: Int): Option[Double]
}

// To help matching against the ClassTag in the ColumnBuilder
private object Classes {
  val Byte = java.lang.Byte.TYPE
  val Short = java.lang.Short.TYPE
  val Int = java.lang.Integer.TYPE
  val Long = java.lang.Long.TYPE
  val Float = java.lang.Float.TYPE
  val Double = java.lang.Double.TYPE
  val String = classOf[String]
}

/**
 * Class to help transpose a set of rows of type R to ByteBuffer-backed columns.
 * @param schema a Seq of IngestColumn describing the [[ColumnBuilder]] used for each column
 * @param ingestSupport something to convert from a row R to specific types
 *
 * TODO: Add stats about # of rows, chunks/buffers encoded, bytes encoded, # NA's etc.
 * Write them to a Cass table.
 */
class RowToColumnBuilder[R](schema: Seq[IngestColumn], ingestSupport: RowIngestSupport[R]) {
  val ingestFuncs: Seq[(R, Int) => Unit] = schema.map { case IngestColumn(_, builder) =>
    builder.classTagA.runtimeClass match {
      case Classes.Int    =>
        (r: R, c: Int) => builder.asInstanceOf[IntColumnBuilder].addOption(ingestSupport.getInt(r, c))
      case Classes.Long   =>
        (r: R, c: Int) => builder.asInstanceOf[LongColumnBuilder].addOption(ingestSupport.getLong(r, c))
      case Classes.Double =>
        (r: R, c: Int) => builder.asInstanceOf[DoubleColumnBuilder].addOption(ingestSupport.getDouble(r, c))
      case Classes.String =>
        (r: R, c: Int) => builder.asInstanceOf[StringColumnBuilder].addOption(ingestSupport.getString(r, c))
      case x              => throw new RuntimeException("Unsupported input type " + x)
    }
  }

  /**
   * Resets the ColumnBuilders.  Call this before the next batch of rows to transpose.
   * @return {[type]} [description]
   */
  def reset() {
    schema foreach { _.builder.reset() }
  }

  /**
   * Adds a single row of data to each of the ColumnBuilders.
   * @param row the row of data to transpose.  Each column will be added to the right Builders.
   */
  def addRow(row: R) {
    ingestFuncs.zipWithIndex.foreach { case (func, i) =>
      func(row, i)
    }
  }

  /**
   * Converts the contents of the [[ColumnBuilder]]s to ByteBuffers for writing or transmission.
   */
  def convertToBytes(): Map[String, ByteBuffer] = {
    schema.map { case IngestColumn(columnName, builder) =>
      val bytes = builder.classTagA.runtimeClass match {
        case Classes.Int    =>
          BufferConverter.convertToBuffer(builder.asInstanceOf[IntColumnBuilder])
        case Classes.Long   =>
          BufferConverter.convertToBuffer(builder.asInstanceOf[LongColumnBuilder])
        case Classes.Double =>
          BufferConverter.convertToBuffer(builder.asInstanceOf[DoubleColumnBuilder])
        case Classes.String =>
          BufferConverter.convertToBuffer(builder.asInstanceOf[StringColumnBuilder])
        case x              => throw new RuntimeException("Unsupported input type " + x)
      }
      (columnName, bytes)
    }.toMap
  }
}

object GdeltSchema {
  val schema = Seq(
    IngestColumn("globalEventId", new StringColumnBuilder),
    IngestColumn("sqlDate",       new LongColumnBuilder),    // really this is DateTime, converted to epoch
    IngestColumn("monthYear",     new IntColumnBuilder),
    IngestColumn("year",          new IntColumnBuilder),
    IngestColumn("fractionDate",  new DoubleColumnBuilder),
    IngestColumn("actor1Code",    new StringColumnBuilder),
    IngestColumn("actor1Name",    new StringColumnBuilder),
    IngestColumn("actor1CountryCode",    new StringColumnBuilder),
    IngestColumn("actor1KnownGroupCode", new StringColumnBuilder),
    IngestColumn("actor1EthnicCode",    new StringColumnBuilder),
    IngestColumn("actor1Religion1Code",    new StringColumnBuilder),
    IngestColumn("actor1Religion2Code",    new StringColumnBuilder),
    IngestColumn("actor1Type1Code",    new StringColumnBuilder),
    IngestColumn("actor1Type2Code",    new StringColumnBuilder),
    IngestColumn("actor1Type3Code",    new StringColumnBuilder),
    IngestColumn("actor2Code",    new StringColumnBuilder),
    IngestColumn("actor2Name",    new StringColumnBuilder),
    IngestColumn("actor2CountryCode",    new StringColumnBuilder),
    IngestColumn("actor2KnownGroupCode", new StringColumnBuilder),
    IngestColumn("actor2EthnicCode",    new StringColumnBuilder)
  )
}

object TupleRowIngestSupport extends RowIngestSupport[Product] {
  type R = Product
  def getString(row: R, columnNo: Int): Option[String] =
    row.productElement(columnNo).asInstanceOf[Option[String]]
  def getInt(row: R, columnNo: Int): Option[Int] =
    row.productElement(columnNo).asInstanceOf[Option[Int]]
  // The only Long column is the Date column which needs to be converted
  def getLong(row: R, columnNo: Int): Option[Long] =
    row.productElement(columnNo).asInstanceOf[Option[String]].flatMap { str =>
      Try(CsvParsingUtils.DateTimeConv.convert(str).getMillis).toOption
    }
  def getDouble(row: R, columnNo: Int): Option[Double] =
    row.productElement(columnNo).asInstanceOf[Option[Double]]
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

  val reader = new BufferedReader(new FileReader(gdeltFilePath))
  val lineIter = CsvParser[Option[String], Option[String], Option[Int], Option[Int], Option[Double],
                           Option[String], Option[String], Option[String], Option[String], Option[String],
                           Option[String], Option[String], Option[String], Option[String], Option[String],
                           Option[String], Option[String], Option[String], Option[String], Option[String]].
                   iterator(reader, hasHeader = true)

  // Parse each line into a case class
  println("Ingesting, each dot equals 1000 records...")
  val builder = new RowToColumnBuilder(GdeltSchema.schema, TupleRowIngestSupport)
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

  private def analyzeData() {
    println("\n---")
    GdeltSchema.schema.foreach { case IngestColumn(name, builder) =>
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

  import VectorExtractorBuilder._

  println("Querying just monthYear column out of 20, counting # of elements...")
  val (result2, elapsed2) = GdeltRecord.elapsed {
    (0 to 40).foldLeft(0) { (acc, shard) =>
      val f = DataTableRecord.readSelectColumns("gdelt", 0, shard, List("monthYear")) run (
                Iteratee.fold(0) { (acc, x: DataTableRecord.ColRowBytes) =>
                  val col = ColumnParser.parseAsSimpleColumn[Int](x._3)
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
                  val col = ColumnParser.parseAsSimpleColumn[Int](x._3)
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