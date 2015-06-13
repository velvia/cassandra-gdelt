import com.github.marklister.collections.io.GeneralConverter
import com.opencsv.CSVReader
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.util.Try

// Unfortunately had to keep the old stuff around
object CsvParsingUtils {
  implicit object OptionalStringConv extends GeneralConverter[Option[String]] {
    def convert(x: String) = convertOptStr(x)
  }

  implicit object DateTimeConv extends GeneralConverter[DateTime] {
    def convert(x: String): DateTime = convertDT(x)
  }

  def convertOptStr(x: String): Option[String] = if (x.isEmpty) None else Some(x)

  val formatter = DateTimeFormat.forPattern("dd/mm/yy hh:mm:ss aa")
  def convertDT(x: String): DateTime = DateTime.parse(x, formatter)

  def convertOptInt(x: String): Option[Int] = Try(x.toInt).toOption
  def convertOptD(x: String): Option[Double] = Try(x.toDouble).toOption

  def convertActorInfo(line: Array[String], index: Int): ActorInfo = {
    ActorInfo(convertOptStr(line(index)),
              convertOptStr(line(index + 1)),
              convertOptStr(line(index + 2)),
              convertOptStr(line(index + 3)),
              convertOptStr(line(index + 4)),
              convertOptStr(line(index + 5)),
              convertOptStr(line(index + 6)),
              convertOptStr(line(index + 7)),
              convertOptStr(line(index + 8)),
              convertOptStr(line(index + 9)))
  }

  def convertActorGeo(line: Array[String], index: Int, fullLoc: Int): GeoInfo =
    GeoInfo(convertOptStr(line(index)),
            convertOptStr(line(index + 1)),
            convertOptStr(line(index + 2)),
            convertOptStr(line(index + 3)),
            line(index + 4).toDouble,
            line(index + 5).toDouble,
            convertOptStr(line(index + 6)),
            convertOptStr(line(fullLoc))
           )
}

class GdeltReader(reader: CSVReader) extends Iterator[GdeltModel] {
  import CsvParsingUtils._

  var curLine: Array[String] = null
  def hasNext: Boolean = {
    curLine = reader.readNext
    curLine != null
  }

  def next: GdeltModel = {
    GdeltModel(curLine(0),
               convertDT(curLine(1)),
               convertOptInt(curLine(2)),
               convertOptInt(curLine(3)),
               convertOptD(curLine(4)),
               convertActorInfo(curLine, 5),
               convertActorInfo(curLine, 15),
               curLine(25).toInt, // isRootEvent
               convertOptStr(curLine(26)),
               convertOptStr(curLine(27)),
               convertOptStr(curLine(28)),
               convertOptInt(curLine(29)),
               curLine(30).toDouble,  // goldsteinScale
               curLine(31).toInt,
               curLine(32).toInt,
               curLine(33).toInt,
               curLine(34).toDouble,
               convertOptStr(curLine(56)),
               convertActorGeo(curLine, 35, 57),
               convertActorGeo(curLine, 42, 58),
               convertActorGeo(curLine, 49, 59)
              )
  }
}