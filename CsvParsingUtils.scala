import com.github.marklister.collections.io.GeneralConverter
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object CsvParsingUtils {
  implicit object OptionalStringConv extends GeneralConverter[Option[String]] {
    def convert(x: String): Option[String] = if (x.isEmpty) None else Some(x)
  }

  implicit object DateTimeConv extends GeneralConverter[DateTime] {
    val formatter = DateTimeFormat.forPattern("dd/mm/yy hh:mm:ss aa")
    def convert(x: String): DateTime = DateTime.parse(x, formatter)
  }
}