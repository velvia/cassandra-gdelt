/**
 * Many classes for ingestion and converting rows to columnar FlatBuffers.
 */
import com.google.flatbuffers.FlatBufferBuilder
import framian.column.{Mask, MaskBuilder}
import java.nio.ByteBuffer
import org.velvia.ColumnStore._
import scala.collection.mutable.BitSet
import scala.reflect.ClassTag

// A bunch of builders for row-oriented ingestion to create columns in parallel
// @param empty The empty value to insert for an NA or missing value
sealed abstract class ColumnBuilder[A](empty: A)(implicit val classTagA: ClassTag[A]) {
  // True for a row number (or bit is part of the set) if data for that row is not available
  val naMask = new MaskBuilder
  val data = new collection.mutable.ArrayBuffer[A]

  def addNA() {
    naMask += data.length
    data += empty
  }

  def addData(value: A) { data += value }

  def addOption(value: Option[A]) {
    value.foreach { v => addData(v) }
    value.orElse  { addNA(); None }
  }

  def reset() {
    naMask.clear
    data.clear
  }
}

class IntColumnBuilder extends ColumnBuilder(0)
class LongColumnBuilder extends ColumnBuilder(0L)
class DoubleColumnBuilder extends ColumnBuilder(0.0)
class StringColumnBuilder extends ColumnBuilder("") {
  // For dictionary encoding. NOTE: this set does NOT include empty value
  val stringSet = new collection.mutable.HashSet[String]

  override def addData(value: String) {
    stringSet += value
    super.addData(value)
  }

  override def reset() {
    stringSet.clear
    super.reset()
  }
}

trait BufferConverter[A] {
  def convert(builder: ColumnBuilder[A], hint: BufferConverter.ConversionHint): ByteBuffer
}

/**
 * Classes to convert a Builder to a queryable binary representation.
 * Methods automatically detect the best conversion method to use, but hints are available
 * to pass to the methods.
 */
object BufferConverter {
  sealed trait ConversionHint
  case object AutoDetect extends ConversionHint
  case object SimpleConversion extends ConversionHint
  case object DictionaryEncoding extends ConversionHint

  implicit object IntConverter extends BufferConverter[Int] {
    def convert(builder: ColumnBuilder[Int], hint: ConversionHint) = {
      SimpleConverters.toSimpleColumn(builder.data, builder.naMask.result,
                                      Utils.intVectorBuilder)
    }
  }

  implicit object LongConverter extends BufferConverter[Long] {
    def convert(builder: ColumnBuilder[Long], hint: ConversionHint) = {
      SimpleConverters.toSimpleColumn(builder.data, builder.naMask.result,
                                      Utils.longVectorBuilder)
    }
  }

  implicit object DoubleConverter extends BufferConverter[Double] {
    def convert(builder: ColumnBuilder[Double], hint: ConversionHint) = {
      SimpleConverters.toSimpleColumn(builder.data, builder.naMask.result,
                                      Utils.doubleVectorBuilder)
    }
  }

  implicit object StringConverter extends BufferConverter[String] {
    def convert(builder: ColumnBuilder[String], hint: ConversionHint) = {
      val useDictEncoding = hint match {
        case DictionaryEncoding => true
        case SimpleConversion   => false
        case x => builder match {
          case sb: StringColumnBuilder =>
            // If the string cardinality is below say half of # of elements
            // then definitely worth it to do dictionary encoding.
            // Empty/missing elements do not count towards cardinality, so columns with
            // many NA values will get dict encoded, which saves space
            sb.stringSet.size <= (sb.data.size / 2)
          case x =>  // Someone used something other than our own builder. Oh well. TODO: log
            false
        }
      }
      (useDictEncoding, builder) match {
        case (true, sb: StringColumnBuilder) =>
          DictEncodingConverters.toDictStringColumn(sb.data, sb.naMask.result, sb.stringSet)
        case x =>
          SimpleConverters.toSimpleColumn(builder.data, builder.naMask.result,
                                          Utils.stringVectorBuilder)
      }
    }
  }

  def convertToBuffer[A: BufferConverter](builder: ColumnBuilder[A],
                                          hint: ConversionHint = AutoDetect): java.nio.ByteBuffer = {
    implicitly[BufferConverter[A]].convert(builder, hint)
  }
}

/**
 * Common utilities for creating FlatBuffers, including mask and data vector building
 */
object Utils {
  // default initial size of bytebuffer to allocate.  flatbufferbuilder will expand the buffer if needed.
  // don't make this too big, because one buffer needs to be allocated per column, and writing many columns
  // at once will use up a ton of memory otherwise.
  val BufferSize = 64 * 1024

  // (offset of mask table, true if all NAs / bitmask full / empty data
  def populateNaMask(fbb: FlatBufferBuilder, mask: Mask): (Int, Boolean) = {
    val empty = mask.size == 0
    val full = mask.size > 0 && mask.size == mask.max.get
    var bitMaskOffset = 0

    // Simple bit mask, 1 bit per row
    // Compressed bitmaps require deserialization, but can use JavaEWAH
    // RoaringBitmap is really cool, but very space inefficient when you have less than 4096 integers;
    //    it's much better when you have 100000 or more rows
    // NOTE: we cannot nest structure creation, so have to create bitmask vector first :(
    if (!empty && !full) bitMaskOffset = NaMask.createBitMaskVector(fbb, mask.toBitSet.toBitMask)

    NaMask.startNaMask(fbb)
    NaMask.addMaskType(fbb, if (empty)     { MaskType.AllZeroes }
                            else if (full) { MaskType.AllOnes }
                            else           { MaskType.SimpleBitMask })

    if (!empty && !full) NaMask.addBitMask(fbb, bitMaskOffset)
    (NaMask.endNaMask(fbb), full)
  }

  type DataVectorBuilder[A] = (FlatBufferBuilder, Seq[A]) => (Int, Byte)

  def byteVectorBuilder(fbb: FlatBufferBuilder, data: Seq[Byte]): (Int, Byte) = {
    val vectOffset = ByteVector.createDataVector(fbb, data.toArray)
    (ByteVector.createByteVector(fbb, ByteDataType.TByte, vectOffset), AnyVector.ByteVector)
  }

  def shortVectorBuilder(fbb: FlatBufferBuilder, data: Seq[Short]): (Int, Byte) = {
    val vectOffset = ShortVector.createDataVector(fbb, data.toArray)
    (ShortVector.createShortVector(fbb, vectOffset), AnyVector.ShortVector)
  }

  def intVectorBuilder(fbb: FlatBufferBuilder, data: Seq[Int]): (Int, Byte) = {
    val vectOffset = IntVector.createDataVector(fbb, data.toArray)
    (IntVector.createIntVector(fbb, vectOffset), AnyVector.IntVector)
  }

  def longVectorBuilder(fbb: FlatBufferBuilder, data: Seq[Long]): (Int, Byte) = {
    val vectOffset = LongVector.createDataVector(fbb, data.toArray)
    (LongVector.createLongVector(fbb, vectOffset), AnyVector.LongVector)
  }

  def doubleVectorBuilder(fbb: FlatBufferBuilder, data: Seq[Double]): (Int, Byte) = {
    val vectOffset = DoubleVector.createDataVector(fbb, data.toArray)
    (DoubleVector.createDoubleVector(fbb, vectOffset), AnyVector.DoubleVector)
  }

  def stringVectorBuilder(fbb: FlatBufferBuilder, data: Seq[String]): (Int, Byte) = {
    val vectOffset = makeStringVect(fbb, data)
    (StringVector.createStringVector(fbb, vectOffset), AnyVector.StringVector)
  }

  def makeStringVect(fbb: FlatBufferBuilder, data: Seq[String]): Int = {
    fbb.startVector(4, data.length, 4)
    data.reverseIterator.foreach { str => fbb.addOffset(fbb.createString(str)) }
    fbb.endVector()
  }

  // Just finishes the Column and returns the ByteBuffer.
  // It would be nice to wrap the lifecycle, but too many intricacies with building a FB now.
  def finishColumn(fbb: FlatBufferBuilder, colType: Byte): ByteBuffer = {
    // We want to at least throw an error here if colType is not in the valid range.
    // Better than writing out a random type byte and failing upon read.
    AnyColumn.name(colType)
    val colOffset = Column.createColumn(fbb, colType, fbb.endObject())
    Column.finishColumnBuffer(fbb, colOffset)
    fbb.dataBuffer()
  }
}

/**
 * A whole bunch of converters for simple (no compression) binary representation of sequences,
 * using Google FlatBuffers
 */
object SimpleConverters {
  import Utils._

  var count = 0

  def toSimpleColumn[A](data: Seq[A], naMask: Mask, vectBuilder: DataVectorBuilder[A]): ByteBuffer = {
    count += 1
    val fbb = new FlatBufferBuilder(BufferSize)
    val (naOffset, empty) = populateNaMask(fbb, naMask)
    val (dataOffset, dataType) = if (empty) (0, 0.toByte) else vectBuilder(fbb, data)
    SimpleColumn.startSimpleColumn(fbb)
    SimpleColumn.addNaMask(fbb, naOffset)
    if (!empty) {
      SimpleColumn.addVector(fbb, dataOffset)
      SimpleColumn.addVectorType(fbb, dataType)
    }
    finishColumn(fbb, AnyColumn.SimpleColumn)
  }
}

object DictEncodingConverters {
  import Utils._

  var count = 0

  // Uses the smallest vector possible to fit in integer values
  def smallIntVectorBuilder(fbb: FlatBufferBuilder, data: Seq[Int], maxNum: Int): (Int, Byte) = {
    // Add support for stuff below byte level
    if (maxNum < 256)        byteVectorBuilder(fbb, data.map(_.toByte))
    else if (maxNum < 65536) shortVectorBuilder(fbb, data.map(_.toShort))
    else                     intVectorBuilder(fbb, data)
  }

  def toDictStringColumn(data: Seq[String], naMask: Mask, stringSet: collection.Set[String]): ByteBuffer = {
    count += 1

    // Convert the set of strings to an encoding
    val uniques = stringSet.toSeq
    val strToCode = uniques.zipWithIndex.toMap

    // Encode each string to the code per the map above
    val codes = data.zipWithIndex.map { case (s, i) => if (naMask(i)) 0 else strToCode(s) }

    val fbb = new FlatBufferBuilder(BufferSize)
    val (naOffset, empty) = populateNaMask(fbb, naMask)
    val (dataOffset, dataType) = if (empty) (0, 0.toByte) else smallIntVectorBuilder(fbb, codes, stringSet.size)
    val dictVect = makeStringVect(fbb, uniques)
    DictStringColumn.startDictStringColumn(fbb)
    DictStringColumn.addNaMask(fbb, naOffset)
    DictStringColumn.addDictionary(fbb, dictVect)
    if (!empty) {
      DictStringColumn.addCodes(fbb, dataOffset)
      DictStringColumn.addCodesType(fbb, dataType)
    }
    finishColumn(fbb, AnyColumn.DictStringColumn)
  }
}