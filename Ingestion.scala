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
class StringColumnBuilder extends ColumnBuilder("")
class LongColumnBuilder extends ColumnBuilder(0L)
class DoubleColumnBuilder extends ColumnBuilder(0.0)

trait BufferConverter[A] {
  def convert(builder: ColumnBuilder[A]): ByteBuffer
}

/**
 * Classes to convert a Builder to a FlatBuffer.
 * Now, this would probably be broken into multiple objects for different types of converters.
 * TODO: Intelligence.  Maybe it can detect automatically the best method to use.
 */
object BufferConverter {
  implicit object IntConverter extends BufferConverter[Int] {
    def convert(builder: ColumnBuilder[Int]) = {
      SimpleConverters.toSimpleColumn(builder.data, builder.naMask.result,
                                      SimpleConverters.intVectorBuilder)
    }
  }

  implicit object LongConverter extends BufferConverter[Long] {
    def convert(builder: ColumnBuilder[Long]) = {
      SimpleConverters.toSimpleColumn(builder.data, builder.naMask.result,
                                      SimpleConverters.longVectorBuilder)
    }
  }

  implicit object DoubleConverter extends BufferConverter[Double] {
    def convert(builder: ColumnBuilder[Double]) = {
      SimpleConverters.toSimpleColumn(builder.data, builder.naMask.result,
                                      SimpleConverters.doubleVectorBuilder)
    }
  }

  implicit object StringConverter extends BufferConverter[String] {
    def convert(builder: ColumnBuilder[String]) = {
      SimpleConverters.toSimpleColumn(builder.data, builder.naMask.result,
                                      SimpleConverters.stringVectorBuilder)
    }
  }

  def convertToBuffer[A: BufferConverter](builder: ColumnBuilder[A]): java.nio.ByteBuffer = {
    implicitly[BufferConverter[A]].convert(builder)
  }
}

/**
 * A whole bunch of converters for simple (no compression) binary representation of sequences
 */
object SimpleConverters {
  // Default initial size of bytebuffer to allocate.  FlatBufferBuilder will expand the buffer if needed.
  // Don't make this too big, because one buffer needs to be allocated per column, and writing many columns
  // at once will use up a ton of memory otherwise.
  val BufferSize = 64 * 1024

  type DataVectorBuilder[A] = (FlatBufferBuilder, Seq[A]) => (Int, Byte)

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
    val offsets = data.map { str => fbb.createString(str) }
    val vectOffset = StringVector.createDataVector(fbb, offsets.toArray)
    (StringVector.createStringVector(fbb, vectOffset), AnyVector.StringVector)
  }

  def toSimpleColumn[A](data: Seq[A], naMask: Mask, vectBuilder: DataVectorBuilder[A]): ByteBuffer = {
    val fbb = new FlatBufferBuilder(BufferSize)
    val (naOffset, empty) = populateNaMask(fbb, naMask)
    val (dataOffset, dataType) = if (empty) (0, 0.toByte) else vectBuilder(fbb, data)
    SimpleColumn.startSimpleColumn(fbb)
    SimpleColumn.addNaMask(fbb, naOffset)
    if (!empty) {
      SimpleColumn.addVector(fbb, dataOffset)
      SimpleColumn.addVectorType(fbb, dataType)
    }
    val simpColOffset = SimpleColumn.endSimpleColumn(fbb)
    val colOffset = Column.createColumn(fbb, AnyColumn.SimpleColumn, simpColOffset)
    Column.finishColumnBuffer(fbb, colOffset)
    fbb.dataBuffer()
  }

  // (offset of mask table, true if all NAs / bitmask full / empty data
  private def populateNaMask(fbb: FlatBufferBuilder, mask: Mask): (Int, Boolean) = {
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
}