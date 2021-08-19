package execution.datasources

import scala.math.Ordering
import org.apache.hadoop.hbase.util.Bytes

package object hbase {
  type HBaseType = Array[Byte]

  val ord: Ordering[HBaseType] = new Ordering[HBaseType] {

    def compare(x: Array[Byte], y: Array[Byte]): Int = {
      return Bytes.compareTo(x, y)
    }
  }

  //Do not use BinaryType.ordering
  implicit val order: Ordering[HBaseType] =  ord


  val ByteMax = -1.asInstanceOf[Byte]
  val ByteMin = 0.asInstanceOf[Byte]
  val MaxLength = 256
}
