package org.apache.spark.sql.arangodb.commons

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

package object filter {

  private[filter] def splitAttributeNameParts(attribute: String): Array[String] = {
    val parts = new ArrayBuffer[String]()
    var sb = new StringBuilder()
    var inEscapedBlock = false
    for (c <- attribute.toCharArray) {
      if (c == '`') inEscapedBlock = !inEscapedBlock
      if (c == '.' && !inEscapedBlock) {
        parts += sb.toString()
        sb = new StringBuilder()
      } else if (c != '`') {
        sb.append(c)
      }
    }
    parts += sb.toString()
    parts.toArray
  }

  private[filter] def supportsType(t: AbstractDataType): Boolean = t match {
    // atomic types
    case _:
           DateType
         | TimestampType
         | StringType
         | BooleanType
         | FloatType
         | DoubleType
         | IntegerType
         | ShortType
    => true
    // complex types
    case _: NullType => true
    case at: ArrayType => supportsType(at.elementType)
    case st: StructType => st.forall(f => supportsType(f.dataType))
    case _ => false
  }

  private[filter] def getValue(t: AbstractDataType, v: Any): String = t match {
    case NullType => "null"
    case _: DateType | StringType => s""""$v""""
    case _: TimestampType | BooleanType | FloatType | DoubleType | IntegerType | ShortType => v.toString
    case at: ArrayType => s"""[${v.asInstanceOf[Traversable[Any]].map(getValue(at.elementType, _)).mkString(",")}]"""
    case _: StructType =>
      val row = v.asInstanceOf[GenericRowWithSchema]
      val parts = row.values.zip(row.schema).map(sf =>
        s""""${sf._2.name}":${getValue(sf._2.dataType, sf._1)}"""
      )
      s"{${parts.mkString(",")}}"
  }

}
