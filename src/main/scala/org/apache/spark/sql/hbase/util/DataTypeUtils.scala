/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.spark.sql.hbase.util

import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.spark.sql.catalyst.expressions.{Literal, MutableRow, Row}
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.types._
import org.apache.spark.sql.hbase._

/**
 * Data Type conversion utilities
 */
object DataTypeUtils {
  /**
   * convert the byte array to data
   * @param src the input byte array
   * @param offset the offset in the byte array
   * @param length the length of the data, only used by StringType
   * @param dt the data type
   * @return the actual data converted from byte array
   */
  def bytesToData(src: HBaseRawType, offset: Int, length: Int, dt: DataType): Any = {
    dt match {
      case BooleanType => BytesUtils.toBoolean(src, offset)
      case ByteType => BytesUtils.toByte(src, offset)
      case DoubleType => BytesUtils.toDouble(src, offset)
      case FloatType => BytesUtils.toFloat(src, offset)
      case IntegerType => BytesUtils.toInt(src, offset)
      case LongType => BytesUtils.toLong(src, offset)
      case ShortType => BytesUtils.toShort(src, offset)
      case StringType => BytesUtils.toUTF8String(src, offset, length)
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
  }

  /**
   * convert data to byte array
   * @param src the input data
   * @param dt the data type
   * @return the output byte array
   */
  def dataToBytes(src: Any,
                  dt: DataType): HBaseRawType = {
    // TODO: avoid new instance per invocation
    lazy val bu = BytesUtils.create(dt)
    dt match {
      case BooleanType => bu.toBytes(src.asInstanceOf[Boolean])
      case ByteType => bu.toBytes(src.asInstanceOf[Byte])
      case DoubleType => bu.toBytes(src.asInstanceOf[Double])
      case FloatType => bu.toBytes(src.asInstanceOf[Float])
      case IntegerType => bu.toBytes(src.asInstanceOf[Int])
      case LongType => bu.toBytes(src.asInstanceOf[Long])
      case ShortType => bu.toBytes(src.asInstanceOf[Short])
      case StringType => bu.toBytes(src)
      case _ => SparkSqlSerializer.serialize[Any](src) //TODO
    }
  }

  /**
   * set the row data from byte array
   * @param row the row to be set
   * @param index the index in the row
   * @param src the input byte array
   * @param offset the offset in the byte array
   * @param length the length of the data, only used by StringType
   * @param dt the data type
   */
  def setRowColumnFromHBaseRawType(row: MutableRow,
                                   index: Int,
                                   src: HBaseRawType,
                                   offset: Int,
                                   length: Int,
                                   dt: DataType): Unit = {
    dt match {
      case BooleanType => row.setBoolean(index, BytesUtils.toBoolean(src, offset))
      case ByteType => row.setByte(index, BytesUtils.toByte(src, offset))
      case DoubleType => row.setDouble(index, BytesUtils.toDouble(src, offset))
      case FloatType => row.setFloat(index, BytesUtils.toFloat(src, offset))
      case IntegerType => row.setInt(index, BytesUtils.toInt(src, offset))
      case LongType => row.setLong(index, BytesUtils.toLong(src, offset))
      case ShortType => row.setShort(index, BytesUtils.toShort(src, offset))
      case StringType => row.update(index, BytesUtils.toUTF8String(src, offset, length))
      case _ => row.update(index, SparkSqlSerializer.deserialize[Any](src)) //TODO
    }
  }

  def string2TypeData(v: String, dt: DataType): Any = {
    v match {
      case null => null
      case _ =>
        dt match {
          // TODO: handle some complex types
          case BooleanType => v.toBoolean
          case ByteType => v.getBytes()(0)
          case DoubleType => v.toDouble
          case FloatType => v.toFloat
          case IntegerType => v.toInt
          case LongType => v.toLong
          case ShortType => v.toShort
          case StringType => v
        }
    }
  }

  /**
   * get the data from row based on index
   * @param row the input row
   * @param index the index of the data
   * @param dt the data type
   * @return the data from the row based on index
   */
  def getRowColumnInHBaseRawType(row: Row, index: Int, dt: DataType): HBaseRawType = {
    if (row.isNullAt(index)) return new Array[Byte](0)

    val bu = BytesUtils.create(dt)
    dt match {
      case BooleanType => bu.toBytes(row.getBoolean(index))
      case ByteType => bu.toBytes(row.getByte(index))
      case DoubleType => bu.toBytes(row.getDouble(index))
      case FloatType => bu.toBytes(row.getFloat(index))
      case IntegerType => bu.toBytes(row.getInt(index))
      case LongType => bu.toBytes(row.getLong(index))
      case ShortType => bu.toBytes(row.getShort(index))
      case StringType => bu.toBytes(row.getString(index))
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
  }

  /**
   * create binary comparator for the input expression
   * @param bu the byte utility
   * @param expression the input expression
   * @return the constructed binary comparator
   */
  def getBinaryComparator(bu: BytesUtils, expression: Literal): BinaryComparator = {
    expression.dataType match {
      case BooleanType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Boolean]))
      case ByteType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Byte]))
      case DoubleType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Double]))
      case FloatType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Float]))
      case IntegerType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Int]))
      case LongType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Long]))
      case ShortType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Short]))
      case StringType => new BinaryComparator(bu.toBytes(expression.value))
      case _ => throw new Exception("Cannot convert the data type using BinaryComparator")
    }
  }
}
