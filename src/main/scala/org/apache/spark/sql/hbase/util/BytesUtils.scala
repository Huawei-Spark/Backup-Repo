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

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types._
import org.apache.spark.sql.hbase._

object BytesUtils {
  def create(dataType: DataType): BytesUtils = {
    dataType match {
      case BooleanType => new BytesUtils(new HBaseRawType(Bytes.SIZEOF_BOOLEAN), BooleanType)
      case ByteType => new BytesUtils(new HBaseRawType(Bytes.SIZEOF_BYTE), ByteType)
      case DoubleType => new BytesUtils(new HBaseRawType(Bytes.SIZEOF_DOUBLE), DoubleType)
      case FloatType => new BytesUtils(new HBaseRawType(Bytes.SIZEOF_FLOAT), FloatType)
      case IntegerType => new BytesUtils(new HBaseRawType(Bytes.SIZEOF_INT), IntegerType)
      case LongType => new BytesUtils(new HBaseRawType(Bytes.SIZEOF_LONG), LongType)
      case ShortType => new BytesUtils(new HBaseRawType(Bytes.SIZEOF_SHORT), ShortType)
      case StringType => new BytesUtils(null, StringType)
    }
  }

  def toUTF8String(input: HBaseRawType, offset: Int, length: Int): UTF8String = {
    UTF8String(input.slice(offset, offset + length))
  }

  def toByte(input: HBaseRawType, offset: Int): Byte = {
    // Flip sign bit back
    val v: Int = input(offset) ^ 0x80
    v.asInstanceOf[Byte]
  }

  def toBoolean(input: HBaseRawType, offset: Int): Boolean = {
    input(offset) != 0
  }

  def toDouble(input: HBaseRawType, offset: Int): Double = {
    var l: Long = Bytes.toLong(input, offset, Bytes.SIZEOF_DOUBLE)
    l = l - 1
    l ^= (~l >> java.lang.Long.SIZE - 1) | java.lang.Long.MIN_VALUE
    java.lang.Double.longBitsToDouble(l)
  }

  def toShort(input: HBaseRawType, offset: Int): Short = {
    // flip sign bit back
    var v: Int = input(offset) ^ 0x80
    v = (v << 8) + (input(1 + offset) & 0xff)
    v.asInstanceOf[Short]
  }

  def toFloat(input: HBaseRawType, offset: Int): Float = {
    var i = Bytes.toInt(input, offset)
    i = i - 1
    i ^= (~i >> Integer.SIZE - 1) | Integer.MIN_VALUE
    java.lang.Float.intBitsToFloat(i)
  }

  def toInt(input: HBaseRawType, offset: Int): Int = {
    // Flip sign bit back
    var v: Int = input(offset) ^ 0x80
    for (i <- 1 to Bytes.SIZEOF_INT - 1) {
      v = (v << 8) + (input(i + offset) & 0xff)
    }
    v
  }

  def toLong(input: HBaseRawType, offset: Int): Long = {
    // Flip sign bit back
    var v: Long = input(offset) ^ 0x80
    for (i <- 1 to Bytes.SIZEOF_LONG - 1) {
      v = (v << 8) + (input(i + offset) & 0xff)
    }
    v
  }

  /**
   * append one to the byte array
   * @param input the byte array
   * @return the modified byte array
   */
  def addOneString(input: HBaseRawType): HBaseRawType = {
    val len = input.length
    val result = new HBaseRawType(len + 1)
    Array.copy(input, 0, result, 0, len)
    result(len) = 0x01.asInstanceOf[Byte]
    result
  }

  /**
   * add one to the unsigned byte array
   * @param input the unsigned byte array
   * @return null if the byte array is all 0xff, otherwise increase by 1
   */
  def addOne(input: HBaseRawType): HBaseRawType = {
    val len = input.length
    val result = new HBaseRawType(len)
    Array.copy(input, 0, result, 0, len)
    var setValue = false
    for (index <- len - 1 to 0 by -1 if !setValue) {
      val item: Byte = input(index)
      if (item != 0xff.toByte) {
        setValue = true
        if ((item & 0x01.toByte) == 0.toByte) {
          result(index) = (item ^ 0x01.toByte).toByte
        } else if ((item & 0x02.toByte) == 0.toByte) {
          result(index) = (item ^ 0x03.toByte).toByte
        } else if ((item & 0x04.toByte) == 0.toByte) {
          result(index) = (item ^ 0x07.toByte).toByte
        } else if ((item & 0x08.toByte) == 0.toByte) {
          result(index) = (item ^ 0x0f.toByte).toByte
        } else if ((item & 0x10.toByte) == 0.toByte) {
          result(index) = (item ^ 0x1f.toByte).toByte
        } else if ((item & 0x20.toByte) == 0.toByte) {
          result(index) = (item ^ 0x3f.toByte).toByte
        } else if ((item & 0x40.toByte) == 0.toByte) {
          result(index) = (item ^ 0x7f.toByte).toByte
        } else {
          result(index) = (item ^ 0xff.toByte).toByte
        }
        // after increment, set remaining bytes to zero
        for (rest <- index + 1 until len) {
          result(rest) = 0x00.toByte
        }
      }
    }
    if (!setValue) {
      null
    } else {
      result
    }
  }
}

class BytesUtils(var buffer: HBaseRawType, dt: DataType) {
  val dataType = dt

  def toBytes(input: UTF8String): HBaseRawType = {
    buffer = input.getBytes
    buffer
  }

  def toBytes(input: Byte): HBaseRawType = {
    // Flip sign bit so that Byte is binary comparable
    buffer(0) = (input ^ 0x80).asInstanceOf[Byte]
    buffer
  }

  def toBytes(input: Boolean): HBaseRawType = {
    if (input) {
      buffer(0) = (-1).asInstanceOf[Byte]
    } else {
      buffer(0) = 0.asInstanceOf[Byte]
    }
    buffer
  }

  def toBytes(input: Double): HBaseRawType = {
    var l: Long = java.lang.Double.doubleToLongBits(input)
    l = (l ^ ((l >> java.lang.Long.SIZE - 1) | java.lang.Long.MIN_VALUE)) + 1
    Bytes.putLong(buffer, 0, l)
    buffer
  }

  def toBytes(input: Short): HBaseRawType = {
    buffer(0) = ((input >> 8) ^ 0x80).asInstanceOf[Byte]
    buffer(1) = input.asInstanceOf[Byte]
    buffer
  }

  def toBytes(input: Float): HBaseRawType = {
    var i: Int = java.lang.Float.floatToIntBits(input)
    i = (i ^ ((i >> Integer.SIZE - 1) | Integer.MIN_VALUE)) + 1
    Bytes.putInt(buffer, 0, i)
    buffer
  }

  def toBytes(input: Int): HBaseRawType = {
    // Flip sign bit so that INTEGER is binary comparable
    buffer(0) = ((input >> 24) ^ 0x80).asInstanceOf[Byte]
    buffer(1) = (input >> 16).asInstanceOf[Byte]
    buffer(2) = (input >> 8).asInstanceOf[Byte]
    buffer(3) = input.asInstanceOf[Byte]
    buffer
  }

  def toBytes(input: Long): HBaseRawType = {
    buffer(0) = ((input >> 56) ^ 0x80).asInstanceOf[Byte]
    buffer(1) = (input >> 48).asInstanceOf[Byte]
    buffer(2) = (input >> 40).asInstanceOf[Byte]
    buffer(3) = (input >> 32).asInstanceOf[Byte]
    buffer(4) = (input >> 24).asInstanceOf[Byte]
    buffer(5) = (input >> 16).asInstanceOf[Byte]
    buffer(6) = (input >> 8).asInstanceOf[Byte]
    buffer(7) = input.asInstanceOf[Byte]
    buffer
  }

  def toBytes(input: Any): HBaseRawType = {
    input match {
      case item: Boolean => toBytes(item)
      case item: Byte => toBytes(item)
      case item: Double => toBytes(item)
      case item: Float => toBytes(item)
      case item: Int => toBytes(item)
      case item: Long => toBytes(item)
      case item: Short => toBytes(item)
      case item: String => toBytes(UTF8String(item))
      case item: UTF8String => toBytes(item)
    }
  }
}
