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
package org.apache.spark.sql.hbase.types

import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.language.implicitConversions
import scala.math.PartialOrdering
import scala.reflect.runtime.universe.typeTag

class Range[+T](val start: Option[T], // None for open ends
               val startInclusive: Boolean,
               val end: Option[T], // None for open ends
               val endInclusive: Boolean,
               val dt: AtomicType) extends Serializable {
  require(dt != null && !(start.isDefined && end.isDefined &&
    ((dt.ordering.eq(start.get, end.get) &&
      (!startInclusive || !endInclusive)) ||
      dt.ordering.gt(start.get.asInstanceOf[dt.InternalType], end.get.asInstanceOf[dt.InternalType]))),
    "Inappropriate range parameters")
  @transient lazy val isPoint: Boolean = start.isDefined && end.isDefined &&
    startInclusive && endInclusive && start.get.equals(end.get)
}

private[hbase] class RangeType[T] extends PartialOrderingDataType {
  override def defaultSize: Int = 4096
  private[sql] type InternalType = Range[T]
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }

  private[spark] override def asNullable: RangeType[T] = this

  /**
   * Convert a value to a point range
   * @param s value to be converted from
   * @param dt runtime type
   * @return
   */
  override def toPartiallyOrderingDataType(s: Any, dt: AtomicType): InternalType = s match {
    case r: InternalType => r
    case _ =>
      new Range(Some(s.asInstanceOf[T] ), true, Some(s.asInstanceOf[T] ), true, dt)
  }

  val partialOrdering = new PartialOrdering[InternalType] {
    // Right now we just support comparisons between a range and a point
    // In the future when more generic range comparisons, these two methods
    // must be functional as expected
    // return -2 if a < b; -1 if a <= b; 0 if a = b; 1 if a >= b; 2 if a > b
    def tryCompare(a: InternalType, b: InternalType): Option[Int] = {
      val aRange = a.asInstanceOf[Range[T]]
      val aStartInclusive = aRange.startInclusive
      val aStart = aRange.start.getOrElse(null).asInstanceOf[aRange.dt.InternalType]
      val aEnd = aRange.end.getOrElse(null).asInstanceOf[aRange.dt.InternalType]
      val aEndInclusive = aRange.endInclusive
      val bRange = b.asInstanceOf[Range[T]]
      val bStart = bRange.start.getOrElse(null).asInstanceOf[aRange.dt.InternalType]
      val bEnd = bRange.end.getOrElse(null).asInstanceOf[aRange.dt.InternalType]
      val bStartInclusive = bRange.startInclusive
      val bEndInclusive = bRange.endInclusive

      // return 1 iff aStart > bEnd
      // return 1 iff aStart = bEnd, aStartInclusive & bEndInclusive are not true at same position
      if ((aStart != null && bEnd != null)
        && (aRange.dt.ordering.gt(aStart, bEnd)
        || (aRange.dt.ordering.equiv(aStart, bEnd) && !(aStartInclusive && bEndInclusive)))) {
        Some(2)
      } // Vice versa
      else if ((bStart != null && aEnd != null)
        && (aRange.dt.ordering.gt(bStart, aEnd)
        || (aRange.dt.ordering.equiv(bStart, aEnd) && !(bStartInclusive && aEndInclusive)))) {
        Some(-2)
      } else if (aStart != null && aEnd != null && bStart != null && bEnd != null &&
        aRange.dt.ordering.equiv(bStart, aEnd)
        && aRange.dt.ordering.equiv(aStart, aEnd)
        && aRange.dt.ordering.equiv(bStart, bEnd)
        && (aStartInclusive && aEndInclusive && bStartInclusive && bEndInclusive)) {
        Some(0)
      } else if (aEnd != null && bStart != null && aRange.dt.ordering.equiv(aEnd, bStart)
        && aEndInclusive && bStartInclusive) {
        Some(-1)
      } else if (aStart != null && bEnd != null && aRange.dt.ordering.equiv(aStart, bEnd)
        && aStartInclusive && bEndInclusive) {
        Some(1)
      } else {
        None
      }
    }

    def lteq(a: InternalType, b: InternalType): Boolean = {
      // [(aStart, aEnd)] and [(bStart, bEnd)]
      // [( and )] mean the possibilities of the inclusive and exclusive condition
      val aRange = a.asInstanceOf[Range[T]]
      val aStartInclusive = aRange.startInclusive
      val aEnd = if (aRange.end.isEmpty) null else aRange.end.get
      val aEndInclusive = aRange.endInclusive
      val bRange = b.asInstanceOf[Range[T]]
      val bStart = if (bRange.start.isEmpty) null else bRange.start.get
      val bStartInclusive = bRange.startInclusive
      val bEndInclusive = bRange.endInclusive

      // Compare two ranges, return true iff the upper bound of the lower range is lteq to
      // the lower bound of the upper range. Because the exclusive boundary could be null, which
      // means the boundary could be infinity, we need to further check this conditions.
      val result =
        (aStartInclusive, aEndInclusive, bStartInclusive, bEndInclusive) match {
          // [(aStart, aEnd] compare to [bStart, bEnd)]
          case (_, true, true, _) =>
            if (aRange.dt.ordering.lteq(aEnd.asInstanceOf[aRange.dt.InternalType],
              bStart.asInstanceOf[aRange.dt.InternalType])) {
              true
            } else {
              false
            }
          // [(aStart, aEnd] compare to (bStart, bEnd)]
          case (_, true, false, _) =>
            if (bStart != null && aRange.dt.ordering.lteq(aEnd.asInstanceOf[aRange.dt.InternalType],
              bStart.asInstanceOf[aRange.dt.InternalType])) {
              true
            } else {
              false
            }
          // [(aStart, aEnd) compare to [bStart, bEnd)]
          case (_, false, true, _) =>
            if (aEnd != null && aRange.dt.ordering.lteq(aEnd.asInstanceOf[aRange.dt.InternalType],
              bStart.asInstanceOf[aRange.dt.InternalType])) {
              true
            } else {
              false
            }
          // [(aStart, aEnd) compare to (bStart, bEnd)]
          case (_, false, false, _) =>
            if (aEnd != null && bStart != null &&
              aRange.dt.ordering.lteq(aEnd.asInstanceOf[aRange.dt.InternalType],
                bStart.asInstanceOf[aRange.dt.InternalType])) {
              true
            } else {
              false
            }
        }

      result
    }
  }
}

object RangeType {
  import scala.reflect.runtime.universe.TypeTag
  private val typeMap = new mutable.HashMap[TypeTag[_], RangeType[_]]
    with mutable.SynchronizedMap[TypeTag[_], RangeType[_]]

  implicit class partialOrdering(dt: AtomicType) {
    private[sql] def toRangeType[T]: RangeType[T] =
      typeMap.getOrElseUpdate(dt.tag, new RangeType[T]).asInstanceOf[RangeType[T]]
  }
}
