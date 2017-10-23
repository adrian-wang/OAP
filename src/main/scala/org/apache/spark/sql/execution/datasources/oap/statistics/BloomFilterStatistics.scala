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

package org.apache.spark.sql.execution.datasources.oap.statistics

import java.io.OutputStream

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

private[oap] class BloomFilterStatistics extends Statistics {
  override val id: Int = BloomFilterStatisticsType.id

  protected var bfIndex: BloomFilter = _

  private lazy val bfMaxBits: Int = StatisticsManager.bloomFilterMaxBits
  private lazy val bfHashFuncs: Int = StatisticsManager.bloomFilterHashFuncs

  @transient private var projectors: Array[UnsafeProjection] = _ // for write
  @transient private lazy val converter: UnsafeProjection = UnsafeProjection.create(schema)
  @transient private lazy val ordering = GenerateOrdering.create(schema)

  override def initialize(schema: StructType): Unit = {
    super.initialize(schema)
    bfIndex = new BloomFilter(bfMaxBits, bfHashFuncs)()
    val boundReference = schema.zipWithIndex.map(x =>
      BoundReference(x._2, x._1.dataType, nullable = true))
    // for multi-column index, add all subsets into bloom filter
    // For example, a column with a = 1, b = 2, a and b are index columns
    // then three records: a = 1, b = 2, a = 1 b = 2, are inserted to bf
    projectors = boundReference.toSet.subsets().filter(_.nonEmpty).map(s =>
      UnsafeProjection.create(s.toArray)).toArray
  }

  override def addOapKey(key: Key): Unit = {
    assert(bfIndex != null, "Please initialize the statistics")
    projectors.foreach(p => bfIndex.addValue(p(key).getBytes))
  }

  override def write(writer: OutputStream, sortedKeys: ArrayBuffer[Key]): Long = {
    var offset = super.write(writer, sortedKeys)

    // Bloom filter index file format:
    // numOfLong            4 Bytes, Int, record the total number of Longs in bit array
    // numOfHashFunction    4 Bytes, Int, record the total number of Hash Functions
    // elementCount         4 Bytes, Int, number of elements stored in the
    //                      related DataFile
    //
    // long 1               8 Bytes, Long, the first element in bit array
    // long 2               8 Bytes, Long, the second element in bit array
    // ...
    // long $numOfLong      8 Bytes, Long, the $numOfLong -th element in bit array
    val bfBitArray = bfIndex.getBitMapLongArray
    IndexUtils.writeInt(writer, bfBitArray.length) // bfBitArray length
    IndexUtils.writeInt(writer, bfIndex.getNumOfHashFunc) // numOfHashFunc
    offset += 8
    bfBitArray.foreach(l => {
      IndexUtils.writeLong(writer, l)
      offset += 8
    })
    offset
  }

  override def read(bytes: Array[Byte], baseOffset: Long): Long = {
    var offset = super.read(bytes, baseOffset) + baseOffset
    offset += readBloomFilter(bytes, offset)
    offset - baseOffset
  }

  private def readBloomFilter(bytes: Array[Byte], baseOffset: Long): Long = {
    var offset = baseOffset
    val bitLength = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
    val numHashFunc = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET + offset + 4)
    offset += 8

    val bitSet = new Array[Long](bitLength)

    for (i <- 0 until bitLength) {
      bitSet(i) = Platform.getLong(bytes, Platform.BYTE_ARRAY_OFFSET + offset)
      offset += 8
    }

    bfIndex = BloomFilter(bitSet, numHashFunc)
    offset - baseOffset
  }

  override def analyse(intervalArray: ArrayBuffer[RangeInterval]): Double = {

    val partialSchema = StructType(schema.dropRight(1))
    val partialConverter = UnsafeProjection.create(partialSchema)
    /**
     * When to skip Index? If any interval in intervalArray satisfies any of below:
     * 1. schema.length > 1 means a multiple column index. First (schema.length-1) fields are same.
     *    1.1 interval.start == interval.end (numFields == schema.length) and NOT exist in bfIndex
     *    1.2 interval.start != interval.end (not equal or DUMMY_KEY) and first
     *        (schema.length - 1) fields NOT exist in bfIndex.
     * 2. schema.length = 1 means single column index. First (schema.length-1) fields are empty.
     *    2.1 interval.start == interval.end (numFields == schema.length) and NOT exist in bfIndex
     *    2.2 interval.start != interval.end (not equal or DUMMY_KEY). Just return false
     */
    val skipIndex = intervalArray.exists { interval =>
      val numFields = math.min(interval.start.numFields, interval.end.numFields)
      if (schema.length > 1) {
        if (numFields == schema.length && ordering.compare(interval.start, interval.end) == 0) {
            !bfIndex.checkExist(converter(interval.start).getBytes)
        } else !bfIndex.checkExist(partialConverter(interval.start).getBytes)
      } else {
        if (numFields == 1 && ordering.compare(interval.start, interval.end) == 0) {
          !bfIndex.checkExist(converter(interval.start).getBytes)
        } else false
      }
    }

    if (skipIndex) StaticsAnalysisResult.SKIP_INDEX
    else StaticsAnalysisResult.USE_INDEX
  }
}
