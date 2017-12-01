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

package org.apache.spark.sql.execution.datasources.oap.index

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import sun.nio.ch.DirectBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.filecache.{BTreeFiber, CacheResult, FiberCacheManager}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.io.ChunkedByteBuffer

private[index] case class BTreeIndexRecordRangeReader(
    configuration: Configuration,
    schema: StructType) extends BTreeIndexRecordReader(configuration, schema) {
  def initialize(path: Path, intervalArray: ArrayBuffer[RangeInterval]): Unit = {
    reader = BTreeIndexFileReader(configuration, path)

    footerFiber = BTreeFiber(() => reader.readFooter(), reader.file.toString, 0, 0)
    footerCache = FiberCacheManager.getOrElseUpdate(footerFiber, configuration)
    footer = BTreeIndexRecordReader.BTreeFooter(footerCache.buffer)

    rowIdListFiber = BTreeFiber(() => reader.readRowIdList(), reader.file.toString, 1, 0)
    rowIdListCache = FiberCacheManager.getOrElseUpdate(rowIdListFiber, configuration)
    rowIdList = BTreeIndexRecordReader.BTreeRowIdList(rowIdListCache.buffer)

    internalIterator = intervalArray.toIterator.flatMap { interval =>
      val (start, end) = findRowIdRange(interval)
      (start until end).toIterator.map(rowIdList.getRowId)
    }
  }

  def findRowIdRange(interval: RangeInterval): (Int, Int) = {
    val (nodeIdxForStart, isStartFound) = findNodeIdx(interval.start, isStart = true)
    val (nodeIdxForEnd, isEndFound) = findNodeIdx(interval.end, isStart = false)

    val recordCount = footer.getRecordCount
    if (nodeIdxForStart == nodeIdxForEnd && !isStartFound && !isEndFound) {
      (0, 0)
    } else {
      val start = if (interval.start == IndexScanner.DUMMY_KEY_START) 0
      else {
        nodeIdxForStart.map { idx =>
          findRowIdPos(idx, interval.start, isStart = true, !interval.startInclude)
        }.getOrElse(recordCount)
      }
      val end = if (interval.end == IndexScanner.DUMMY_KEY_END) recordCount
      else {
        nodeIdxForEnd.map { idx =>
          findRowIdPos(idx, interval.end, isStart = false, interval.endInclude)
        }.getOrElse(recordCount)
      }
      (start, end)
    }
  }
}

private[index] case class BTreeIndexRecordPatternReader(
    configuration: Configuration,
    schema: StructType) extends BTreeIndexRecordReader(configuration, schema) {
  def initialize(path: Path, patternArray: ArrayBuffer[SearchPattern]): Unit = {
    reader = BTreeIndexFileReader(configuration, path)

    footerFiber = BTreeFiber(() => reader.readFooter(), reader.file.toString, 0, 0)
    footerCache = FiberCacheManager.getOrElseUpdate(footerFiber, configuration)
    footer = BTreeIndexRecordReader.BTreeFooter(footerCache.buffer)

    rowIdListFiber = BTreeFiber(() => reader.readRowIdList(), reader.file.toString, 1, 0)
    rowIdListCache = FiberCacheManager.getOrElseUpdate(rowIdListFiber, configuration)
    rowIdList = BTreeIndexRecordReader.BTreeRowIdList(rowIdListCache.buffer)

    internalIterator = patternArray.toIterator.flatMap { pattern =>
      val (start, end) = findRowIdRange(pattern)
      (start until end).toIterator.map(rowIdList.getRowId)
    }
  }

  def findRowIdRange(pattern: SearchPattern): (Int, Int) = {
    val (nodeIdxForStart, isStartFound) = findNodeIdx(pattern.pattern, isStart = true)
    val (nodeIdxForEnd, isEndFound) = findNodeIdx(interval.end, isStart = false)

    val recordCount = footer.getRecordCount
    if (nodeIdxForStart == nodeIdxForEnd && !isStartFound && !isEndFound) {
      (0, 0)
    } else {
      val start = if (interval.start == IndexScanner.DUMMY_KEY_START) 0
      else {
        nodeIdxForStart.map { idx =>
          findRowIdPos(idx, interval.start, isStart = true, !interval.startInclude)
        }.getOrElse(recordCount)
      }
      val end = if (interval.end == IndexScanner.DUMMY_KEY_END) recordCount
      else {
        nodeIdxForEnd.map { idx =>
          findRowIdPos(idx, interval.end, isStart = false, interval.endInclude)
        }.getOrElse(recordCount)
      }
      (start, end)
    }
  }
}

private[index] class BTreeIndexRecordReader(
    configuration: Configuration,
    schema: StructType) extends Iterator[Int] {

  protected var internalIterator: Iterator[Int] = _

  import BTreeIndexRecordReader.{BTreeFooter, BTreeRowIdList, BTreeNodeData}
  protected var footer: BTreeFooter = _
  protected var footerFiber: BTreeFiber = _
  protected var footerCache: CacheResult = _
  protected var rowIdList: BTreeRowIdList = _
  protected var rowIdListFiber: BTreeFiber = _
  protected var rowIdListCache: CacheResult = _

  protected var reader: BTreeIndexFileReader = _

  private lazy val ordering = GenerateOrdering.create(schema)
  private lazy val partialOrdering = GenerateOrdering.create(StructType(schema.dropRight(1)))

  protected def findRowIdPos(
      nodeIdx: Int,
      candidate: InternalRow,
      isStart: Boolean,
      findNext: Boolean): Int = {

    val nodeFiber = BTreeFiber(
      () => reader.readNode(footer.getNodeOffset(nodeIdx), footer.getNodeSize(nodeIdx)),
      reader.file.toString,
      2,
      nodeIdx
    )
    val nodeCache = FiberCacheManager.getOrElseUpdate(nodeFiber, configuration)
    val node = BTreeNodeData(nodeCache.buffer)

    val keyCount = node.getKeyCount

    val (pos, found) =
      binarySearch(0, keyCount, node.getKey(_, schema), candidate, rowOrdering(_, _, isStart))

    val keyPos = if (found && findNext) pos + 1 else pos

    val rowPos =
      if (keyPos == keyCount) {
        if (nodeIdx + 1 == footer.getNodesCount) footer.getRecordCount
        else {
          val offset = footer.getNodeOffset(nodeIdx + 1)
          val size = footer.getNodeSize(nodeIdx + 1)
          val nextNodeFiber = BTreeFiber(
            () => reader.readNode(offset, size),
            reader.file.toString,
            2,
            nodeIdx + 1)
          val nextNodeCache = FiberCacheManager.getOrElseUpdate(nextNodeFiber, configuration)
          val nextNode = BTreeNodeData(nextNodeCache.buffer)
          val rowPos = nextNode.getRowIdPos(0)
          releaseCache(nextNodeCache, nextNodeFiber)
          rowPos
        }
      } else node.getRowIdPos(keyPos)
    releaseCache(nodeCache, nodeFiber)
    rowPos
  }

  /**
   * Find the Node index contains the candidate. If no, return the first which node.max >= candidate
   * If candidate > all node.max, return None
   * @param isStart to indicate if the candidate is interval.start or interval.end
   * @return Option of Node index and if candidate falls in node (means min <= candidate < max)
   */
  protected def findNodeIdx(candidate: InternalRow, isStart: Boolean): (Option[Int], Boolean) = {
    val idxOption = (0 until footer.getNodesCount).find { idx =>
      rowOrdering(candidate, footer.getMaxValue(idx, schema), isStart) <= 0
    }

    (idxOption, idxOption.exists { idx =>
      rowOrdering(candidate, footer.getMinValue(idx, schema), isStart) >= 0
    })
  }

  /**
   * Constrain: keys.last >= candidate must be true. This is guaranteed by [[findNodeIdx]]
   * @return the first key >= candidate. (keys.last >= candidate makes this always possible)
   */
  private[index] def binarySearch(
      start: Int, length: Int,
      keys: Int => InternalRow, candidate: InternalRow,
      compare: (InternalRow, InternalRow) => Int): (Int, Boolean) = {
    var s = 0
    var e = length - 1
    var found = false
    var m = s
    while (s <= e & !found) {
      assert(s + e >= 0, "too large array size caused overflow")
      m = (s + e) / 2
      val cmp = compare(keys(m), candidate)
      if (cmp == 0) found = true
      else if (cmp < 0) s = m + 1
      else e = m - 1
    }
    if (!found) m = s
    (m, found)
  }

  /**
   * Compare auxiliary function.
   * @param x, y are the key to be compared.
   *         One comes from interval.start/end. One comes from index records
   * @param isStart indicates to compare interval.start or interval end
   */
  private[index] def rowOrdering(x: InternalRow, y: InternalRow, isStart: Boolean): Int = {
    if (x.numFields == y.numFields) {
      ordering.compare(x, y)
    } else if (x.numFields < y.numFields) {
      val cmp = partialOrdering.compare(x, y)
      if (cmp == 0) {
        if (isStart) -1 else 1
      } else cmp
    } else {
      -rowOrdering(y, x, isStart) // Keep x.numFields <= y.numFields to simplify
    }
  }

  private def releaseCache(cache: CacheResult, fiber: BTreeFiber): Unit = {
    if (cache != null) {
      if (cache.cached) FiberCacheManager.releaseLock(fiber)
      else cache.buffer.dispose()
    }
  }

  def close(): Unit = {
    releaseCache(footerCache, footerFiber)
    releaseCache(rowIdListCache, rowIdListFiber)
    reader.close()
  }

  /**
   * TODO: if this hasNext doesn't reach false, the resource can't be released
   * For example:
   *   Assume recordReader.size = 100, Someone called `recordReader.take(10)`.
   */
  override def hasNext: Boolean = {
    if (internalIterator.hasNext) true
    else {
      close()
      false
    }
  }

  override def next(): Int = internalIterator.next()
}

private[index] object BTreeIndexRecordReader {
  private[index] def readBasedOnSchema(
      baseObj: Object, offset: Long, schema: StructType): InternalRow = {
    var pos = offset
    val values = schema.map(_.dataType).map { dataType =>
      val (value, length) = IndexUtils.readBasedOnDataType(baseObj, pos, dataType)
      pos += length
      value
    }
    InternalRow.fromSeq(values)
  }

  private[index] case class BTreeFooter(buf: ChunkedByteBuffer) {
    private val nodePosOffset = Integer.SIZE / 8
    private val nodeSizeOffset = Integer.SIZE / 8 * 2
    private val minPosOffset = Integer.SIZE / 8 * 3
    private val maxPosOffset = Integer.SIZE / 8 * 4
    private val nodeMetaStart = Integer.SIZE / 8 * 2
    private val nodeMetaByteSize = Integer.SIZE / 8 * 5

    private val (baseObj, baseOffset): (Object, Long) = buf.chunks.head match {
      case db: DirectBuffer => (null, db.address())
      case _ => (buf.toArray, Platform.BYTE_ARRAY_OFFSET)
    }
    def getRecordCount: Int = Platform.getInt(baseObj, baseOffset)
    def getNodesCount: Int = Platform.getInt(baseObj, baseOffset + Integer.SIZE / 8)
    def getMaxValue(idx: Int, schema: StructType): InternalRow =
      BTreeIndexRecordReader.readBasedOnSchema(baseObj, baseOffset + getMaxValueOffset(idx), schema)
    def getMinValue(idx: Int, schema: StructType): InternalRow =
      BTreeIndexRecordReader.readBasedOnSchema(baseObj, baseOffset + getMinValueOffset(idx), schema)
    def getMinValueOffset(idx: Int): Int =
      Platform.getInt(baseObj, baseOffset + nodeMetaStart + nodeMetaByteSize * idx + minPosOffset) +
          nodeMetaStart + nodeMetaByteSize * getNodesCount
    def getMaxValueOffset(idx: Int): Int =
      Platform.getInt(baseObj, baseOffset + nodeMetaStart + nodeMetaByteSize * idx + maxPosOffset) +
          nodeMetaStart + nodeMetaByteSize * getNodesCount
    def getNodeOffset(idx: Int): Int =
      Platform.getInt(baseObj, baseOffset + nodeMetaStart + idx * nodeMetaByteSize + nodePosOffset)
    def getNodeSize(idx: Int): Int =
      Platform.getInt(baseObj, baseOffset + nodeMetaStart + idx * nodeMetaByteSize + nodeSizeOffset)
  }

  private[index] case class BTreeRowIdList(buf: ChunkedByteBuffer) {
    private val (baseObj, baseOffset): (Object, Long) = buf.chunks.head match {
      case db: DirectBuffer => (null, db.address())
      case _ => (buf.toArray, Platform.BYTE_ARRAY_OFFSET)
    }
    def getRowId(idx: Int): Int = Platform.getInt(baseObj, baseOffset + idx * Integer.SIZE / 8)
  }

  private[index] case class BTreeNodeData(buf: ChunkedByteBuffer) {
    private val posSectionStart = Integer.SIZE / 8
    private val posEntrySize = Integer.SIZE / 8 * 2
    private val (baseObj, baseOffset): (Object, Long) = buf.chunks.head match {
      case db: DirectBuffer => (null, db.address())
      case _ => (buf.toArray, Platform.BYTE_ARRAY_OFFSET)
    }
    private def valueSectionStart = posSectionStart + getKeyCount * posEntrySize

    def getKeyCount: Int = Platform.getInt(baseObj, baseOffset)
    def getKey(idx: Int, schema: StructType): InternalRow = {
      val offset = valueSectionStart +
          Platform.getInt(baseObj, baseOffset + posSectionStart + idx * posEntrySize)
      BTreeIndexRecordReader.readBasedOnSchema(baseObj, baseOffset + offset, schema)
    }
    def getRowIdPos(idx: Int): Int =
      Platform.getInt(baseObj, baseOffset + posSectionStart + idx * posEntrySize + Integer.SIZE / 8)
  }
}
