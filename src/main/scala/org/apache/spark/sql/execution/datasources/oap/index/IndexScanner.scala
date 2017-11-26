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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.io.OapIndexInfo
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType


private[oap] object IndexScanner {
  val DUMMY_KEY_START: Key = new UnsafeRow() // we compare the ref not the value
  val DUMMY_KEY_END: Key = new UnsafeRow() // we compare the ref not the value
}

private[oap] abstract class IndexScanner(idxMeta: IndexMeta)
  extends Iterator[Long] with Serializable with Logging{

  // TODO Currently, only B+ tree supports indexs, so this flag is toggled only in
  // BPlusTreeScanner we can add other index-aware stats for other type of index later
  def canBeOptimizedByStatistics: Boolean = false

  @transient protected var ordering: Ordering[Key] = _
  var intervalArray: ArrayBuffer[RangeInterval] = _
  protected var keySchema: StructType = _

  /**
   * Scan N items from each index entry.
   */
  private var limitScan : Int = 0

  def setScanNumLimit(scanNum : Int) : Unit = {
    limitScan = scanNum
  }

  def getLimitScanNum : Int = limitScan

  def limitScanEnabled() : Boolean = limitScan > 0

  def meta: IndexMeta = idxMeta
  def getSchema: StructType = keySchema

  def indexIsAvailable(dataPath: Path, conf: Configuration): Boolean = {
    val indexPath = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)
    if (!indexPath.getFileSystem(conf).exists(indexPath)) {
      logDebug("No index file exist for data file: " + dataPath)
      false
    } else {
      val start = System.currentTimeMillis()
      val enableOIndex = conf.getBoolean(SQLConf.OAP_ENABLE_OINDEX.key,
        SQLConf.OAP_ENABLE_OINDEX.defaultValue.get)
      val useIndex = enableOIndex && canUseIndex(indexPath, dataPath, conf)
      val end = System.currentTimeMillis()
      logDebug("Index Selection Time (Executor): " + (end - start) + "ms")
      if (!useIndex) {
        logWarning("OAP index is skipped. Set below flags to force enable index,\n" +
            "sqlContext.conf.setConfString(SQLConf.OAP_USE_INDEX_FOR_DEVELOPERS.key, true) or \n" +
            "sqlContext.conf.setConfString(SQLConf.OAP_EXECUTOR_INDEX_SELECTION.key, false)")
        false
      } else {
        OapIndexInfo.partitionOapIndex.put(dataPath.toString, true)
        logInfo("Partition File " + dataPath.toString + " will use OAP index.\n")
        true
      }
    }
  }

  /**
   * Executor chooses to use index or not according to policies.
   *  1. OAP_EXECUTOR_INDEX_SELECTION is enabled.
   *  2. Statistics info recommends index scan.
   *  3. Considering about file I/O, index file size should be less
   *     than data file.
   *  4. TODO: add more.
   *
   * @param indexPath: index file path.
   * @param conf: configurations
   * @return Boolean to indicate if executor chooses to use index.
   */
  private def canUseIndex(indexPath: Path, dataPath: Path, conf: Configuration): Boolean = {
    if (conf.getBoolean(SQLConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key,
      SQLConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.defaultValue.get)) {
      // Index selection is enabled, executor chooses index according to policy.

      // Policy 1: index file size < data file size.
      val indexFileSize = indexPath.getFileSystem(conf).getContentSummary(indexPath).getLength
      val dataFileSize = dataPath.getFileSystem(conf).getContentSummary(dataPath).getLength
      val ratio = conf.getDouble(SQLConf.OAP_INDEX_FILE_SIZE_MAX_RATIO.key,
        SQLConf.OAP_INDEX_FILE_SIZE_MAX_RATIO.defaultValue.get)
      indexFileSize <= dataFileSize * ratio

      // Policy 2: statistics tells the scan cost
      // TODO: Get statistics info after statistics is enabled.
      // val statsAnalyseResult = tryToReadStatistics(indexPath, conf)
      // statsAnalyseResult == StaticsAnalysisResult.USE_INDEX

      // More Policies
    } else {
      // Index selection is disabled, executor always uses index.
      true
    }
  }

  def withKeySchema(schema: StructType): IndexScanner = {
    this.keySchema = schema
    this
  }

  def initialize(dataPath: Path, conf: Configuration): IndexScanner
}

// A dummy scanner will actually not do any scanning
private[oap] object DUMMY_SCANNER extends IndexScanner(null) {
  override def initialize(path: Path, configuration: Configuration): IndexScanner = { this }
  override def hasNext: Boolean = false
  override def next(): Long = throw new NoSuchElementException("end of iterating.")
  override def meta: IndexMeta = throw new NotImplementedError()
}

// The building of Search Scanner according to the filter and indices,
private[oap] object ScannerBuilder extends Logging {

  type IntervalArrayMap = mutable.HashMap[String, ArrayBuffer[RangeInterval]]
  type PatternArrayMap = mutable.HashMap[String, ArrayBuffer[SearchPattern]]

  def combineIntervalMaps(
      leftMap: IntervalArrayMap,
      rightMap: IntervalArrayMap,
      ic: IndexContext,
      needMerge: Boolean): IntervalArrayMap = {

    for ((attribute, intervals) <- rightMap) {
      if (leftMap.contains(attribute)) {
        attribute match {
          case ic (filterOptimizer) => // extract the corresponding scannerBuilder
            // combine all intervals of the same attribute of leftMap and rightMap
            if (needMerge) leftMap.put(attribute,
              filterOptimizer.mergeBound(leftMap.getOrElseUpdate (attribute, null), intervals) )
            // add bound of the same attribute to the left map
            else leftMap.put(attribute,
              filterOptimizer.addBound(leftMap.getOrElse (attribute, null), intervals) )
          case _ => // this attribute does not exist, do nothing
        }
      }
      else {
        leftMap.put(attribute, intervals)
      }
    }
    leftMap
  }

  def optimizeFilterBound(
      filter: Filter, ic: IndexContext): mutable.HashMap[String, ArrayBuffer[RangeInterval]] = {
    filter match {
      case And(leftFilter, rightFilter) =>
        val leftMap = optimizeFilterBound(leftFilter, ic)
        val rightMap = optimizeFilterBound(rightFilter, ic)
        combineIntervalMaps(leftMap, rightMap, ic, needMerge = true)
      case Or(leftFilter, rightFilter) =>
        val leftMap = optimizeFilterBound(leftFilter, ic)
        val rightMap = optimizeFilterBound(rightFilter, ic)
        combineIntervalMaps(leftMap, rightMap, ic, needMerge = false)
      case In(attribute, ic(keys)) =>
        val eqBounds = keys.distinct
          .map(key => new RangeInterval(key, key, true, true))
          .to[ArrayBuffer]
        scala.collection.mutable.HashMap(attribute -> eqBounds)
      case EqualTo(attribute, ic(key)) =>
        val ranger = new RangeInterval(key, key, true, true)
        scala.collection.mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case GreaterThanOrEqual(attribute, ic(key)) =>
        val ranger = new RangeInterval(key, IndexScanner.DUMMY_KEY_END, true, true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case GreaterThan(attribute, ic(key)) =>
        val ranger = new RangeInterval(key, IndexScanner.DUMMY_KEY_END, false, true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case LessThanOrEqual(attribute, ic(key)) =>
        val ranger = new RangeInterval(IndexScanner.DUMMY_KEY_START, key, true, true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case LessThan(attribute, ic(key)) =>
        val ranger = new RangeInterval(IndexScanner.DUMMY_KEY_START, key, true, false)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case IsNotNull(attribute) =>
        val ranger =
          new RangeInterval(IndexScanner.DUMMY_KEY_START, IndexScanner.DUMMY_KEY_END, true, true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case _ => mutable.HashMap.empty
    }
  }

  def combinePatternAnd(
      left: PatternArrayMap,
      right: PatternArrayMap): PatternArrayMap = {
    throw new NotImplementedError("TODO")
  }

  def combinePatternOr(
      left: PatternArrayMap,
      right: PatternArrayMap): PatternArrayMap = {
    throw new NotImplementedError("TODO")
  }

  def optimizeFilterPattern(
      filter: Filter): mutable.HashMap[String, ArrayBuffer[SearchPattern]] = {
    filter match {
      case And(leftFilter, rightFilter) =>
        val left = optimizeFilterPattern(leftFilter)
        val right = optimizeFilterPattern(rightFilter)
        combinePatternAnd(left, right)
      case Or(leftFilter, rightFilter) =>
        val left = optimizeFilterPattern(leftFilter)
        val right = optimizeFilterPattern(rightFilter)
        combinePatternOr(left, right)
      case StringStartsWith(attribute, prefix) =>
        val pattern = new SearchPattern(prefix, null, null, null, valid = true)
        mutable.HashMap(attribute -> ArrayBuffer(pattern))
      case _ => mutable.HashMap.empty
    }
  }

  // return whether a Filter predicate can be supported by our current work
  def canSupport(filter: Filter, ic: IndexContext): Boolean = {
    filter match {
      case EqualTo(ic(indexer), _) => true
      case GreaterThan(ic(indexer), _) => true
      case GreaterThanOrEqual(ic(indexer), _) => true
      case LessThan(ic(indexer), _) => true
      case LessThanOrEqual(ic(indexer), _) => true
      case Or(ic(indexer), _) => true
      case And(ic(indexer), _) => true
      case In(ic(indexer), _) => true
      case _ => false
    }
  }

  def build(filters: Array[Filter], ic: IndexContext): Array[Filter] = {
    if (filters == null || filters.isEmpty) return filters
    logDebug("Transform filters into Intervals:")
    val intervalMapArray = filters.map(optimizeFilterBound(_, ic))
    // reduce multiple hashMap to one hashMap(AND operation)
    val intervalMap = intervalMapArray.reduce(
      (leftMap, rightMap) =>
        if (leftMap == null || leftMap.isEmpty) rightMap
        else if (rightMap == null || rightMap.isEmpty) leftMap
        else combineIntervalMaps(leftMap, rightMap, ic, needMerge = true)
    )

    if (intervalMap.nonEmpty) {
      intervalMap.foreach(intervals =>
        logDebug("\t" + intervals._1 + ": " + intervals._2.mkString(" - ")))

      ic.selectAvailableIndex(intervalMap)
      val (num, idxMeta) = ic.getBestIndexer(intervalMap.size)
      ic.buildScanner(num, idxMeta, intervalMap)
    }

    filters.filterNot(canSupport(_, ic))
  }

  // return whether a Filter predicate can be supported by our current work
  def canSupportPattern(filter: Filter, ic: IndexContext): Boolean = {
    filter match {
      case StringStartsWith(ic(indexer), _) => true
      case Or(ic(indexer), _) => true
      case And(ic(indexer), _) => true
      case _ => false
    }
  }

  def buildPatternScanner(filters: Array[Filter], ic: IndexContext): Array[Filter] = {
    if (filters == null || filters.isEmpty) return filters

    val patternMapArray = filters.map(optimizeFilterPattern)
    // TODO combine again
    val patternMap = patternMapArray.head

    ic.selectAvailableIndexForPattern(patternMap)
    val (num, idxMeta) = ic.getBestIndexer(patternMap.size)
    ic.buildPatternScanner(num, idxMeta, intervalMap)

    filters.filterNot(canSupportPattern(_, ic))
  }
}
