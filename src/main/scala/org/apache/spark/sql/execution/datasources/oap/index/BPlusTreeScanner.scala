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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.filecache._

// we scan the index from the smallest to the largest,
// this will scan the B+ Tree (index) leaf node.
private[oap] abstract class BPlusTreeScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {
  override def canBeOptimizedByStatistics: Boolean = true

  var currentKeyIdx = 0
  var indexFiber: IndexFiber = _
  var indexData: CacheResult = _
  var recordReader: BTreeIndexRecordReader = _

  override def hasNext: Boolean = recordReader.hasNext

  override def next(): Long = recordReader.next()
}

private[oap] class BPlusTreeRangeScanner(idxMeta: IndexMeta) extends BPlusTreeScanner(idxMeta) {
  override def toString(): String = "BPlusTreeRangeScanner"

  def initialize(dataPath: Path, conf: Configuration): IndexScanner = {
    assert(keySchema ne null)
    // val root = BTreeIndexCacheManager(dataPath, context, keySchema, meta)
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)
    logDebug("Loading Index File: " + path)
    logDebug("\tFile Size: " + path.getFileSystem(conf).getFileStatus(path).getLen)

    recordReader = BTreeIndexRecordRangeReader(conf, keySchema)
    recordReader.initialize(path, intervalArray)
    this
  }
}

private[oap] class BPlusTreePatternScanner(idxMeta: IndexMeta) extends BPlusTreeScanner(idxMeta) {
  override def toString(): String = "BPlusTreePatternScanner"

  def initialize(dataPath: Path, conf: Configuration): IndexScanner = {
    assert(keySchema ne null)
    // val root = BTreeIndexCacheManager(dataPath, context, keySchema, meta)
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)
    logDebug("Loading Index File: " + path)
    logDebug("\tFile Size: " + path.getFileSystem(conf).getFileStatus(path).getLen)

    recordReader = BTreeIndexRecordPatternReader(conf, keySchema)
    recordReader.initialize(path, patternArray)
    this
  }
}
