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

import com.google.common.collect.ArrayListMultimap
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.catalyst.expressions.FromUnsafeProjection
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.StatisticsManager
import org.apache.spark.sql.execution.datasources.oap.utils.PermutermUtils
import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.InputFileNameHolder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.Utils

private[oap] class PermutermWriter(
    relation: WriteIndexRelation,
    job: Job,
    indexColumns: Array[IndexColumn],
    keySchema: StructType,
    indexName: String,
    time: String,
    isAppend: Boolean) extends IndexWriter(relation, job, isAppend) {

  assert(keySchema.length == 1, "not supported building permuterm index on more than 1 column")
  assert(keySchema.head.dataType == StringType, "only support Permuterm index on string column")

  override def writeIndexFromRows(
      taskContext: TaskContext, iterator: Iterator[InternalRow]): Seq[IndexBuildResult] = {
    var taskReturn: Seq[IndexBuildResult] = Nil
    var writeNewFile = false
    executorSideSetup(taskContext)
    val configuration = taskAttemptContext.getConfiguration
    // to get input filename
    if (!iterator.hasNext) return Nil
    // configuration.set(DATASOURCE_OUTPUTPATH, outputPath)
    if (isAppend) {
      val fs = FileSystem.get(configuration)
      var nextFile = InputFileNameHolder.getInputFileName().toString
      var skip = fs.exists(IndexUtils.indexFileFromDataFile(new Path(nextFile), indexName, time))
      // iterator.next()
      while(iterator.hasNext && skip) {
        val cacheFile = nextFile
        nextFile = InputFileNameHolder.getInputFileName().toString
        // avoid calling `fs.exists` for every row
        skip = cacheFile == nextFile ||
          fs.exists(IndexUtils.indexFileFromDataFile(new Path(nextFile), indexName, time))
        if(skip) iterator.next()
      }
      if (skip) return Nil
    }
    val filename = InputFileNameHolder.getInputFileName().toString
    configuration.set(IndexWriter.INPUT_FILE_NAME, filename)
    configuration.set(IndexWriter.INDEX_NAME, indexName)
    configuration.set(IndexWriter.INDEX_TIME, time)
    // TODO deal with partition
    // configuration.set(FileOutputFormat.OUTDIR, getWorkPath)
    var writer = newIndexOutputWriter()

    def commitTask(): Seq[WriteResult] = {
      try {
        var writeResults: Seq[WriteResult] = Nil
        if (writer != null) {
          writeResults = writeResults :+ writer.close()
          writer = null
        }
        super.commitTask()
        writeResults
      } catch {
        case cause: Throwable =>
          // This exception will be handled in `InsertIntoHadoopFsRelation.insert$writeRows`, and
          // will cause `abortTask()` to be invoked.
          throw new RuntimeException("Failed to commit task", cause)
      }
    }

    def abortTask(): Unit = {
      try {
        if (writer != null) {
          writer.close()
        }
      } finally {
        super.abortTask()
      }
    }

    def writeTask(): Seq[IndexBuildResult] = {
      val statisticsManager = new StatisticsManager
      statisticsManager.initialize(PermutermIndexType, keySchema, configuration)
      // key -> RowIDs list
      val multiHashMap = ArrayListMultimap.create[InternalRow, Int]()
      var cnt = 0
      while (iterator.hasNext && !writeNewFile) {
        val fname = InputFileNameHolder.getInputFileName().toString
        if (fname != filename) {
          taskReturn = taskReturn ++: writeIndexFromRows(taskContext, iterator)
          writeNewFile = true
        } else {
          val v = genericProjector(iterator.next()).copy()
          statisticsManager.addOapKey(v)
          multiHashMap.put(v, cnt)
          cnt = cnt + 1
        }
      }
      val partitionUniqueSize = multiHashMap.keySet().size()
      val uniqueKeys = multiHashMap.keySet().toArray(new Array[InternalRow](partitionUniqueSize))
      assert(uniqueKeys.size == partitionUniqueSize)

      // build index file
      var i = 0
      var fileOffset = 0
      val offsetMap = new java.util.HashMap[InternalRow, Int]()
      fileOffset += writeHead(writer, IndexFile.INDEX_VERSION)
      // write data segment.
      while (i < partitionUniqueSize) {
        offsetMap.put(uniqueKeys(i), fileOffset)
        val rowIds = multiHashMap.get(uniqueKeys(i))
        // row count for same key
        IndexUtils.writeInt(writer, rowIds.size())
        // 4 -> value1, stores rowIds count.
        fileOffset = fileOffset + 4
        var idIter = 0
        while (idIter < rowIds.size()) {
          // TODO wrap this to define use Long or Int
          IndexUtils.writeInt(writer, rowIds.get(idIter))
          // 8 -> value2, stores a row id
          fileOffset = fileOffset + 8
          idIter = idIter + 1
        }
        i = i + 1
      }
      val dataEnd = fileOffset
      // write index segment.
      val uniqueKeysList = new java.util.LinkedList[InternalRow]()
      import scala.collection.JavaConverters._
      uniqueKeysList.addAll(uniqueKeys.toSeq.asJava)

      val trie = InMemoryTrie()
      val trieSize = PermutermUtils.generatePermuterm(uniqueKeysList, offsetMap, trie)
      val treeMap = Map[TrieNode, Int]()
      val treeLength = writeTrie(writer, trie, treeMap, 0)
      fileOffset += treeLength

      statisticsManager.write(writer)

      IndexUtils.writeInt(writer, dataEnd)
      IndexUtils.writeInt(writer, treeLength)

      taskReturn :+ IndexBuildResult(filename, cnt, "", new Path(filename).getParent.toString)
    }

    // If anything below fails, we should abort the task.
    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks {
        val res = writeTask()
        commitTask()
        res
      }(catchBlock = abortTask())
    } catch {
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows", t)
    }
  }
  @transient private lazy val genericProjector = FromUnsafeProjection(keySchema)

  private def writeTrie(
      writer: IndexOutputWriter, trieNode: TrieNode,
      treeMap: Map[TrieNode, Int], treeOffset: Int): Int = {
    var base = treeOffset
    treeMap.updated(trieNode, treeOffset)
    trieNode.children.foreach(base += writeTrie(writer, _, treeMap, base))
    IndexUtils.writeInt(writer, trieNode.nodeKey << 16 + trieNode.childCount)
    IndexUtils.writeInt(writer, trieNode.rowIdsPointer)
    base += 8
    trieNode.children.foreach(c => IndexUtils.writeInt(writer, treeMap.getOrElse(c, -1)))
    base + trieNode.childCount * 4
  }
}
