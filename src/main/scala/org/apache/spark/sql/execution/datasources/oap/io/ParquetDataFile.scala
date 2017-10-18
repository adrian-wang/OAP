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

package org.apache.spark.sql.execution.datasources.oap.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.StringUtils
import org.apache.parquet.column.Dictionary
import org.apache.parquet.hadoop.RecordReaderBuilder
import org.apache.parquet.hadoop.api.RecordReader

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportHelper
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.io.ChunkedByteBuffer


private[oap] case class ParquetDataFile
(path: String, schema: StructType, configuration: Configuration) extends DataFile {

  def getFiberData(groupId: Int, fiberId: Int, conf: Configuration): ChunkedByteBuffer = {
    // TODO data cache
    throw new UnsupportedOperationException("Not support getFiberData Operation.")
  }

  def iterator(conf: Configuration, requiredIds: Array[Int]): Iterator[UnsafeRow] = {
    val recordReader = recordReaderBuilder(conf, requiredIds)
      .buildDefault()
    recordReader.initialize()
    new FileRecordReaderIterator[UnsafeRow](
      recordReader.asInstanceOf[RecordReader[UnsafeRow]])
  }

  def iterator(conf: Configuration,
               requiredIds: Array[Int],
               rowIds: Array[Long]): Iterator[UnsafeRow] = {
    if (rowIds == null || rowIds.length == 0) {
      ParquetDataFile.emptyIterator
    } else {
      val recordReader = recordReaderBuilder(conf, requiredIds)
        .withGlobalRowIds(rowIds).buildIndexed()
      recordReader.initialize()
      new FileRecordReaderIterator[UnsafeRow](
        recordReader.asInstanceOf[RecordReader[UnsafeRow]])
    }
  }

  private def recordReaderBuilder(conf: Configuration,
                                  requiredIds: Array[Int]): RecordReaderBuilder[UnsafeRow] = {
    val requestSchemaString = {
      var requestSchema = new StructType
      for (index <- requiredIds) {
        requestSchema = requestSchema.add(schema(index))
      }
      requestSchema.json
    }
    conf.set(ParquetReadSupportHelper.SPARK_ROW_REQUESTED_SCHEMA, requestSchemaString)

    val readSupport = new OapReadSupportImpl

    val meta: ParquetDataFileHandle = DataFileHandleCacheManager(this)
    RecordReaderBuilder
      .builder(readSupport, new Path(StringUtils.unEscapeString(path)), conf)
      .withFooter(meta.footer)
  }

  private class FileRecordReaderIterator[V](rowReader: RecordReader[V])
    extends Iterator[V] {
    private[this] var havePair = false
    private[this] var finished = false

    override def hasNext: Boolean = {
      if (!finished && !havePair) {
        finished = !rowReader.nextKeyValue
        if (finished) {
          rowReader.close()
        }
        havePair = !finished
      }
      !finished
    }

    override def next(): V = {
      if (!hasNext) {
        throw new java.util.NoSuchElementException("End of stream")
      }
      havePair = false
      rowReader.getCurrentValue
    }
  }

  override def createDataFileHandle(): ParquetDataFileHandle = {
    new ParquetDataFileHandle().read(configuration, new Path(StringUtils.unEscapeString(path)))
  }

  override def getDictionary(fiberId: Int, conf: Configuration): Dictionary = null
}

private[oap] object  ParquetDataFile {

  val emptyIterator = new Iterator[UnsafeRow] {
    override def hasNext: Boolean = false

    override def next(): UnsafeRow =
      throw new java.util.NoSuchElementException("Input is Empty RowIds Array")
  }
}
