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

import java.io.OutputStream

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.datasources.oap.OapFileFormat


/**
 * Utils for Index read/write
 */
object IndexUtils {
  // TODO remove this and corresponding test
  def writeInt(out: OutputStream, v: Int): Unit = {
    out.write((v >>>  0) & 0xFF)
    out.write((v >>>  8) & 0xFF)
    out.write((v >>> 16) & 0xFF)
    out.write((v >>> 24) & 0xFF)
  }

  def indexFileFromDataFile(dataFile: Path, name: String, time: String): Path = {
    import OapFileFormat._
    val dataFileName = dataFile.getName
    val pos = dataFileName.lastIndexOf(".")
    val indexFileName = if (pos > 0) {
      dataFileName.substring(0, pos)
    } else {
      dataFileName
    }
    new Path(
      dataFile.getParent, "." + indexFileName + "." + time + "." +  name + OAP_INDEX_EXTENSION)
  }

  def writeLong(writer: OutputStream, v: Long): Unit = {
    writer.write((v >>>  0).toInt & 0xFF)
    writer.write((v >>>  8).toInt & 0xFF)
    writer.write((v >>> 16).toInt & 0xFF)
    writer.write((v >>> 24).toInt & 0xFF)
    writer.write((v >>> 32).toInt & 0xFF)
    writer.write((v >>> 40).toInt & 0xFF)
    writer.write((v >>> 48).toInt & 0xFF)
    writer.write((v >>> 56).toInt & 0xFF)
  }
}
