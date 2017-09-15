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

package org.apache.spark.sql.execution.datasources.oap.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.index.InMemoryTrie
import org.apache.spark.unsafe.types.UTF8String

private[oap] object PermutermUtils extends Logging {
  def generatePermuterm(
      uniqueList: java.util.LinkedList[InternalRow],
      offsetMap: java.util.HashMap[InternalRow, Int]): InMemoryTrie = {
    val root = InMemoryTrie()
    val it = uniqueList.iterator()
    while (it.hasNext) {
      val row = it.next()
      addWordToPermutermTree(row, root, offsetMap)
    }
    root
  }

  private def addWordToPermutermTree(
      row: InternalRow,
      root: InMemoryTrie,
      offsetMap: java.util.HashMap[InternalRow, Int]): Unit = {
    val utf8String = row.getUTF8String(0)
    val bytes = utf8String.getBytes
    assert(offsetMap.containsKey(row))
    val endMark = UTF8String.fromString("$").getBytes
    val offset = offsetMap.get(row)
    // including "$123" and "123$" and "23$1" and "3$12"
    (0 to bytes.length).foreach(i => {
      val token = bytes.slice(i, bytes.length) ++ endMark ++ bytes.slice(0, i)
      addArrayByteToTrie(token, offset, root)
    })
  }

  private def addArrayByteToTrie(
      bytes: Seq[Byte], offset: Int, root: InMemoryTrie): Unit = {
    bytes match {
      case Nil => root.setPointer(offset)
      case letter :: Nil =>
        assert(root.children.forall(c => c.nodeKey != letter || !c.canTerminate))
        root.children.find(_.nodeKey == letter) match {
          case Some(tn: InMemoryTrie) => tn.setPointer(offset)
          case _ => root.addChild(InMemoryTrie(letter, offset))
        }
      case letter :: tail =>
        root.children.find(_.nodeKey == letter) match {
          case Some(tn: InMemoryTrie) =>
            addArrayByteToTrie(tail, offset, tn)
          case _ =>
            val parent = InMemoryTrie(letter)
            root.addChild(parent)
            addArrayByteToTrie(tail, offset, parent)
        }
    }
  }
}
