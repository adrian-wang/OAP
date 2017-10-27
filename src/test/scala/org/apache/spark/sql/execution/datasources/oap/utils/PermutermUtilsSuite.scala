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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.utils.PermutermUtils

class PermutermUtilsSuite extends SparkFunSuite {
  test("generate permuterm") {
    val row1 = InternalRow("Alpha")
    val row2 = InternalRow("Alphabeta")
    val row3 = InternalRow("AlphaHello")
    val row4 = InternalRow("Beta")
    val row5 = InternalRow("Zero")

    val uniqueList = new java.util.LinkedList[InternalRow]()
    val offsetMap = new java.util.HashMap[InternalRow, Int]()

    val list1 = List(row1, row2)
    list1.foreach(uniqueList.add)
    list1.zipWithIndex.foreach(i => offsetMap.put(i._1, i._2))
    val trie1 = PermutermUtils.generatePermuterm(uniqueList, offsetMap)
    assert(trie1.toString.equals(""))
    uniqueList.clear()
    offsetMap.clear()

    val list2 = List(row2, row3, row4, row5)
    list2.foreach(uniqueList.add)
    list2.zipWithIndex.foreach(i => offsetMap.put(i._1, i._2))
    val trie2 = PermutermUtils.generatePermuterm(uniqueList, offsetMap)
    assert(trie2.toString.equals(""))
    uniqueList.clear()
    offsetMap.clear()
  }
}
