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

package com.darrenxyli.krypton.libs

import org.apache.spark.graphx._
import scala.reflect.ClassTag

object BetweennessCentrality {
    type BCMap = Map[VertexId, Double]

    private def makeMap(x: (VertexId, Double)*) = Map(x: _*)

    private def incrementMap(spmap: BCMap): BCMap = spmap.map { case (v, d) => v -> (d+1) }

    private def addMaps(spmap1: BCMap, spmap2: BCMap): BCMap =
        (spmap1.keySet ++ spmap2.keySet).map { k => k -> (spmap1.getOrElse(k, 0.0)+spmap2.getOrElse(k, 0.0)) }.toMap

    private def count(set: Set[VertexId]): BCMap = {

        if (set.size == 1) return makeMap()

        val mapTemp = set.map { case vid => vid -> 1.0 }.toMap
        return mapTemp
    }

    def run[VD, ED: ClassTag](graph: Graph[VD, ED], SPGraph: Array[Set[VertexId]]): BCMap = {
        val bcMap = graph.vertices.map { case (vid, attr) => vid -> 0.0 }.collect().toMap

        val passPathCountMap = SPGraph.map(count).reduce(addMaps)
        val totalSPPath = SPGraph.map{ set => if (set.size == 1) 0 else 1}.reduce(_+_)

        val result = addMaps(bcMap, passPathCountMap).map{ case (vid, counter) => vid -> counter/totalSPPath}

        return result
    }
}
