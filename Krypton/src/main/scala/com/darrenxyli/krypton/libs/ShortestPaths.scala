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

/**
 * Computes shortest paths to the given set of landmark vertices, returning a graph where each
 * vertex attribute is a map containing the shortest-path distance to each reachable landmark.
 */
object ShortestPaths {
    /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
    type SPMap = Map[VertexId, Set[VertexId]]

    private def makeMap(x: (VertexId, Set[VertexId])*) = Map(x: _*)

    private def incrementMap(spmap: SPMap, srcId: VertexId): SPMap =
        spmap.map { case (v, d) => v -> incrementSet(d, srcId) }

    private def shorterSet(spmap1: SPMap, spmap2: SPMap, k: VertexId): Set[VertexId] = {


        val set1: Set[VertexId] = spmap1.getOrElse(k, Set.empty[VertexId])
        val set2: Set[VertexId] = spmap2.getOrElse(k, Set.empty[VertexId])

        if (spmap1.contains(k) && spmap2.contains(k)) {
            if (set1.size < set2.size) return set1
            else return set2
        } else if (spmap1.contains(k)) return set1
        else return set2

    }

    private def incrementSet(originSet: Set[VertexId], vId: VertexId): Set[VertexId] = {
        val newSet: Set[VertexId] = originSet + vId
        return newSet
    }

    private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
        (spmap1.keySet ++ spmap2.keySet).map { k => k -> shorterSet(spmap1, spmap2, k) }.toMap

    /**
     * Computes shortest paths to the given set of landmark vertices.
     *
     * @tparam ED the edge attribute type (not used in the computation)
     *
     * @param graph the graph for which to compute the shortest paths
     *
     * @return a graph where each vertex attribute is a map containing the shortest-path distance to
     * each reachable landmark vertex.
     */
    def run[VD, ED: ClassTag](graph: Graph[VD, ED]): Graph[SPMap, ED] = {
        val spGraph = graph.mapVertices { (vid, attr) =>
            makeMap(vid -> Set.empty[VertexId])
        }

        val initialMessage = makeMap()

        def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
            addMaps(attr, msg)
        }

        def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
            val newAttr = incrementMap(edge.dstAttr, edge.srcId)
            if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
            else Iterator.empty
        }

        Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
    }
}
