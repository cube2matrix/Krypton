package com.darrenxyli.krypton.libs

import org.apache.spark.graphx._
import scala.reflect.ClassTag


object BaderBC {
    /** Stores a map from the vertex id of a landmark to the distance to that landmark. */

    // Message (sourceId, g[src], d[src])
    // Attr (Set, g[cur], d[cur])

    type Attribution = (Set[VertexId], Int, Int)
    type Message = (VertexId, Int, Int)

    /**
    * Computes shortest paths to the given set of landmark vertices.
    *
    * @tparam ED the edge attribute type (not used in the computation)
    *
    * @param graph the graph for which to compute the shortest paths
    * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
    * landmark.
    *
    * @return a graph where each vertex attribute is a map containing the shortest-path distance to
    * each reachable landmark vertex.
    */
    def run[VD, ED: ClassTag](graph: Graph[VD, ED], landmarks: VertexId): Graph[Attribution, ED] = {
        val initialGraph = graph.mapVertices { (vid, attr) =>
            if (landmarks == vid) (Set[VertexId](), 1, 0) else (Set[VertexId](), 0, -1)
        }

        val initialMessage = (-1.toLong, 0, -2)

        def vertexProgram(id: VertexId,
                          attr: Attribution,
                          msg: Message): Attribution = {
            val (pre, gLocal, dLocal) = attr
            val (srcId, gSrc, dSrc) = msg

            var newDLocal = dLocal
            var newGLocal = gLocal
            var newPre = pre

            if (srcId != -1.toLong) {
                if (newDLocal == -1) newDLocal = dSrc + 1
                if (newDLocal == dSrc + 1) {
                    newPre += srcId
                    newGLocal += gSrc
                }
            }
            (newPre, newGLocal, newDLocal)
        }

        def sendMessage(edge: EdgeTriplet[Attribution, _]): Iterator[(VertexId, Message)] = {

            if (edge.dstAttr._1.contains(edge.srcId)) Iterator.empty
            else Iterator((edge.dstId, (edge.srcId, edge.srcAttr._2, edge.srcAttr._3)))

        }

        def mergeMessage(msg1: Message, msg2: Message): Message = {
            if (msg1._3 < msg2._3) msg1 else msg2
        }

        Pregel(initialGraph, initialMessage)(vertexProgram, sendMessage, mergeMessage)
    }
}
