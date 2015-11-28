package com.darrenxyli.krypton.libs

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.hyperx.{HyperPregel => Pregel, HyperedgeTuple => EdgeTriplet, Hypergraph => Graph}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Compute the betweenness centrality for every vertex in the graph
 *
 * The betweenness centrality is defined on a vertex as the fraction of shortest
 * paths between specified two vertices in the graph that passes this
 * vertex
 *
 * The implementation employs a breadth first search first to get the shortest
 * paths for every vertex, and then conducts a back propagation along the
 * shortest paths to accumulate the centrality incrementally
 */
object BaderBetweennessCentrality extends Logging {

    // sourceId -> (dst, value, precedence[count])
    type BCMap = Map[VertexId, (Int, Int, OpenHashMap[VertexId, Int])]
    type GeneralMap[VD] = OpenHashMap[VertexId, VD]

    def run[VD: ClassTag, ED: ClassTag] (graph: Graph[VD, ED])
    : RDD[(VertexId, Double)] = {

        val size = graph.vertices.count()
        // val num = math.log(size.toDouble)
        val num = size / 2
        val startVertexes = (1.toLong to num)
//        run(graph, graph.pickRandomVertices(num.toInt).toSeq)
        run(graph, startVertexes.toSeq)
    }


    def run[VD: ClassTag, ED: ClassTag] (graph: Graph[VD, ED], landMarks: Seq[VertexId])
    : RDD[(VertexId, Double)] = {

        val bcGraph = graph.mapVertices((vid, attr) =>
            if (landMarks.contains(vid)) {
                makeBCMap(vid -> (0, 0, new OpenHashMap[VertexId, Int]()))
            }
            else {
                makeBCMap()
            }
        )

        val initialMsg = makeBCMap()

        def vertexProgram(id: VertexId, attr: BCMap, msg: BCMap): BCMap =
            mergeMap(attr, msg)

        def sendMessage(tuple: EdgeTriplet[BCMap, ED])
        : Iterator[(VertexId, BCMap)] = {
            val newAttr = mergeMap(
                tuple.srcAttr.map(attr => increase(attr._2, attr._1)).iterator)

            tuple.dstAttr.filter(attr => !is(attr._2, mergeMap(attr._2, newAttr)))
                .flatMap(attr => Iterator((attr._1, newAttr))).iterator
        }

        // breadth first search
        val bfsGraph = Pregel(bcGraph, initialMsg)(
            vertexProgram, sendMessage, mergeMap)

        // Dependency Accumulation, back propagation from the farthest vertices
        val sc = graph.vertices.context
        val vertices = bfsGraph.vertices.collect()
//        val vertexBC = sc.accumulable(new mutable.HashMap[VertexId, Double])(HashMapParam)
        val vertexBC = sc.accumulableCollection(
            mutable.HashMap[VertexId, Double]())
        vertices.foreach{v => vertexBC.value.update(v._1, 0.0)}
        val broadcastVertices = sc.broadcast(vertices)

        sc.parallelize(landMarks).foreach{ source =>

            val vertexInfluence = new mutable.HashMap[VertexId, Double]()
            val sortedVertices = broadcastVertices.value.filter(v =>
                v._2.contains(source)).sortBy(v => -v._2(source)._1)

            // initial g
            sortedVertices.foreach(v => vertexInfluence.update(v._1, 0.0))

            sortedVertices.foreach{v =>
                vertexInfluence(v._1) += 1
                v._2(source)._3.foreach{precedent =>
                    val preId = precedent._1
                    // this could be inefficient due to the linear scan filtering
                    val tracking = broadcastVertices.value.filter(v =>
                        v._1 == preId)(0)._2(source)._2
                    vertexInfluence(preId) +=
                        precedent._2 * 1.0 / v._2(source)._2 *
                            vertexInfluence(v._1) * tracking
                }
                vertexBC += v._1 -> vertexInfluence(v._1)
            }
        }

        sc.parallelize(vertexBC.value.map(v => (v._1, v._2)).toSeq)
    }

    def makeBCMap(x: (VertexId, (Int, Int, GeneralMap[Int]))*) =
        Map(x: _*)

    private def increase(map: BCMap, id: VertexId)
    : BCMap = {
        map.map{case (vid, (dist, value, attr)) =>
            vid -> (dist + 1, value, if (attr == null && attr.isEmpty)
                makeSelfAttr(id) else attr)}
    }

    private def mergeMap(maps: Iterator[BCMap])
    : BCMap = {
        maps.reduce(mergeMap)
    }

    private def mergeMap(bcMapA: BCMap, bcMapB: BCMap)
    : BCMap = {
        val mergedMap = (bcMapA.keySet ++ bcMapB.keySet).map {
            k => k ->(math.min(bcMapDist(bcMapA, k), bcMapDist(bcMapB, k)), 0,
                new OpenHashMap[VertexId, Int]())
        }.toMap
        updateMap(updateMap(mergedMap, bcMapA), bcMapB)
    }

    private def updateMap(bcMapA: BCMap, bcMapB: BCMap)
    : BCMap = {
        if (bcMapB.isEmpty) {
            bcMapA
        } else {
            bcMapA.keySet.map { k => k ->(
                bcMapDist(bcMapA, k),
                if (bcMapDist(bcMapA, k) == bcMapDist(bcMapB, k)) bcMapVal(bcMapA, k) + bcMapVal(bcMapB, k)
                else bcMapVal(bcMapA, k),
                if (bcMapDist(bcMapA, k) == bcMapDist(bcMapB, k)) updateMapAttr(bcMapA, bcMapB, k)
                else bcMapAttr(bcMapA, k))
            }.toMap
        }
    }

    private def updateMapAttr (mapA: BCMap, mapB: BCMap, k: VertexId)
    : GeneralMap[Int] = {
        if (mapB.nonEmpty && mapB.contains(k)) {
            bcMapAttr(mapB, k).foreach(attr =>
                bcMapAttr(mapA, k).update(attr._1, attr._2 +
                    bcMapAttr(mapA, k).getOrElse(attr._1, 0)))
        }
        bcMapAttr(mapA, k)
    }

    private def bcMapDist(map: BCMap, key: VertexId)
    : Int = {
        map.getOrElse(key,
            (Int.MaxValue, 0, null.asInstanceOf[GeneralMap[Int]]))._1
    }

    private def bcMapVal(map: BCMap, key: VertexId)
    : Int = {
        map.getOrElse(key,
            (Int.MaxValue, 0, null.asInstanceOf[GeneralMap[Int]]))._2
    }

    private def bcMapAttr(map: BCMap, key: VertexId)
    : GeneralMap[Int]= {
        map.getOrElse(key,
            (Int.MaxValue, 0, null.asInstanceOf[GeneralMap[Int]]))._3
    }

    private def makeSelfAttr(id: VertexId)
    : GeneralMap[Int] = {
        val attr = new OpenHashMap[VertexId, Int]()
        attr.update(id, 1)
        attr
    }

    def is(a: BCMap, b: BCMap)
    : Boolean = {
        if (a.size != b.size) false
        else if (a.isEmpty && b.nonEmpty || a.nonEmpty && b.isEmpty) false
        else if (a.isEmpty && b.isEmpty) true
        else {
            val keys = a.map(v => b.contains(v._1)).reduce(_ && _)
            if (keys) {
                val values = a.map(v => b(v._1)._1 == v._2._1 &&
                    b(v._1)._2 == v._2._2).reduce(_ && _)
                if (values) {
                    val innerSet = a.map(v =>
                        v._2._3.size == b(v._1)._3.size && ((v._2._3.isEmpty &&
                            b(v._1)._3.isEmpty) || (v._2._3.nonEmpty &&
                            b(v._1)._3.nonEmpty && v._2._3.map(i =>
                            b(v._1)._3.hasKey(i._1) &&
                                b(v._1)._3(i._1) == i._2)
                            .reduce(_ && _)))
                    ).reduce(_ && _)
                    innerSet
                }
                else false
            }
            else false
        }
    }
}
