package com.darrenxyli.krypton.libs

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

object BetweennessCentrality extends Logging {

    // sourceId -> (dst, value, Precedence[count])
    type Precedence = mutable.Map[VertexId, Int]
    type Attrbut = (Int, Int, Precedence)
    type BCMap = Map[VertexId, Attrbut]

    /*
     * BCMap opreations
     */
    def makeBCMap(x: (VertexId, (Int, Int, Precedence))*) =
        Map(x: _*)

    private def bcMapDist(map: BCMap, key: VertexId)
    : Int = {
        map.getOrElse(key,
            (Int.MaxValue, 0, null.asInstanceOf[Precedence]))._1
    }

    private def bcMapVal(map: BCMap, key: VertexId)
    : Int = {
        map.getOrElse(key,
            (Int.MaxValue, 0, null.asInstanceOf[Precedence]))._2
    }

    private def bcMapAttr(map: BCMap, key: VertexId)
    : Precedence= {
        map.getOrElse(key,
            (Int.MaxValue, 0, null.asInstanceOf[Precedence]))._3
    }


    def increase(map: BCMap, id: VertexId)
    : BCMap = {
        map.map{case (vid, (dist, value, attr)) =>
            vid -> (dist + 1, value, if (attr == null && attr.isEmpty)
                makeSelfAttr(id) else attr)}
    }

    private def makeSelfAttr(id: VertexId)
    : Precedence = {
        var attr:Precedence = mutable.Map.empty
        attr += (id -> 1)
        attr
    }

    def pickRandomVertices[VD: ClassTag, ED: ClassTag] (hypergraph: Graph[VD, ED], num: Int)
    : mutable.HashSet[VertexId] = {
        val probability = num * 1.0 / hypergraph.numVertices
        val retSet = new mutable.HashSet[VertexId]
        if (probability > 0.5) {
            hypergraph.vertices.map(_._1).collect().foreach{v =>
                if (Random.nextDouble() < probability) {
                    retSet.add(v)
                }
            }
        } else {
            while (retSet.size < num) {
                val selectedVertices = hypergraph.vertices.flatMap { vidVvals =>
                    if (Random.nextDouble() < probability) {
                        Some(vidVvals._1)
                    }
                    else {
                        None
                    }
                }
                if (selectedVertices.count > 1) {
                    val collectedVertices = selectedVertices.collect()
                    collectedVertices.foreach(retSet.add)
                }
            }
        }
        retSet
    }

    private def mergeMap(maps: Iterator[BCMap])
    : BCMap = {
        maps.filterNot(bcMap => !bcMap.isEmpty).reduce ((map1, map2) => {
            if (map1.isEmpty) map2
            else if (map2.isEmpty) map1
            else mergeMap(map1, map2)
        })
    }

    private def mergeMap(bcMapA: BCMap, bcMapB: BCMap)
    : BCMap = {
        val mergedMap = (bcMapA.keySet ++ bcMapB.keySet).map {
            k => k ->(math.min(bcMapDist(bcMapA, k), bcMapDist(bcMapB, k)), 0,
                mutable.Map.empty[VertexId, Int])
        }.toMap
        updateMap(updateMap(mergedMap, bcMapA), bcMapB)
    }

    private def updateMap(bcMapA: BCMap, bcMapB: BCMap)
    : BCMap = {
        if (bcMapB.isEmpty) {
            bcMapA
        }
        else {
            bcMapA.keySet.map { k => k ->(bcMapDist(bcMapA, k),
                if (bcMapDist(bcMapA, k) == bcMapDist(bcMapB, k))
                    bcMapVal(bcMapA, k) + bcMapVal(bcMapB, k)
                else bcMapVal(bcMapA, k),
                if (bcMapDist(bcMapA, k) == bcMapDist(bcMapB, k))
                    updateMapAttr(bcMapA, bcMapB, k)
                else bcMapAttr(bcMapA, k))
            }.toMap
        }
    }

    private def updateMapAttr (mapA: BCMap, mapB: BCMap, k: VertexId)
    : Precedence = {
        if (mapB.nonEmpty && mapB.contains(k)) {
            bcMapAttr(mapB, k).foreach(attr =>
                bcMapAttr(mapA, k) += (attr._1->
                    (attr._2 + bcMapAttr(mapA, k).getOrElse(attr._1, 0))))
        }
        bcMapAttr(mapA, k)
    }

    def is(a: BCMap, b: BCMap): Boolean = {
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
                            b(v._1)._3.contains(i._1) &&
                                b(v._1)._3(i._1) == i._2)
                            .reduce(_ && _)))
                    ).reduce(_ && _)
                    innerSet
                } else false
            } else false
        }
    }

    def run[VD: ClassTag, ED: ClassTag] (graph: Graph[VD, ED])
    : RDD[(VertexId, Double)] = {

        // val num = 56
        val num = 28
        run(graph, pickRandomVertices(graph, num.toInt).toSeq)
    }

    def run[VD: ClassTag, ED: ClassTag] (hypergraph: Graph[VD, ED], landMarks: Seq[VertexId])
    : RDD[(VertexId, Double)] = {

        val bcHypergraph = hypergraph.mapVertices((vid, attr) =>
            if (landMarks.contains(vid)) {
                val attr:Precedence = mutable.Map.empty
                makeBCMap(vid -> (0, 0, attr))
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
                tuple.srcAttr.map(attr => increase(tuple.srcAttr, attr._1)).iterator)

            tuple.dstAttr.filter(attr => !is(tuple.dstAttr, mergeMap(tuple.dstAttr, newAttr)))
                .flatMap(attr => Iterator((attr._1, newAttr))).iterator
        }

        // breadth first search
        val bfsHypergraph = Pregel(bcHypergraph, initialMsg)(
            vertexProgram, sendMessage, mergeMap)

        // back propagation from the farthest vertices
        val sc = hypergraph.vertices.context
        val vertices = bfsHypergraph.vertices.collect()
        val vertexBC = sc.accumulableCollection(
            mutable.HashMap[VertexId, Double]())
        vertices.foreach{v => vertexBC.value.update(v._1, 0)}
        val broadcastVertices = sc.broadcast(vertices)
        sc.parallelize(landMarks).foreach{source =>
            val vertexInfluence = new mutable.HashMap[VertexId, Double]()
            val sortedVertices = broadcastVertices.value.filter(v =>
                v._2.contains(source)).sortBy(v => -v._2(source)._1)
            sortedVertices.foreach(v => vertexInfluence.update(v._1, 0))
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
}
