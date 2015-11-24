package com.darrenxyli.krypton.libs

import com.darrenxyli.krypton.libs.BetweennessCentrality.BCMap
import org.apache.spark.graphx
import org.apache.spark.graphx._
import scala.reflect.ClassTag

object BaderBC {

    private def calculateSingleNode[VD: ClassTag](node : graphx.VertexId, VD): Unit = {

    }


    def run[VD, ED: ClassTag](graph: Graph[VD, ED]): BCMap = {

        val bcMap = graph.vertices.map { case (vid, attr) => vid -> 0.0 }.collect().toMap

        for s in graph.vertices


    }
}
