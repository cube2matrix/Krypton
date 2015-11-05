import com.darrenxyli.krypton.libs.ShortestPaths
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object Krypton {
    def main(args: Array[String]) {
        val logFile = "/Volumes/extend1/amazon/data/pubmed/603/edges/part-00000"
        val conf = new SparkConf().setAppName("Krypton")
        val sc = new SparkContext(conf)
        val graph = GraphLoader.edgeListFile(sc, logFile)
        // val ranks = graph.pageRank(0.0001).vertices
        // println(ranks.top(5).mkString("\n"))
        // graph.triplets.take(5).foreach(println(_))
//        val shortestPaths = Set(
//            (1, Map(1 -> 0, 4 -> 2)), (2, Map(1 -> 1, 4 -> 2)), (3, Map(1 -> 2, 4 -> 1)),
//            (4, Map(1 -> 2, 4 -> 0)), (5, Map(1 -> 1, 4 -> 1)), (6, Map(1 -> 3, 4 -> 1)))
//
//        val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)).flatMap {
//            case e => Seq(e, e.swap)
//        }
//        val edges = sc.parallelize(edgeSeq).map { case (v1, v2) => (v1.toLong, v2.toLong) }
//        val graph = Graph.fromEdgeTuples(edges, 1)
        val results = ShortestPaths.run(graph).vertices.collect.map {
            case (v, spMap) => (v, spMap.map {case (k, p) => (k, p+k)})
        }
        val out = results.map {
            case (v, spMap) => println(v + "--->" + spMap)
        }
    }
}

