import com.darrenxyli.krypton.libs.{BetweennessCentrality, ShortestPaths}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object Krypton {
    def main(args: Array[String]) {
        val logFile = "/Volumes/extend1/amazon/data/pubmed/603/edges/part-00000"
        val conf = new SparkConf().setAppName("Krypton")
        val sc = new SparkContext(conf)
        val graph = GraphLoader.edgeListFile(sc, logFile)
//        val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)).flatMap {
//            case e => Seq(e, e.swap)
//        }
//        val edges = sc.parallelize(edgeSeq).map { case (v1, v2) => (v1.toLong, v2.toLong) }
//        val graph = Graph.fromEdgeTuples(edges, 1)
        val allSPs = ShortestPaths.run(graph).vertices.collect.flatMap {
            case (v, spMap) => spMap.map {case (k, p) => (k, p+k)}.values
        }

        val allBCValue = BetweennessCentrality.run(graph, allSPs)

        println(allBCValue)
    }
}
