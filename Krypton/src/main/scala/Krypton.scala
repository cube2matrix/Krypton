import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Krypton {
    def main(args: Array[String]) {
        val logFile = "/Users/darrenxyli/Documents/Krypton/test/data/edges/part-00000"
        val conf = new SparkConf().setAppName("Krypton")
        val sc = new SparkContext(conf)
        val graph = GraphLoader.edgeListFile(sc, logFile)
        val ranks = graph.pageRank(0.0001).vertices
        println(ranks.top(5).mkString("\n"))
    }
}
