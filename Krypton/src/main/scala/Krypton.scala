import org.apache.spark.hyperx.HypergraphLoader
import org.apache.spark.hyperx.lib.BetweennessCentrality
import org.apache.spark.{SparkConf, SparkContext}
import scala.compat.Platform.currentTime

object Krypton {
    def main(args: Array[String]) {
        val logFile = args{0}
        val conf = new SparkConf().setAppName("Krypton")
        val sc = new SparkContext(conf)
        val g = HypergraphLoader.hyperedgeListFile(sc, logFile, " ", false, 1)
        val executionStart: Long = currentTime

        BetweennessCentrality.run(g).map { case (id, v) => print(id + ":" +v)}

        val total = currentTime - executionStart
        println("[total " + total + "ms]")
	sc.stop()
    }
}
