import com.darrenxyli.krypton.libs.{BaderBetweennessCentrality}
import org.apache.spark.hyperx.HypergraphLoader
import org.apache.spark.{SparkConf, SparkContext}
import scala.compat.Platform.currentTime

object Krypton {
    def main(args: Array[String]) {
        val logFile = args{0}
        val partitionNum = args{1}.toInt
        val conf = new SparkConf().setAppName("Krypton")
        val sc = new SparkContext(conf)
        val g = HypergraphLoader.hyperedgeListFile(sc, logFile, " ", false, partitionNum)
        val executionStart: Long = currentTime

        BaderBetweennessCentrality.run(g).collect()

        val total = currentTime - executionStart
        println("[total " + total + "ms]")
        Thread sleep 10000
        sc.stop()
    }
}
