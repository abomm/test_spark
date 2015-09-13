package test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD


/**
 * Created by abomm on 9/7/15.
 */
object RunStats {
  def main(args: Array[String]) {

/*
    val logFile = "/usr/share/dict/words"
    val conf = new SparkConf().setAppName("RunStats")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 4).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
*/

    val conf = new SparkConf().setAppName("RunStats")
    val sc = new SparkContext(conf)

    val observations: RDD[Vector] = sc.parallelize(
      Seq(
        Vectors.dense(1,1),
        Vectors.dense(2,1),
        Vectors.dense(3,1),
        Vectors.dense(4,1),
        Vectors.dense(5,1),
        Vectors.dense(6,1)
      )
    )
    val summary = Statistics.colStats(observations)
    println(s"here is the observations count ${observations.count()}")
    println(s"Here is mean ${summary.mean}, and variance ${summary.variance}, and nonzeros ${summary.numNonzeros}, " +
      s"and l1 norm ${summary.normL1}")


    // Next

    val seriesX: RDD[Double] = sc.parallelize(Seq(1,2,3,4,5,6))
    val seriesY: RDD[Double] = sc.parallelize(Seq(10,20,30,40,50,60))
    val correlation = Statistics.corr(seriesX, seriesY, "pearson")

    val correlMatrix = Statistics.corr(observations)

    println(s"Here is correlation for Spearman: $correlation")
    println(s"Here are correlations! ${correlMatrix}")

    println("generate random data...")
    val u: RDD[Vector] = RandomRDDs.normalVectorRDD(sc, 3, 5,3)
    println("Get statistics...")
    val summaryNormal = Statistics.colStats(u)
    println(s"From random data, here are summary statistics mean ${summaryNormal.mean}, cardinality ${summaryNormal.count}, maximum ${summaryNormal.max}, σ^2${summaryNormal.variance}")


  }
}
