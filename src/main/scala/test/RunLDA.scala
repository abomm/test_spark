package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * Created by abomm on 9/7/15.
 */
object RunLDA {
  val conf = new SparkConf().setAppName("RunLDA")
  val sc = new SparkContext(conf)
  val data = sc.textFile("data/mllib/sample_lda_data.txt")
  val parsedData: RDD[Vector] = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
  val corpus: RDD[(Long, Vector)] = parsedData.zipWithIndex.map(_.swap).cache()

  print("Running LDA...")
  val ldaModel = new LDA().setK(3).run(corpus)
  println("done!")

  println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
  val topics = ldaModel.topicsMatrix
  for (topic <- Range(0,3)) {
    print (s"Topic $topic: ")
    for (word <- Range(0, ldaModel.vocabSize)) {
      print(s" ${topics(word, topic)}")
    }
    println()
  }


}
